#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>

#include <curl/curl.h>

#include "acconfig.h"
#ifdef FASTCGI_INCLUDE_DIR
# include "fastcgi/fcgiapp.h"
#else
# include "fcgiapp.h"
#endif

#include "rgw_fcgi.h"

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "common/Timer.h"
#include "common/Throttle.h"
#include "common/safe_io.h"
#include "include/str_list.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_user.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_swift.h"
#include "rgw_rest_admin.h"
#include "rgw_rest_usage.h"
#include "rgw_rest_user.h"
#include "rgw_rest_bucket.h"
#include "rgw_rest_metadata.h"
#include "rgw_rest_log.h"
#include "rgw_rest_opstate.h"
#include "rgw_replica_log.h"
#include "rgw_rest_replica_log.h"
#include "rgw_rest_config.h"
#include "rgw_swift_auth.h"
#include "rgw_swift.h"
#include "rgw_log.h"
#include "rgw_tools.h"
#include "rgw_resolve.h"
#include "rgw_mongoose.h"

#include "mongoose/mongoose.h"

#include <map>
#include <string>
#include <vector>
#include <iostream>
#include <sstream>

#include "include/types.h"
#include "common/BackTrace.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

static sig_t sighandler_alrm;

class RGWProcess;

static int signal_fd[2] = {0, 0};
static atomic_t disable_signal_fd;


#define SOCKET_BACKLOG 1024

struct RGWRequest
{
  uint64_t id;
  struct req_state *s;
  string req_str;
  RGWOp *op;
  utime_t ts;

  RGWRequest() : id(0), s(NULL), op(NULL) {
  }

  ~RGWRequest() {
    delete s;
  }
 
  req_state *init_state(CephContext *cct, RGWEnv *env) { 
    s = new req_state(cct, env);
    return s;
  }

  void log_format(struct req_state *s, const char *fmt, ...)
  {
#define LARGE_SIZE 1024
    char buf[LARGE_SIZE];
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);

    log(s, buf);
  }

  void log_init() {
    ts = ceph_clock_now(g_ceph_context);
  }

  void log(struct req_state *s, const char *msg) {
    if (s->info.method && req_str.size() == 0) {
      req_str = s->info.method;
      req_str.append(" ");
      req_str.append(s->info.request_uri);
    }
    utime_t t = ceph_clock_now(g_ceph_context) - ts;
    dout(2) << "req " << id << ":" << t << ":" << s->dialect << ":" << req_str << ":" << (op ? op->name() : "") << ":" << msg << dendl;
  }
};

class RGWFrontendConfig {
  string config;
  map<string, string> config_map;
  int parse_config(const string& config, map<string, string>& config_map);
  string framework;
public:
  RGWFrontendConfig(const string& _conf) : config(_conf) {}
  int init() {
    int ret = parse_config(config, config_map);
    if (ret < 0)
      return ret;
    return 0;
  }
  bool get_val(const string& key, const string& def_val, string *out);
  bool get_val(const string& key, int def_val, int *out);

  string get_framework() { return framework; }
};


struct RGWFCGXRequest : public RGWRequest {
  FCGX_Request fcgx;
};

struct RGWProcessEnv {
  RGWRados *store;
  RGWREST *rest;
  OpsLogSocket *olog;
  int port;
};

class RGWProcess {
  RGWRados *store;
  OpsLogSocket *olog;
  deque<RGWFCGXRequest *> m_req_queue;
  ThreadPool m_tp;
  Throttle req_throttle;
  RGWREST *rest;
  int sock_fd;
  RGWFrontendConfig *conf;

  RGWProcessEnv *process_env;

  struct RGWWQ : public ThreadPool::WorkQueue<RGWFCGXRequest> {
    RGWProcess *process;
    RGWWQ(RGWProcess *p, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<RGWFCGXRequest>("RGWWQ", timeout, suicide_timeout, tp), process(p) {}

    bool _enqueue(RGWFCGXRequest *req) {
      process->m_req_queue.push_back(req);
      perfcounter->inc(l_rgw_qlen);
      dout(20) << "enqueued request req=" << hex << req << dec << dendl;
      _dump_queue();
      return true;
    }
    void _dequeue(RGWFCGXRequest *req) {
      assert(0);
    }
    bool _empty() {
      return process->m_req_queue.empty();
    }
    RGWFCGXRequest *_dequeue() {
      if (process->m_req_queue.empty())
	return NULL;
      RGWFCGXRequest *req = process->m_req_queue.front();
      process->m_req_queue.pop_front();
      dout(20) << "dequeued request req=" << hex << req << dec << dendl;
      _dump_queue();
      perfcounter->inc(l_rgw_qlen, -1);
      return req;
    }
    void _process(RGWFCGXRequest *req) {
      perfcounter->inc(l_rgw_qactive);
      process->handle_request(req);
      process->req_throttle.put(1);
      perfcounter->inc(l_rgw_qactive, -1);
    }
    void _dump_queue() {
      deque<RGWFCGXRequest *>::iterator iter;
      if (process->m_req_queue.empty()) {
        dout(20) << "RGWWQ: empty" << dendl;
        return;
      }
      dout(20) << "RGWWQ:" << dendl;
      for (iter = process->m_req_queue.begin(); iter != process->m_req_queue.end(); ++iter) {
        dout(20) << "req: " << hex << *iter << dec << dendl;
      }
    }
    void _clear() {
      assert(process->m_req_queue.empty());
    }
  } req_wq;

  uint64_t max_req_id;

public:
  RGWProcess(CephContext *cct, RGWProcessEnv *pe, int num_threads, RGWFrontendConfig *_conf)
    : store(pe->store), olog(pe->olog), m_tp(cct, "RGWProcess::m_tp", num_threads),
      req_throttle(cct, "rgw_ops", num_threads * 2),
      rest(pe->rest), sock_fd(-1),
      conf(_conf),
      req_wq(this, g_conf->rgw_op_thread_timeout,
	     g_conf->rgw_op_thread_suicide_timeout, &m_tp),
      max_req_id(0) {}
  void run();
  void handle_request(RGWFCGXRequest *req);

  void close_fd() {
    if (sock_fd >= 0)
      close(sock_fd);
  }
};

void RGWProcess::run()
{
  sock_fd = 0;

  string socket_path;
  string socket_port;
  string socket_host;

  conf->get_val("socket_path", g_conf->rgw_socket_path, &socket_path);
  conf->get_val("socket_port", g_conf->rgw_port, &socket_port);
  conf->get_val("socket_host", g_conf->rgw_host, &socket_host);


  if (!socket_path.empty()) {
    string path_str = socket_path;

    /* this is necessary, as FCGX_OpenSocket might not return an error, but rather ungracefully exit */
    int fd = open(path_str.c_str(), O_CREAT, 0644);
    if (fd < 0) {
      int err = errno;
      /* ENXIO is actually expected, we'll get that if we try to open a unix domain socket */
      if (err != ENXIO) {
        dout(0) << "ERROR: cannot create socket: path=" << path_str << " error=" << cpp_strerror(err) << dendl;
        return;
      }
    } else {
      close(fd);
    }

    const char *path = path_str.c_str();
    sock_fd = FCGX_OpenSocket(path, SOCKET_BACKLOG);
    if (sock_fd < 0) {
      dout(0) << "ERROR: FCGX_OpenSocket (" << path << ") returned " << sock_fd << dendl;
      return;
    }
    if (chmod(path, 0777) < 0) {
      dout(0) << "WARNING: couldn't set permissions on unix domain socket" << dendl;
    }
  } else if (!socket_port.empty()) {
    string bind = socket_host + ":" + socket_port;
    sock_fd = FCGX_OpenSocket(bind.c_str(), SOCKET_BACKLOG);
    if (sock_fd < 0) {
      dout(0) << "ERROR: FCGX_OpenSocket (" << bind.c_str() << ") returned " << sock_fd << dendl;
      return;
    }
  }

  m_tp.start();

  for (;;) {
    RGWFCGXRequest *req = new RGWFCGXRequest;
    req->id = ++max_req_id;
    dout(10) << "allocated request req=" << hex << req << dec << dendl;
    FCGX_InitRequest(&req->fcgx, sock_fd, 0);
    req_throttle.get(1);
    int ret = FCGX_Accept_r(&req->fcgx);
    if (ret < 0) {
      delete req;
      dout(0) << "ERROR: FCGX_Accept_r returned " << ret << dendl;
      req_throttle.put(1);
      break;
    }

    req_wq.queue(req);
  }

  m_tp.drain();
  m_tp.stop();
}

static void signal_shutdown()
{
  if (!disable_signal_fd.read()) {
    int val = 0;
    int ret = write(signal_fd[0], (char *)&val, sizeof(val));
    if (ret < 0) {
      int err = -errno;
      derr << "ERROR: " << __func__ << ": write() returned " << cpp_strerror(-err) << dendl;
    }
  }
}

static void wait_shutdown()
{
  int val;
  int r = safe_read_exact(signal_fd[1], &val, sizeof(val));
  if (r < 0) {
    derr << "safe_read_exact returned with error" << dendl;
  }
}

int signal_fd_init()
{
  return socketpair(AF_UNIX, SOCK_STREAM, 0, signal_fd);
}

static void signal_fd_finalize()
{
  close(signal_fd[0]);
  close(signal_fd[1]);
}

static void handle_sigterm(int signum)
{
  dout(1) << __func__ << dendl;
  FCGX_ShutdownPending();

  // send a signal to make fcgi's accept(2) wake up.  unfortunately the
  // initial signal often isn't sufficient because we race with accept's
  // check of the flag wet by ShutdownPending() above.
  if (signum != SIGUSR1) {
    signal_shutdown();

    // safety net in case we get stuck doing an orderly shutdown.
    uint64_t secs = g_ceph_context->_conf->rgw_exit_timeout_secs;
    if (secs)
      alarm(secs);
    dout(1) << __func__ << " set alarm for " << secs << dendl;
  }

}

static void godown_alarm(int signum)
{
  _exit(0);
}

static int call_log_intent(RGWRados *store, void *ctx, rgw_obj& obj, RGWIntentEvent intent)
{
  struct req_state *s = (struct req_state *)ctx;
  return rgw_log_intent(store, s, obj, intent);
}


static int process_request(RGWRados *store, RGWREST *rest, RGWRequest *req, RGWClientIO *client_io, OpsLogSocket *olog)
{
  int ret = 0;

  client_io->init(g_ceph_context);

  req->log_init();

  dout(1) << "====== starting new request req=" << hex << req << dec << " =====" << dendl;
  perfcounter->inc(l_rgw_req);

  RGWEnv& rgw_env = client_io->get_env();

  struct req_state *s = req->init_state(g_ceph_context, &rgw_env);
  s->obj_ctx = store->create_context(s);
  store->set_intent_cb(s->obj_ctx, call_log_intent);

  s->req_id = store->unique_id(req->id);

  req->log(s, "initializing");

  RGWOp *op = NULL;
  int init_error = 0;
  bool should_log = false;
  RGWRESTMgr *mgr;
  RGWHandler *handler = rest->get_handler(store, s, client_io, &mgr, &init_error);
  if (init_error != 0) {
    abort_early(s, NULL, init_error);
    goto done;
  }

  should_log = mgr->get_logging();

  req->log(s, "getting op");
  op = handler->get_op(store);
  if (!op) {
    abort_early(s, NULL, -ERR_METHOD_NOT_ALLOWED);
    goto done;
  }
  req->op = op;

  req->log(s, "authorizing");
  ret = handler->authorize();
  if (ret < 0) {
    dout(10) << "failed to authorize request" << dendl;
    abort_early(s, op, ret);
    goto done;
  }

  if (s->user.suspended) {
    dout(10) << "user is suspended, uid=" << s->user.user_id << dendl;
    abort_early(s, op, -ERR_USER_SUSPENDED);
    goto done;
  }
  req->log(s, "reading permissions");
  ret = handler->read_permissions(op);
  if (ret < 0) {
    abort_early(s, op, ret);
    goto done;
  }

  req->log(s, "init op");
  ret = op->init_processing();
  if (ret < 0) {
    abort_early(s, op, ret);
    goto done;
  }

  req->log(s, "verifying op mask");
  ret = op->verify_op_mask();
  if (ret < 0) {
    abort_early(s, op, ret);
    goto done;
  }

  req->log(s, "verifying op permissions");
  ret = op->verify_permission();
  if (ret < 0) {
    if (s->system_request) {
      dout(2) << "overriding permissions due to system operation" << dendl;
    } else {
      abort_early(s, op, ret);
      goto done;
    }
  }

  req->log(s, "verifying op params");
  ret = op->verify_params();
  if (ret < 0) {
    abort_early(s, op, ret);
    goto done;
  }

  if (s->expect_cont)
    dump_continue(s);

  req->log(s, "executing");
  op->execute();
  op->complete();
done:
  int r = client_io->complete_request();
  if (r < 0) {
    dout(0) << "ERROR: client_io->complete_request() returned " << r << dendl;
  }
  if (should_log) {
    rgw_log_op(store, s, (op ? op->name() : "unknown"), olog);
  }

  int http_ret = s->err.http_ret;

  req->log_format(s, "http status=%d", http_ret);

  if (handler)
    handler->put_op(op);
  rest->put_handler(handler);
  store->destroy_context(s->obj_ctx);

  dout(1) << "====== req done req=" << hex << req << dec << " http_status=" << http_ret << " ======" << dendl;

  return ret;
}

void RGWProcess::handle_request(RGWFCGXRequest *req)
{
  FCGX_Request *fcgx = &req->fcgx;
  RGWFCGX client_io(fcgx);

 
  int ret = process_request(store, rest, req, &client_io, olog);
  if (ret < 0) {
    /* we don't really care about return code */
    dout(20) << "process_request() returned " << ret << dendl;
  }

  FCGX_Finish_r(fcgx);

  delete req;
}


static int mongoose_callback(struct mg_connection *conn) {
  struct mg_request_info *req_info = mg_get_request_info(conn);
  RGWProcessEnv *pe = (RGWProcessEnv *)req_info->user_data;
  RGWRados *store = pe->store;
  RGWREST *rest = pe->rest;
  OpsLogSocket *olog = pe->olog;

  RGWRequest *req = new RGWRequest;
  RGWMongoose client_io(conn, pe->port);

  client_io.init(g_ceph_context);


  int ret = process_request(store, rest, req, &client_io, olog);
  if (ret < 0) {
    /* we don't really care about return code */
    dout(20) << "process_request() returned " << ret << dendl;
  }

  delete req;

// Mark as processed
  return 1;
}

#ifdef HAVE_CURL_MULTI_WAIT
static void check_curl()
{
}
#else
static void check_curl()
{
  derr << "WARNING: libcurl doesn't support curl_multi_wait()" << dendl;
  derr << "WARNING: cross zone / region transfer performance may be affected" << dendl;
}
#endif

class C_InitTimeout : public Context {
public:
  C_InitTimeout() {}
  void finish(int r) {
    derr << "Initialization timeout, failed to initialize" << dendl;
    exit(1);
  }
};

int usage()
{
  cerr << "usage: radosgw [options...]" << std::endl;
  cerr << "options:\n";
  cerr << "   --rgw-region=<region>     region in which radosgw runs\n";
  cerr << "   --rgw-zone=<zone>         zone in which radosgw runs\n";
  generic_server_usage();
  return 0;
}

static RGWRESTMgr *set_logging(RGWRESTMgr *mgr)
{
  mgr->set_logging(true);
  return mgr;
}


int RGWFrontendConfig::parse_config(const string& config, map<string, string>& config_map)
{
  list<string> config_list;
  get_str_list(config, " ", config_list);

  list<string>::iterator iter;
  for (iter = config_list.begin(); iter != config_list.end(); ++iter) {
    string& entry = *iter;
    string key;
    string val;

    if (framework.empty()) {
      framework = entry;
      dout(0) << "framework: " << framework << dendl;
      continue;
    }

    ssize_t pos = entry.find('=');
    if (pos < 0) {
      dout(0) << "framework conf key: " << entry << dendl;
      config_map[entry] = "";
      continue;
    }

    int ret = parse_key_value(entry, key, val);
    if (ret < 0) {
      cerr << "ERROR: can't parse " << entry << std::endl;
      return ret;
    }

    dout(0) << "framework conf key: " << key << ", val: " << val << dendl;
    config_map[key] = val;
  }

  return 0;
}


bool RGWFrontendConfig::get_val(const string& key, const string& def_val, string *out)
{
 map<string, string>::iterator iter = config_map.find(key);
 if (iter == config_map.end()) {
   *out = def_val;
   return false;
 }

 *out = iter->second;
 return true;
}


bool RGWFrontendConfig::get_val(const string& key, int def_val, int *out)
{
  string str;
  bool found = get_val(key, "", &str);
  if (!found) {
    *out = def_val;
    return false;
  }
  string err;
  *out = strict_strtol(str.c_str(), 10, &err);
  if (!err.empty()) {
    cerr << "error parsing int: " << str << ": " << err << std::endl;
    return -EINVAL;
  }
  return 0;
}

class RGWFrontend {
public:
  virtual ~RGWFrontend() {}
  virtual int run() = 0;
  virtual void stop() = 0;
  virtual void join() = 0;
};

class RGWFCGXControlThread : public Thread {
  RGWProcess *pprocess;
public:
  RGWFCGXControlThread(RGWProcess *_pprocess) : pprocess(_pprocess) {}

  void *entry() {
    pprocess->run();
    return NULL;
  };
};

class RGWFCGXFrontend : public RGWFrontend {
  RGWFrontendConfig *conf;
  RGWProcess *pprocess;
  RGWProcessEnv env;
  RGWFCGXControlThread *thread;

public:
  RGWFCGXFrontend(RGWProcessEnv& pe, RGWFrontendConfig *_conf) : conf(_conf), env(pe) {
    pprocess = new RGWProcess(g_ceph_context, &env, g_conf->rgw_thread_pool_size, conf);
    thread = new RGWFCGXControlThread(pprocess);
  }

  ~RGWFCGXFrontend() {
    delete thread;
    delete pprocess;
  }

  int run() {
    thread->create();
    return 0;
  }

  void stop() {
    pprocess->close_fd();
    thread->kill(SIGUSR1);
  }

  void join() {
    thread->join();
  }
};

class RGWMongooseFrontend : public RGWFrontend {
  RGWFrontendConfig *conf;
  struct mg_context *ctx;
  RGWProcessEnv env;

public:
  RGWMongooseFrontend(RGWProcessEnv& pe, RGWFrontendConfig *_conf) : conf(_conf), ctx(NULL), env(pe) {
  }

  int run() {
    char thread_pool_buf[32];
    snprintf(thread_pool_buf, sizeof(thread_pool_buf), "%d", (int)g_conf->rgw_thread_pool_size);
    string port_str;
    conf->get_val("port", "80", &port_str);
    const char *options[] = {"listening_ports", port_str.c_str(), "enable_keep_alive", "yes", "num_threads", thread_pool_buf, NULL};

    struct mg_callbacks cb;
    memset((void *)&cb, 0, sizeof(cb));
    cb.begin_request = mongoose_callback;
    ctx = mg_start(&cb, &env, (const char **)&options);

    if (!ctx) {
      return -EIO;
    }

    return 0;
  }

  void stop() {
    if (ctx) {
      mg_stop(ctx);
    }
  }

  void join() {
  }
};

/*
 * start up the RADOS connection and then handle HTTP messages as they come in
 */
int main(int argc, const char **argv)
{
  // dout() messages will be sent to stderr, but FCGX wants messages on stdout
  // Redirect stderr to stdout.
  TEMP_FAILURE_RETRY(close(STDERR_FILENO));
  if (TEMP_FAILURE_RETRY(dup2(STDOUT_FILENO, STDERR_FILENO) < 0)) {
    int err = errno;
    cout << "failed to redirect stderr to stdout: " << cpp_strerror(err)
	 << std::endl;
    return ENOSYS;
  }

  /* alternative default for module */
  vector<const char *> def_args;
  def_args.push_back("--debug-rgw=1/5");
  def_args.push_back("--keyring=$rgw_data/keyring");
  def_args.push_back("--log-file=/var/log/radosgw/$cluster-$name");

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  global_init(&def_args, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_DAEMON,
	      CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ++i) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return 0;
    }
  }

  check_curl();

  if (g_conf->daemonize) {
    global_init_daemonize(g_ceph_context, 0);
  }
  Mutex mutex("main");
  SafeTimer init_timer(g_ceph_context, mutex);
  init_timer.init();
  mutex.Lock();
  init_timer.add_event_after(g_conf->rgw_init_timeout, new C_InitTimeout);
  mutex.Unlock();

  common_init_finish(g_ceph_context);

  rgw_tools_init(g_ceph_context);

  rgw_init_resolver();
  rgw_rest_init(g_ceph_context);
  
  curl_global_init(CURL_GLOBAL_ALL);
  
  FCGX_Init();

  int r = 0;
  RGWRados *store = RGWStoreManager::get_storage(g_ceph_context, true);
  if (!store) {
    derr << "Couldn't init storage provider (RADOS)" << dendl;
    r = EIO;
  }
  if (!r)
    r = rgw_perf_start(g_ceph_context);

  mutex.Lock();
  init_timer.cancel_all_events();
  init_timer.shutdown();
  mutex.Unlock();

  if (r) 
    return 1;

  rgw_user_init(store->meta_mgr);
  rgw_bucket_init(store->meta_mgr);
  rgw_log_usage_init(g_ceph_context, store);

  RGWREST rest;

  list<string> apis;
  bool do_swift = false;

  get_str_list(g_conf->rgw_enable_apis, apis);

  map<string, bool> apis_map;
  for (list<string>::iterator li = apis.begin(); li != apis.end(); ++li) {
    apis_map[*li] = true;
  }

  if (apis_map.count("s3") > 0)
    rest.register_default_mgr(set_logging(new RGWRESTMgr_S3));

  if (apis_map.count("swift") > 0) {
    do_swift = true;
    swift_init(g_ceph_context);
    rest.register_resource(g_conf->rgw_swift_url_prefix, set_logging(new RGWRESTMgr_SWIFT));
  }

  if (apis_map.count("swift_auth") > 0)
    rest.register_resource(g_conf->rgw_swift_auth_entry, set_logging(new RGWRESTMgr_SWIFT_Auth));

  if (apis_map.count("admin") > 0) {
    RGWRESTMgr_Admin *admin_resource = new RGWRESTMgr_Admin;
    admin_resource->register_resource("usage", new RGWRESTMgr_Usage);
    admin_resource->register_resource("user", new RGWRESTMgr_User);
    admin_resource->register_resource("bucket", new RGWRESTMgr_Bucket);
  
    /*Registering resource for /admin/metadata */
    admin_resource->register_resource("metadata", new RGWRESTMgr_Metadata);
    admin_resource->register_resource("log", new RGWRESTMgr_Log);
    admin_resource->register_resource("opstate", new RGWRESTMgr_Opstate);
    admin_resource->register_resource("replica_log", new RGWRESTMgr_ReplicaLog);
    admin_resource->register_resource("config", new RGWRESTMgr_Config);
    rest.register_resource(g_conf->rgw_admin_entry, admin_resource);
  }

  OpsLogSocket *olog = NULL;

  if (!g_conf->rgw_ops_log_socket_path.empty()) {
    olog = new OpsLogSocket(g_ceph_context, g_conf->rgw_ops_log_data_backlog);
    olog->init(g_conf->rgw_ops_log_socket_path);
  }

  r = signal_fd_init();
  if (r < 0) {
    derr << "ERROR: unable to initialize signal fds" << dendl;
    exit(1);
  }

  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler(SIGTERM, handle_sigterm);
  register_async_signal_handler(SIGINT, handle_sigterm);
  register_async_signal_handler(SIGUSR1, handle_sigterm);
  sighandler_alrm = signal(SIGALRM, godown_alarm);

  string frontend_frameworks = g_conf->rgw_frontends;

  list<string> frontends;

  get_str_list(g_conf->rgw_frontends, ",", frontends);

  multimap<string, RGWFrontendConfig *> fe_map;
  list<RGWFrontendConfig *> configs;
  if (frontends.empty()) {
    frontends.push_back("fastcgi");
  }
  for (list<string>::iterator iter = frontends.begin(); iter != frontends.end(); ++iter) {
    string& f = *iter;

    RGWFrontendConfig *config = new RGWFrontendConfig(f);
    int r = config->init();
    if (r < 0) {
      cerr << "ERROR: failed to init config: " << f << std::endl;
      return EINVAL;
    }

    configs.push_back(config);

    string framework = config->get_framework();
    fe_map.insert(make_pair<string, RGWFrontendConfig *>(framework, config));
  }

  list<RGWFrontend *> fes;

  for (multimap<string, RGWFrontendConfig *>::iterator fiter = fe_map.begin(); fiter != fe_map.end(); ++fiter) {
    RGWFrontendConfig *config = fiter->second;
    string framework = config->get_framework();
    RGWFrontend *fe;
    if (framework == "fastcgi" || framework == "fcgi") {
      RGWProcessEnv fcgi_pe = { store, &rest, olog, 0 };

      fe = new RGWFCGXFrontend(fcgi_pe, config);
    } else if (framework == "mongoose") {
      string err;

      int port;
      config->get_val("port", 80, &port);

      RGWProcessEnv env = { store, &rest, olog, port };

      fe = new RGWMongooseFrontend(env, config);
    } else {
      dout(0) << "WARNING: skipping unknown framework: " << framework << dendl;
      continue;
    }
    dout(0) << "starting handler: " << fiter->first << dendl;
    fe->run();

    fes.push_back(fe);
  }

  wait_shutdown();

  derr << "shutting down" << dendl;

  for (list<RGWFrontend *>::iterator liter = fes.begin(); liter != fes.end(); ++liter) {
    RGWFrontend *fe = *liter;
    fe->stop();
  }

  for (list<RGWFrontend *>::iterator liter = fes.begin(); liter != fes.end(); ++liter) {
    RGWFrontend *fe = *liter;
    fe->join();
    delete fe;
  }

  for (list<RGWFrontendConfig *>::iterator liter = configs.begin(); liter != configs.end(); ++liter) {
    RGWFrontendConfig *fec = *liter;
    delete fec;
  }

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGTERM, handle_sigterm);
  unregister_async_signal_handler(SIGINT, handle_sigterm);
  unregister_async_signal_handler(SIGUSR1, handle_sigterm);
  shutdown_async_signal_handler();

  if (do_swift) {
    swift_finalize();
  }

  rgw_log_usage_finalize();

  delete olog;

  rgw_perf_stop(g_ceph_context);

  RGWStoreManager::close_storage(store);

  rgw_tools_cleanup();
  rgw_shutdown_resolver();
  curl_global_cleanup();

  dout(1) << "final shutdown" << dendl;
  g_ceph_context->put();

  ceph::crypto::shutdown();

  signal_fd_finalize();

  return 0;
}

