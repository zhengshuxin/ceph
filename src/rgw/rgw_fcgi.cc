
#include <errno.h>
#include "rgw_fcgi.h"

#include "acconfig.h"
#ifdef FASTCGI_INCLUDE_DIR
# include "fastcgi/fcgiapp.h"
#else
# include "fcgiapp.h"
#endif


int RGWFCGX::write_data(const char *buf, int len)
{
  int r = FCGX_PutStr(buf, len, fcgx->out);
  if (r < 0)
    return -errno;
  return r;
}

int RGWFCGX::read_data(char *buf, int len)
{
  int r = FCGX_GetStr(buf, len, fcgx->in);
  if (r < 0)
    return -errno;
  return r;

}

void RGWFCGX::flush()
{
  FCGX_FFlush(fcgx->out);
}

const char **RGWFCGX::envp()
{
  return (const char **)fcgx->envp;
}
