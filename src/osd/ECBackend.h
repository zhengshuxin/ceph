// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef ECBACKEND_H
#define ECBACKEND_H

#include "OSD.h"
#include "PGBackend.h"
#include "osd_types.h"

class ECBackend : public PGBackend {
public:
  RecoveryHandle *open_recovery_op();

  void run_recovery_op(
    RecoveryHandle *h,
    int priority
    );

  void recover_object(
    const hobject_t &hoid,
    ObjectContextRef head,
    ObjectContextRef obc,
    RecoveryHandle *h
    );

  bool handle_message(
    OpRequestRef op
    );

  void check_recovery_sources(const OSDMapRef osdmap);

  void on_change(ObjectStore::Transaction *t) ;
  void clear_state();

  void on_flushed();


  void split_colls(
    pg_t child,
    int split_bits,
    int seed,
    ObjectStore::Transaction *t);

  void temp_colls(list<coll_t> *out);

  void dump_recovery_info(Formatter *f) const;

  coll_t get_temp_coll(ObjectStore::Transaction *t);
  void add_temp_obj(const hobject_t &oid);
  void clear_temp_obj(const hobject_t &oid);

  PGTransaction *get_transaction();

  void submit_transaction(
    const hobject_t &hoid,
    const eversion_t &at_version,
    PGTransaction *t,
    const eversion_t &trim_to,
    vector<pg_log_entry_t> &log_entries,
    Context *on_local_applied_sync,
    Context *on_all_applied,
    Context *on_all_commit,
    tid_t tid,
    osd_reqid_t reqid,
    OpRequestRef op
    );

  void rollback_setattrs(
    const hobject_t &hoid,
    map<string, boost::optional<bufferlist> > &old_attrs,
    ObjectStore::Transaction *t);

  void rollback_append(
    const hobject_t &hoid,
    uint64_t old_size,
    ObjectStore::Transaction *t);

  void rollback_unstash(
    const hobject_t &hoid,
    version_t old_version,
    ObjectStore::Transaction *t);

  void rollback_create(
    const hobject_t &hoid,
    ObjectStore::Transaction *t);

  void trim_stashed_object(
    const hobject_t &hoid,
    version_t stashed_version,
    ObjectStore::Transaction *t);

  int objects_list_partial(
    const hobject_t &begin,
    int min,
    int max,
    snapid_t seq,
    vector<hobject_t> *ls,
    hobject_t *next);

  int objects_list_range(
    const hobject_t &start,
    const hobject_t &end,
    snapid_t seq,
    vector<hobject_t> *ls);

  int objects_get_attr(
    const hobject_t &hoid,
    const string &attr,
    bufferlist *out);

  int objects_get_attrs(
    const hobject_t &hoid,
    map<string, bufferlist> *out);

  int objects_read_sync(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    bufferlist *bl);

  void objects_read_async(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    bufferlist *bl,
    Context *on_complete);
};


#endif
