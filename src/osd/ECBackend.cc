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

#include "ECBackend.h"

PGBackend::RecoveryHandle *open_recovery_op()
{
	return 0;
}

void ECBackend::run_recovery_op(
	RecoveryHandle *h,
	int priority)
{
}

void ECBackend::recover_object(
	const hobject_t &hoid,
	ObjectContextRef head,
	ObjectContextRef obc,
	RecoveryHandle *h)
{
}

bool ECBackend::handle_message(
	OpRequestRef op
	)
{
	return false;
}

void ECBackend::check_recovery_sources(const OSDMapRef osdmap)
{
}

void ECBackend::on_change(ObjectStore::Transaction *t)
{
}

void ECBackend::clear_state()
{
}

void ECBackend::on_flushed()
{
}


void ECBackend::split_colls(
	pg_t child,
	int split_bits,
	int seed,
	ObjectStore::Transaction *t)
{
}

void ECBackend::temp_colls(list<coll_t> *out)
{
}

void ECBackend::dump_recovery_info(Formatter *f) const
{
}

coll_t ECBackend::get_temp_coll(ObjectStore::Transaction *t)
{
	return coll_t();
}

void ECBackend::add_temp_obj(const hobject_t &oid)
{
}

void ECBackend::clear_temp_obj(const hobject_t &oid)
{
}

PGBackend::PGTransaction *ECBackend::get_transaction()
{
	return 0;
}

void ECBackend::submit_transaction(
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
	OpRequestRef op)
{
}

void ECBackend::rollback_setattrs(
	const hobject_t &hoid,
	map<string, boost::optional<bufferlist> > &old_attrs,
	ObjectStore::Transaction *t)
{
}

void ECBackend::rollback_append(
	const hobject_t &hoid,
	uint64_t old_size,
	ObjectStore::Transaction *t)
{
}

void ECBackend::rollback_unstash(
	const hobject_t &hoid,
	version_t old_version,
	ObjectStore::Transaction *t)
{
}

void ECBackend::rollback_create(
	const hobject_t &hoid,
	ObjectStore::Transaction *t)
{
}

void ECBackend::trim_stashed_object(
	const hobject_t &hoid,
	version_t stashed_version,
	ObjectStore::Transaction *t)
{
}

int ECBackend::objects_list_partial(
	const hobject_t &begin,
	int min,
	int max,
	snapid_t seq,
	vector<hobject_t> *ls,
	hobject_t *next)
{
	return 0;
}

int ECBackend::objects_list_range(
	const hobject_t &start,
	const hobject_t &end,
	snapid_t seq,
	vector<hobject_t> *ls)
{
	return 0;
}

int objects_get_attr(
	const hobject_t &hoid,
	const string &attr,
	bufferlist *out)
{
	return 0;
}

int ECBackend::objects_get_attrs(
	const hobject_t &hoid,
	map<string, bufferlist> *out)
{
	return 0;
}

int ECBackend::objects_read_sync(
	const hobject_t &hoid,
	uint64_t off,
	uint64_t len,
	bufferlist *bl)
{
	return -EOPNOTSUPP;
}

void ECBackend::objects_read_async(
	const hobject_t &hoid,
	uint64_t off,
	uint64_t len,
	bufferlist *bl,
	Context *on_complete)
{
	return;
}
