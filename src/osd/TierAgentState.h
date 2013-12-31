// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Sage Weil <sage@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_OSD_TIERAGENT_H
#define CEPH_OSD_TIERAGENT_H

struct TierAgentState {
  /// current position iterating across pool
  hobject_t position;

  /// histogram of ages we've encountered
  pow2_hist_t age_histogram;

  /// past HitSet(s) (not current)
  list<HitSet*> hit_set_map;

  /// a few recent things we've seen that are clean
  list<hobject_t> recent_clean;

  enum flush_mode_t {
    FLUSH_MODE_IDLE,   // nothing to flush
    FLUSH_MODE_ACTIVE, // flush what we can to bring down dirty count
  } flush_mode;     ///< current flush behavior
  static const char *get_flush_mode_name(flush_mode_t m) {
    switch (m) {
    case FLUSH_MODE_IDLE: return "idle";
    case FLUSH_MODE_ACTIVE: return "active";
    default: assert(0 == "bad flush mode");
    }
  }
  const char *get_flush_mode_name() const {
    return get_flush_mode_name(flush_mode);
  }

  enum evict_mode_t {
    EVICT_MODE_IDLE,      // no need to evict anything
    EVICT_MODE_SOME,      // evict some things as we are near the target
    EVICT_MODE_FULL,      // evict anything
  } evict_mode;     ///< current evict behavior
  static const char *get_evict_mode_name(evict_mode_t m) {
    switch (m) {
    case EVICT_MODE_IDLE: return "idle";
    case EVICT_MODE_SOME: return "some";
    case EVICT_MODE_FULL: return "full";
    default: assert(0 == "bad evict mode");
    }
  }
  const char *get_evict_mode_name() const {
    return get_evict_mode_name(evict_mode);
  }

  TierAgentState()
    : flush_mode(FLUSH_MODE_IDLE),
      evict_mode(EVICT_MODE_IDLE)
  {}

  /// false if we have any work to do
  bool is_idle() const {
    return
      flush_mode == FLUSH_MODE_IDLE &&
      evict_mode == EVICT_MODE_IDLE;
  }

  /// estimate object age
  ///
  /// @param obc the object
  /// @param access_age seconds since last access (lower bound)
  /// @param temperature relative temperature (# hitset bins we appear in)
  void agent_estimate_age(ObjectContextRef& obc,
			  int *access_age,
			  int *temperatue);
};

#endif
