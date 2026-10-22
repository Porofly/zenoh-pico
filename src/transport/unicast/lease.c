//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

#include "zenoh-pico/transport/unicast/lease.h"

#include "zenoh-pico/session/interest.h"
#include "zenoh-pico/session/liveliness.h"
#include "zenoh-pico/session/query.h"
#include "zenoh-pico/system/common/platform.h"
#include "zenoh-pico/transport/common/tx.h"
#include "zenoh-pico/transport/transport.h"
#include "zenoh-pico/transport/unicast/transport.h"
#include "zenoh-pico/utils/logging.h"

#if Z_FEATURE_UNICAST_TRANSPORT == 1

z_result_t _zp_unicast_send_keep_alive(_z_transport_unicast_t *ztu) {
    z_result_t ret = _Z_RES_OK;

    _z_transport_message_t t_msg = _z_t_msg_make_keep_alive();
    ret = _z_transport_tx_send_t_msg(&ztu->_common, &t_msg, NULL);

    return ret;
}
#else

z_result_t _zp_unicast_send_keep_alive(_z_transport_unicast_t *ztu) {
    _ZP_UNUSED(ztu);
    _Z_ERROR_RETURN(_Z_ERR_TRANSPORT_NOT_AVAILABLE);
}
#endif  // Z_FEATURE_UNICAST_TRANSPORT == 1

#if Z_FEATURE_MULTI_THREAD == 1 && Z_FEATURE_UNICAST_TRANSPORT == 1

// Longest sleep of the lease task before it checks for a failed link
#define _ZP_UNICAST_LEASE_POLL_MS 100

static void _zp_unicast_failed(_z_transport_unicast_t *ztu) {
    _z_session_rc_t zs = _z_session_weak_upgrade(&ztu->_common._session);
    if (_Z_RC_IS_NULL(&zs)) {
        // The session is being dropped, whoever drops it stops and joins this task
        _z_task_exit();
    }
    _z_session_t *session = _Z_RC_IN_VAL(&zs);
#if Z_FEATURE_AUTO_RECONNECT == 1
    _z_mutex_lock(&session->_mutex_transport);
    if (session->_closing) {
        // The session is being closed, whoever closes it stops and joins this task
        _z_mutex_unlock(&session->_mutex_transport);
        _z_session_rc_drop(&zs);
        _z_task_exit();
    }
    // From here on this task owns _tp: senders stop using it and a session close waits for us
    session->_reconnecting = true;
    session->_tp_ready = false;
    _z_mutex_unlock(&session->_mutex_transport);
#endif
#if Z_FEATURE_LIVELINESS == 1 && Z_FEATURE_SUBSCRIPTION == 1
    _z_liveliness_subscription_undeclare_all(_z_transport_common_get_session(&ztu->_common));
#endif
    // Stop the read task and wait for it: it uses the peer, buffers and link that are freed below,
    // and a detached one would keep running on the reopened transport (same memory, same socket
    // fd). It wakes up at least every socket timeout.
    ztu->_common._read_task_running = false;
    if (ztu->_common._read_task != NULL) {
        if (_z_task_join(ztu->_common._read_task) != _Z_RES_OK) {
            _Z_ERROR("Unable to join the read task");
        }
        _z_task_free(&ztu->_common._read_task);
    }
    // Not under the transport lock: on a stalled link this send can block until the socket send
    // timeout (if configured) or until TCP gives up. A link found dead cannot carry it.
    if (!ztu->_common._link_failed) {
        _z_unicast_transport_close(ztu, _Z_CLOSE_EXPIRED);
    }
    // The link is given up: reset it on close instead of lingering with unsent data
    _z_link_set_abortive_close(ztu->_common._link);
#if Z_FEATURE_AUTO_RECONNECT == 1
    // Wait for senders that started before _tp_ready was cleared
    _z_mutex_lock(&session->_mutex_transport);
#endif
    // Only this (lease) task is left to detach
    _z_unicast_transport_clear(ztu, true);
    // Without this, closing the session would clear the transport a second time
    session->_tp._type = _Z_TRANSPORT_NONE;
#if Z_FEATURE_AUTO_RECONNECT == 1
    _z_mutex_unlock(&session->_mutex_transport);

    z_result_t ret = _z_reopen(&zs);
    if (ret != _Z_RES_OK) {
        _Z_INFO("Reconnect stopped: %i", ret);
    }
    _z_mutex_lock(&session->_mutex_transport);
    session->_reconnecting = false;
    _z_mutex_unlock(&session->_mutex_transport);
#endif
    // Last access to the session. If its owner has dropped it meanwhile, this closes it here: the
    // transport is cleared and no task of it is left, so nothing is joined.
    _z_session_rc_drop(&zs);
    _z_task_exit();
}

void *_zp_unicast_lease_task(void *ztu_arg) {
    _z_transport_unicast_t *ztu = (_z_transport_unicast_t *)ztu_arg;
    ztu->_common._transmitted = false;

    int next_lease = (int)ztu->_common._lease;
    int next_keep_alive = (int)(ztu->_common._lease / Z_TRANSPORT_LEASE_EXPIRE_FACTOR);

    z_whatami_t mode = _z_transport_common_get_session(&ztu->_common)->_mode;
    _z_transport_peer_unicast_t *curr_peer = NULL;
    if (mode == Z_WHATAMI_CLIENT) {
        curr_peer = _z_transport_peer_unicast_slist_value(ztu->_peers);
        assert(curr_peer != NULL);
    }
    while (ztu->_common._lease_task_running) {
        // Process client lease
        if (mode == Z_WHATAMI_CLIENT) {
            if (ztu->_common._link_failed) {
                _Z_INFO("Closing session because the link failed");
                _zp_unicast_failed(ztu);
                return 0;
            }
            if (next_lease <= 0) {
                // Check if received data
                if (curr_peer->common._received) {
                    // Reset the lease parameters
                    curr_peer->common._received = false;
                } else {
                    // THIS LOG STRING USED IN TEST, change with caution
                    _Z_INFO("Closing session because it has expired after %zums", ztu->_common._lease);
                    _zp_unicast_failed(ztu);
                    return 0;
                }
                next_lease = (int)ztu->_common._lease;
            }
            // Next keep alive process
            if (next_keep_alive <= 0) {
                _Z_DEBUG("Sending keep alive");
                // Check if need to send a keep alive
                if (!ztu->_common._transmitted) {
                    if (_zp_unicast_send_keep_alive(ztu) < 0) {
                        // THIS LOG STRING USED IN TEST, change with caution
                        _Z_INFO("Send keep alive failed.");
                        _zp_unicast_failed(ztu);
                        return 0;
                    }
                }
                // Reset the keep alive parameters
                ztu->_common._transmitted = false;
                next_keep_alive = (int)(ztu->_common._lease / Z_TRANSPORT_LEASE_EXPIRE_FACTOR);
            }
        }
#if Z_FEATURE_UNICAST_PEER == 1
        else {  // Peer lease
            if (next_lease <= 0) {
                _z_transport_peer_unicast_slist_t *prev = NULL;
                _z_transport_peer_unicast_slist_t *prev_drop = NULL;
                _z_transport_peer_mutex_lock(&ztu->_common);
                _z_transport_peer_unicast_slist_t *curr_list = ztu->_peers;
                while (curr_list != NULL) {
                    bool drop_peer = false;
                    curr_peer = _z_transport_peer_unicast_slist_value(curr_list);
                    // Check if peer received data
                    if (curr_peer->common._received) {
                        curr_peer->common._received = false;
                    } else {
                        _Z_INFO("Deleting peer because it has expired after %zums", ztu->_common._lease);
                        drop_peer = true;
                        prev_drop = prev;
                    }
                    // Update previous only if current node is not dropped
                    if (!drop_peer) {
                        prev = curr_list;
                    }
                    // Progress list
                    curr_list = _z_transport_peer_unicast_slist_next(curr_list);
                    // Drop if needed
                    if (drop_peer) {
                        _z_session_t *zs = _z_transport_common_get_session(&ztu->_common);
                        _z_subscription_cache_invalidate(zs);
                        _z_queryable_cache_invalidate(zs);
                        _z_interest_peer_disconnected(zs, &curr_peer->common);
                        ztu->_peers = _z_transport_peer_unicast_slist_drop_element(ztu->_peers, prev_drop);
                    }
                }
                _z_transport_peer_mutex_unlock(&ztu->_common);
                next_lease = (int)ztu->_common._lease;
            }
            if (next_keep_alive <= 0) {
                if (!ztu->_common._transmitted) {
                    _Z_DEBUG("Sending keep alive");
                    // Send keep alive to all peers
                    _z_transport_message_t t_msg = _z_t_msg_make_keep_alive();
                    _z_transport_peer_mutex_lock(&ztu->_common);
                    if (!_z_transport_peer_unicast_slist_is_empty(ztu->_peers)) {
                        if (_z_transport_tx_send_t_msg(&ztu->_common, &t_msg, ztu->_peers) != _Z_RES_OK) {
                            _Z_INFO("Send keep alive failed.");
                        }
                    }
                    _z_transport_peer_mutex_unlock(&ztu->_common);
                }
                ztu->_common._transmitted = false;
                next_keep_alive = (int)(ztu->_common._lease / Z_TRANSPORT_LEASE_EXPIRE_FACTOR);
            }
        }
#endif

        // Query timeout process
        _z_pending_query_process_timeout(_z_transport_common_get_session(&ztu->_common));

        // Compute the target interval
        int interval;
        if (next_lease == 0) {
            interval = next_keep_alive;
        } else {
            interval = next_lease;
            if (next_keep_alive < interval) {
                interval = next_keep_alive;
            }
        }

        // The keep alive and lease intervals are expressed in milliseconds. Sleep in short slices
        // so that a link found dead by the read task or a sender is torn down at once.
        int slept = 0;
        while ((slept < interval) && !((mode == Z_WHATAMI_CLIENT) && ztu->_common._link_failed) &&
               ztu->_common._lease_task_running) {
            int slice = interval - slept;
            if (slice > _ZP_UNICAST_LEASE_POLL_MS) {
                slice = _ZP_UNICAST_LEASE_POLL_MS;
            }
            z_sleep_ms((size_t)slice);
            slept += slice;
        }

        next_lease = next_lease - slept;
        next_keep_alive = next_keep_alive - slept;
    }
    return 0;
}

z_result_t _zp_unicast_start_lease_task(_z_transport_t *zt, z_task_attr_t *attr, _z_task_t *task) {
    // Init memory
    (void)memset(task, 0, sizeof(_z_task_t));
    zt->_transport._unicast._common._lease_task_running = true;  // Init before z_task_init for concurrency issue
    // Init task
    if (_z_task_init(task, attr, _zp_unicast_lease_task, &zt->_transport._unicast) != _Z_RES_OK) {
        _Z_ERROR_RETURN(_Z_ERR_SYSTEM_TASK_FAILED);
    }
    // Attach task
    zt->_transport._unicast._common._lease_task = task;
    return _Z_RES_OK;
}

z_result_t _zp_unicast_stop_lease_task(_z_transport_t *zt) {
    zt->_transport._unicast._common._lease_task_running = false;
    return _Z_RES_OK;
}
#else

void *_zp_unicast_lease_task(void *ztu_arg) {
    _ZP_UNUSED(ztu_arg);
    return NULL;
}

z_result_t _zp_unicast_start_lease_task(_z_transport_t *zt, void *attr, void *task) {
    _ZP_UNUSED(zt);
    _ZP_UNUSED(attr);
    _ZP_UNUSED(task);
    _Z_ERROR_RETURN(_Z_ERR_TRANSPORT_NOT_AVAILABLE);
}

z_result_t _zp_unicast_stop_lease_task(_z_transport_t *zt) {
    _ZP_UNUSED(zt);
    _Z_ERROR_RETURN(_Z_ERR_TRANSPORT_NOT_AVAILABLE);
}
#endif
