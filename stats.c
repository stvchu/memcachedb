/*
 *  MemcacheDB - A distributed key-value storage system designed for persistent:
 *
 *      http://memcachedb.googlecode.com
 *
 *  The source code of Memcachedb is most based on Memcached:
 *
 *      http://danga.com/memcached/
 *
 *  Copyright 2008 Steve Chu.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Steve Chu <stvchu@gmail.com>
 *
 */
  
#include "memcachedb.h"
#include <db.h>

void stats_bdb(char *temp){
    char *pos = temp;
    int ret;
    u_int32_t gbytes = 0;
    u_int32_t bytes = 0;
    int ncache = 0;
    /* get bdb version */
    pos += sprintf(pos, "STAT db_ver %d.%d.%d\r\n", bdb_version.majver, 
                                                    bdb_version.minver, 
                                                    bdb_version.patch);
    /* get page size */
    if((ret = dbp->get_pagesize(dbp, &bdb_settings.page_size)) == 0){
        pos += sprintf(pos, "STAT page_size %u\r\n", bdb_settings.page_size);
    }
    
    /* get database type */
    if((ret = dbp->get_type(dbp, &bdb_settings.db_type)) == 0){
        if (bdb_settings.db_type == DB_BTREE){
            pos += sprintf(pos, "STAT db_type btree\r\n");
        }else if (bdb_settings.db_type == DB_HASH){
            pos += sprintf(pos, "STAT db_type hash\r\n");
        }
    }
    
    /* get cache size */
    if((ret = env->get_cachesize(env, &gbytes, &bytes, &ncache)) == 0){
        pos += sprintf(pos, "STAT cache_size %u/%u/%d\r\n", gbytes, bytes, ncache);
    }
    
    pos += sprintf(pos, "STAT txn_lg_bsize %u\r\n", bdb_settings.txn_lg_bsize);
    pos += sprintf(pos, "STAT txn_nosync %d\r\n", bdb_settings.txn_nosync);
    pos += sprintf(pos, "STAT log_auto_remove %d\r\n", bdb_settings.log_auto_remove);
    pos += sprintf(pos, "STAT dldetect_val %d\r\n", bdb_settings.dldetect_val);
    pos += sprintf(pos, "STAT chkpoint_val %d\r\n", bdb_settings.chkpoint_val);
    pos += sprintf(pos, "STAT memp_trickle_val %d\r\n", bdb_settings.memp_trickle_val);
    pos += sprintf(pos, "STAT memp_trickle_percent %d\r\n", bdb_settings.memp_trickle_percent);
    pos += sprintf(pos, "END");
}

void stats_rep(char *temp){
    char *pos = temp;
    int ret;
    DB_REP_STAT *statp = NULL;
    if (env->rep_stat(env, &statp, 0) == 0){
        pos += sprintf(pos, "STAT st_bulk_fills %u\r\n", statp->st_bulk_fills);
        pos += sprintf(pos, "STAT st_bulk_overflows %u\r\n", statp->st_bulk_overflows);
        pos += sprintf(pos, "STAT st_bulk_records %u\r\n", statp->st_bulk_records);
        pos += sprintf(pos, "STAT st_bulk_transfers %u\r\n", statp->st_bulk_transfers);
        pos += sprintf(pos, "STAT st_client_rerequests %u\r\n", statp->st_client_rerequests);
        pos += sprintf(pos, "STAT st_client_svc_miss %u\r\n", statp->st_client_svc_miss);
        pos += sprintf(pos, "STAT st_client_svc_req %u\r\n", statp->st_client_svc_req);
        pos += sprintf(pos, "STAT st_dupmasters %u\r\n", statp->st_dupmasters);
        pos += sprintf(pos, "STAT st_egen %u\r\n", statp->st_egen);
        pos += sprintf(pos, "STAT st_election_cur_winner %u\r\n", statp->st_election_cur_winner);
        pos += sprintf(pos, "STAT st_election_gen %u\r\n", statp->st_election_gen);
        pos += sprintf(pos, "STAT st_election_lsn %u/%u\r\n", statp->st_election_lsn.file,
                                                              statp->st_election_lsn.offset);
        pos += sprintf(pos, "STAT st_election_nsites %u\r\n", statp->st_election_nsites);
        pos += sprintf(pos, "STAT st_election_nvotes %u\r\n", statp->st_election_nvotes);
        pos += sprintf(pos, "STAT st_election_priority %u\r\n", statp->st_election_priority);
        pos += sprintf(pos, "STAT st_election_sec %u\r\n", statp->st_election_sec);
        pos += sprintf(pos, "STAT st_election_status %u\r\n", statp->st_election_status);
        pos += sprintf(pos, "STAT st_election_tiebreaker %u\r\n", statp->st_election_tiebreaker);
        pos += sprintf(pos, "STAT st_election_usec %u\r\n", statp->st_election_usec);
        pos += sprintf(pos, "STAT st_election_votes %u\r\n", statp->st_election_votes);
        pos += sprintf(pos, "STAT st_elections %u\r\n", statp->st_elections);
        pos += sprintf(pos, "STAT st_elections_won %u\r\n", statp->st_elections_won);
        pos += sprintf(pos, "STAT st_env_id %u\r\n", statp->st_env_id);
        pos += sprintf(pos, "STAT st_env_priority %u\r\n", statp->st_env_priority);
        pos += sprintf(pos, "STAT st_gen %u\r\n", statp->st_gen);
        pos += sprintf(pos, "STAT st_log_duplicated %u\r\n", statp->st_log_duplicated);
        pos += sprintf(pos, "STAT st_log_queued %u\r\n", statp->st_log_queued);
        pos += sprintf(pos, "STAT st_log_queued_max %u\r\n", statp->st_log_queued_max);
        pos += sprintf(pos, "STAT st_log_queued_total %u\r\n", statp->st_log_queued_total);
        pos += sprintf(pos, "STAT st_log_records %u\r\n", statp->st_log_records);
        pos += sprintf(pos, "STAT st_log_requested %u\r\n", statp->st_log_requested);
        pos += sprintf(pos, "STAT st_master %u\r\n", statp->st_master);
        pos += sprintf(pos, "STAT st_master_changes %u\r\n", statp->st_master_changes);
        pos += sprintf(pos, "STAT st_max_lease_sec %u\r\n", statp->st_max_lease_sec);
        pos += sprintf(pos, "STAT st_max_lease_usec %u\r\n", statp->st_max_lease_usec);
        pos += sprintf(pos, "STAT st_max_perm_lsn %u/%u\r\n", statp->st_max_perm_lsn.file,
                                                              statp->st_max_perm_lsn.offset);
        pos += sprintf(pos, "STAT st_msgs_badgen %u\r\n", statp->st_msgs_badgen);
        pos += sprintf(pos, "STAT st_msgs_processed %u\r\n", statp->st_msgs_processed);
        pos += sprintf(pos, "STAT st_msgs_recover %u\r\n", statp->st_msgs_recover);
        pos += sprintf(pos, "STAT st_msgs_send_failures %u\r\n", statp->st_msgs_send_failures);
        pos += sprintf(pos, "STAT st_msgs_sent %u\r\n", statp->st_msgs_sent);
        pos += sprintf(pos, "STAT st_newsites %u\r\n", statp->st_newsites);
        pos += sprintf(pos, "STAT st_next_lsn %u/%u\r\n", statp->st_next_lsn.file, 
                                                          statp->st_next_lsn.offset);
        pos += sprintf(pos, "STAT st_next_pg %u\r\n", statp->st_next_pg);
        pos += sprintf(pos, "STAT st_nsites %u\r\n", statp->st_nsites);
        pos += sprintf(pos, "STAT st_nthrottles %u\r\n", statp->st_nthrottles);
        pos += sprintf(pos, "STAT st_outdated %u\r\n", statp->st_outdated);
        pos += sprintf(pos, "STAT st_pg_duplicated %u\r\n", statp->st_pg_duplicated);
        pos += sprintf(pos, "STAT st_pg_records %u\r\n", statp->st_pg_records);
        pos += sprintf(pos, "STAT st_pg_requested %u\r\n", statp->st_pg_requested);
        pos += sprintf(pos, "STAT st_startsync_delayed %u\r\n", statp->st_startsync_delayed);
        pos += sprintf(pos, "STAT st_startup_complete %u\r\n", statp->st_startup_complete);
        pos += sprintf(pos, "STAT st_status %u\r\n", statp->st_status);
        pos += sprintf(pos, "STAT st_txns_applied %u\r\n", statp->st_txns_applied);
        pos += sprintf(pos, "STAT st_waiting_lsn %u/%u\r\n", statp->st_waiting_lsn.file,    
                                                             statp->st_waiting_lsn.offset);
        pos += sprintf(pos, "STAT st_waiting_pg %u\r\n", statp->st_waiting_pg);
    }
    if (statp != NULL)
        free(statp);

    pos += sprintf(pos, "END");
}

void stats_repmgr(char *temp){
    char *pos = temp;
    int ret;
    DB_REPMGR_STAT *statp = NULL;
    if (env->repmgr_stat(env, &statp, 0) == 0){
        pos += sprintf(pos, "STAT st_perm_failed %u\r\n", statp->st_perm_failed);
        pos += sprintf(pos, "STAT st_msgs_queued %u\r\n", statp->st_msgs_queued);
        pos += sprintf(pos, "STAT st_msgs_dropped %u\r\n", statp->st_msgs_dropped);
        pos += sprintf(pos, "STAT st_connection_drop %u\r\n", statp->st_connection_drop);
        pos += sprintf(pos, "STAT st_connect_fail %u\r\n", statp->st_connect_fail);
    }
    if (statp != NULL)
        free(statp);

    pos += sprintf(pos, "END");
}

void stats_repcfg(char *temp){
    char *pos = temp;
    int ret;
    if (env->rep_get_priority(env, &bdb_settings.rep_priority) == 0){
        pos += sprintf(pos, "STAT rep_priority %d\r\n", bdb_settings.rep_priority);
    }
    if (env->repmgr_get_ack_policy(env, &bdb_settings.rep_ack_policy) == 0){
        pos += sprintf(pos, "STAT rep_ack_policy %d\r\n", bdb_settings.rep_ack_policy);
    }
    /* timeout configure */
    if (env->rep_get_timeout(env, DB_REP_ACK_TIMEOUT, &bdb_settings.rep_ack_timeout) == 0){
        pos += sprintf(pos, "STAT rep_ack_timeout %u\r\n", bdb_settings.rep_ack_timeout);
    }
    if (env->rep_get_timeout(env, DB_REP_CHECKPOINT_DELAY, &bdb_settings.rep_chkpoint_delay) == 0){
        pos += sprintf(pos, "STAT rep_chkpoint_delay %u\r\n", bdb_settings.rep_chkpoint_delay);
    }
    if (env->rep_get_timeout(env, DB_REP_CONNECTION_RETRY, &bdb_settings.rep_conn_retry) == 0){
        pos += sprintf(pos, "STAT rep_conn_retry %u\r\n", bdb_settings.rep_conn_retry);
    }
    if (env->rep_get_timeout(env, DB_REP_ELECTION_TIMEOUT, &bdb_settings.rep_elect_timeout) == 0){
        pos += sprintf(pos, "STAT rep_elect_timeout %u\r\n", bdb_settings.rep_elect_timeout);
    }
    if (env->rep_get_timeout(env, DB_REP_ELECTION_RETRY, &bdb_settings.rep_elect_retry) == 0){
        pos += sprintf(pos, "STAT rep_elect_retry %u\r\n", bdb_settings.rep_elect_retry);
    }
    /*
    if (env->rep_get_timeout(env, DB_REP_HEARTBEAT_MONITOR, &bdb_settings.rep_heartbeat_monitor) == 0){
        pos += sprintf(pos, "STAT rep_heartbeat_monitor %u\r\n", bdb_settings.rep_heartbeat_monitor);
    }
    if (env->rep_get_timeout(env, DB_REP_HEARTBEAT_SEND, &bdb_settings.rep_heartbeat_send) == 0){
        pos += sprintf(pos, "STAT rep_heartbeat_send %u\r\n", bdb_settings.rep_heartbeat_send);
    }*/
    /*
    if (env->rep_get_timeout(env, DB_REP_LEASE_TIMEOUT, &bdb_settings.rep_lease_timeout) == 0){
        pos += sprintf(pos, "STAT rep_lease_timeout %u\r\n", bdb_settings.rep_lease_timeout);
    }*/
    
    /* flag configure */
    if (env->rep_get_config(env, DB_REP_CONF_BULK, &bdb_settings.rep_bulk) == 0){
        pos += sprintf(pos, "STAT rep_bulk %d\r\n", bdb_settings.rep_bulk);
    }
    /*
    if (env->rep_get_config(env, DB_REP_CONF_LEASE, &bdb_settings.rep_lease) == 0){
        pos += sprintf(pos, "STAT rep_lease %d\r\n", bdb_settings.rep_lease);
    }*/
    if (env->rep_get_request(env, &bdb_settings.rep_req_min, &bdb_settings.rep_req_max) == 0){ 
        pos += sprintf(pos, "STAT rep_request %u/%u\r\n", bdb_settings.rep_req_min, 
                                                          bdb_settings.rep_req_max);
    }
    /* 
    if (env->rep_get_clockskew(env, &bdb_settings.rep_fast_clock, &bdb_settings.rep_slow_clock) == 0){ 
        pos += sprintf(pos, "STAT rep_clock %u/%u\r\n", bdb_settings.rep_fast_clock, 
                                                        bdb_settings.rep_slow_clock);
    } */
    if (env->rep_get_limit(env, &bdb_settings.rep_limit_gbytes, &bdb_settings.rep_limit_bytes) == 0){ 
        pos += sprintf(pos, "STAT rep_limit %u/%u\r\n", bdb_settings.rep_limit_gbytes, 
                                                        bdb_settings.rep_limit_bytes);
    } 
    if (env->rep_get_nsites(env, &bdb_settings.rep_nsites) == 0){ 
        pos += sprintf(pos, "STAT rep_nsites %u\r\n", bdb_settings.rep_nsites);
    } 
    pos += sprintf(pos, "END");
}

void stats_repms(char *temp){
    char *pos = temp;
    int ret;
    DB_REPMGR_SITE *list = NULL;
    u_int count, i;
    
    /* stats myself */
    if (bdb_settings.rep_master_eid == DB_EID_INVALID){
        pos += sprintf(pos, "STAT site-00 %s:%d/UNKNOWN/--\r\n", bdb_settings.rep_localhost,
                                                                 bdb_settings.rep_localport);
    } else if (bdb_settings.rep_master_eid == BDB_EID_SELF){
        pos += sprintf(pos, "STAT site-00 %s:%d/MASTER/--\r\n", bdb_settings.rep_localhost,
                                                                bdb_settings.rep_localport);
    } else {
        pos += sprintf(pos, "STAT site-00 %s:%d/CLIENT/--\r\n", bdb_settings.rep_localhost,
                                                                bdb_settings.rep_localport);
    }
    
    /* stats others */
    if ((0 == env->repmgr_site_list(env, &count, &list))) { 
        for (i = 0; i < count; ++i) {
            pos += sprintf(pos, "STAT site-%02d %s:%d/%s/%s\r\n", i+1, list[i].host, list[i].port,
                           (bdb_settings.rep_master_eid == list[i].eid ? "MASTER" : "CLIENT"),           
                           (list[i].status == DB_REPMGR_CONNECTED ? "CONNECTED": "DISCONNECTED"));
        }
    }
    if (list != NULL)
        free(list);
    pos += sprintf(pos, "END");
}