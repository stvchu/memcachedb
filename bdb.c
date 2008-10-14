/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  Copyright 2008 Steve Chu.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Steve Chu <stvchu@gmail.com>
 *
 *  $Id: bdb.c 2008-01-22 17:27:13Z steve $
 */

#include "memcachedb.h"
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <signal.h>
#include <db.h>

static void *bdb_chkpoint_thread __P((void *));
static void *bdb_memp_trickle_thread __P((void *));
static void *bdb_dl_detect_thread __P((void *));
static void bdb_event_callback __P((DB_ENV *, u_int32_t, void *));
static void bdb_err_callback(const DB_ENV *dbenv, const char *errpfx, const char *msg);
static void bdb_msg_callback(const DB_ENV *dbenv, const char *msg);


static pthread_t chk_ptid;
static pthread_t mtri_ptid;
static pthread_t dld_ptid;

void bdb_settings_init(void)
{
    bdb_settings.db_file = DBFILE;
    bdb_settings.env_home = DBHOME;
    bdb_settings.cache_size = 64 * 1024 * 1024; /* default is 64MB */ 
    bdb_settings.txn_lg_bsize = 32 * 1024; /* default is 32KB */ 
    bdb_settings.page_size = 4096;  /* default is 4K */
    bdb_settings.db_type = DB_BTREE;
    bdb_settings.txn_nosync = 0; /* default DB_TXN_NOSYNC is off */
    bdb_settings.dldetect_val = 100 * 1000; /* default is 100 millisecond */
    bdb_settings.chkpoint_val = 60 * 5;
    bdb_settings.memp_trickle_val = 30;
    bdb_settings.memp_trickle_percent = 60; 
    bdb_settings.db_flags = DB_CREATE | DB_AUTO_COMMIT;
    bdb_settings.env_flags = DB_CREATE
                          | DB_INIT_LOCK 
                          | DB_THREAD 
                          | DB_INIT_MPOOL 
                          | DB_INIT_LOG 
                          | DB_INIT_TXN
                          | DB_RECOVER;
                              
    bdb_settings.is_replicated = 0;
    bdb_settings.rep_localhost = "127.0.0.1"; /* local host in replication */
    bdb_settings.rep_localport = 31211;  /* local port in replication */
    bdb_settings.rep_remotehost = NULL; /* local host in replication */
    bdb_settings.rep_remoteport = 0;  /* local port in replication */
    bdb_settings.rep_whoami = MDB_UNKNOWN;
    bdb_settings.rep_master_eid = DB_EID_INVALID;
    bdb_settings.rep_start_policy = DB_REP_ELECTION;
    bdb_settings.rep_nsites = 2;
    bdb_settings.rep_ack_policy = DB_REPMGR_ACKS_ONE_PEER;

    bdb_settings.rep_ack_timeout = 50 * 1000;  /* 50ms */
    bdb_settings.rep_chkpoint_delay = 0;
    bdb_settings.rep_conn_retry = 30 * 1000 * 1000; /* 3 seconds*/
    bdb_settings.rep_elect_timeout = 5 * 1000 * 1000; /* 5 seconds */
    bdb_settings.rep_elect_retry = 10 * 1000 * 1000; /* 10 seconds */
    bdb_settings.rep_heartbeat_monitor = 80 * 1000 * 1000; /* 60 seconds */
    bdb_settings.rep_heartbeat_send = 60 * 1000 * 1000; /* 60 seconds */
    bdb_settings.rep_lease_timeout = 0; /* now never used */

    bdb_settings.rep_bulk = 1;
    bdb_settings.rep_lease = 0; /* now never used */

    bdb_settings.rep_priority = 100;

    bdb_settings.rep_req_min = 40000;
    bdb_settings.rep_req_max = 1280000;

    bdb_settings.rep_fast_clock = 102;  /* now never used */
    bdb_settings.rep_slow_clock = 100;

    bdb_settings.rep_limit_gbytes = 0;  
    bdb_settings.rep_limit_bytes = 10 * 1024 * 1024; /* 10MB */
}

void bdb_env_init(void){
    int ret;
    /* db env init */
    if ((ret = db_env_create(&env, 0)) != 0) {
        fprintf(stderr, "db_env_create: %s\n", db_strerror(ret));
        exit(EXIT_FAILURE);
    }

    /* set err&msg display */
    env->set_errpfx(env, PACKAGE);
    /* env->set_errfile(env, stderr); */
    /* env->set_msgfile(env, stderr); */
	env->set_errcall(env, bdb_err_callback);
  	env->set_msgcall(env, bdb_msg_callback);

    /* set BerkeleyDB verbose*/
    if (settings.verbose > 1) {
        if ((ret = env->set_verbose(env, DB_VERB_FILEOPS_ALL, 1)) != 0) {
            fprintf(stderr, "env->set_verbose[DB_VERB_FILEOPS_ALL]: %s\n",
                    db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        if ((ret = env->set_verbose(env, DB_VERB_DEADLOCK, 1)) != 0) {
            fprintf(stderr, "env->set_verbose[DB_VERB_DEADLOCK]: %s\n",
                    db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        if ((ret = env->set_verbose(env, DB_VERB_RECOVERY, 1)) != 0) {
            fprintf(stderr, "env->set_verbose[DB_VERB_RECOVERY]: %s\n",
                    db_strerror(ret));
            exit(EXIT_FAILURE);
        }
    }

    /* set MPOOL size */
    env->set_cachesize(env, 0, bdb_settings.cache_size, 0);

    /* set DB_TXN_NOSYNC flag */
    if (bdb_settings.txn_nosync){
        env->set_flags(env, DB_TXN_NOSYNC, 1);
    }

    /* set locking */
    env->set_lk_max_lockers(env, 20000);
    env->set_lk_max_locks(env, 20000);
    env->set_lk_max_objects(env, 20000);

    /* at least max active transactions */
  	env->set_tx_max(env, 10000);

    /* set transaction log buffer */
    env->set_lg_bsize(env, bdb_settings.txn_lg_bsize);
    
    /* if no home dir existed, we create it */
    if (0 != access(bdb_settings.env_home, F_OK)) {
        if (0 != mkdir(bdb_settings.env_home, 0750)) {
            fprintf(stderr, "mkdir env_home error:[%s]\n", bdb_settings.env_home);
            exit(EXIT_FAILURE);
        }
    }
    
    if(bdb_settings.is_replicated) {
        bdb_settings.env_flags |= DB_INIT_REP;

        /* verbose messages that help us to debug */
        if (settings.verbose > 1) {
            if ((ret = env->set_verbose(env, DB_VERB_REPLICATION, 1)) != 0) {
                fprintf(stderr, "env->set_verbose[DB_VERB_REPLICATION]: %s\n",
                        db_strerror(ret));
                exit(EXIT_FAILURE);
            }
        }

        /* set up the event hook, so we can do logging when something happens */
        env->set_event_notify(env, bdb_event_callback);

        /* ack policy can have a great impact in performance, lantency and consistency */
        env->repmgr_set_ack_policy(env, bdb_settings.rep_ack_policy);

        /* timeout configs */
        env->rep_set_timeout(env, DB_REP_ACK_TIMEOUT, bdb_settings.rep_ack_timeout);
        env->rep_set_timeout(env, DB_REP_CHECKPOINT_DELAY, bdb_settings.rep_chkpoint_delay);
        env->rep_set_timeout(env, DB_REP_CONNECTION_RETRY, bdb_settings.rep_conn_retry);
        env->rep_set_timeout(env, DB_REP_ELECTION_TIMEOUT, bdb_settings.rep_elect_timeout);
        env->rep_set_timeout(env, DB_REP_ELECTION_RETRY, bdb_settings.rep_elect_retry);

        /* notice:
           The "monitor" time should always be at least a little bit longer than the "send" time */
        env->rep_set_timeout(env, DB_REP_HEARTBEAT_MONITOR, bdb_settings.rep_heartbeat_monitor);
        env->rep_set_timeout(env, DB_REP_HEARTBEAT_SEND, bdb_settings.rep_heartbeat_send);

        /* Bulk transfers simply cause replication messages to accumulate
           in a buffer until a triggering event occurs.  
	    	   Bulk transfer occurs when:
           1. Bulk transfers are configured for the master environment, and
           2. the message buffer is full or
           3. a permanent record (for example, a transaction commit or a checkpoint record)
		   is placed in the buffer for the replica.
        */
        env->rep_set_config(env, DB_REP_CONF_BULK, bdb_settings.rep_bulk);

        /* we now never use master lease */
        /*
        env->rep_set_timeout(env, DB_REP_LEASE_TIMEOUT, bdb_settings.rep_lease_timeout);
        env->rep_set_config(env, DB_REP_CONF_LEASE, bdb_settings.rep_lease);
        env->rep_set_clockskew(env, bdb_settings.rep_fast_clock, bdb_settings.rep_slow_clock);
        */

        env->rep_set_priority(env, bdb_settings.rep_priority);
        env->rep_set_request(env, bdb_settings.rep_req_min, bdb_settings.rep_req_max);
		env->rep_set_limit(env, bdb_settings.rep_limit_gbytes, bdb_settings.rep_limit_bytes);

        /* publish the local site communication channel */
        if ((ret = env->repmgr_set_local_site(env, bdb_settings.rep_localhost, bdb_settings.rep_localport, 0)) != 0) {
            fprintf(stderr, "repmgr_set_local_site[%s:%d]: %s\n", 
                    bdb_settings.rep_localhost, bdb_settings.rep_localport, db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        /* add a remote site, mostly this is a master */
        if(NULL != bdb_settings.rep_remotehost) {
            if ((ret = env->repmgr_add_remote_site(env, bdb_settings.rep_remotehost, bdb_settings.rep_remoteport, NULL, 0)) != 0) {
                fprintf(stderr, "repmgr_add_remote_site[%s:%d]: %s\n", 
                        bdb_settings.rep_remotehost, bdb_settings.rep_remoteport, db_strerror(ret));
                exit(EXIT_FAILURE);
            }
        }
        /* nsite is important for electing, default nvotes is (nsite/2 + 1)
           if nsite is equel to 2, then nvotes is 1 */
        if ((ret = env->rep_set_nsites(env, bdb_settings.rep_nsites)) != 0) {
            fprintf(stderr, "rep_set_nsites: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }
    }

    if ((ret = env->open(env, bdb_settings.env_home, bdb_settings.env_flags, 0)) != 0) {
        fprintf(stderr, "db_env_open: %s\n", db_strerror(ret));
        exit(EXIT_FAILURE);
    }

    if(bdb_settings.is_replicated) {
        /* repmgr_start must run after daemon !!!*/
        if ((ret = env->repmgr_start(env, 3, bdb_settings.rep_start_policy)) != 0) {
            fprintf(stderr, "env->repmgr_start: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        /* sleep 5 second for electing or client startup */
        if (bdb_settings.rep_start_policy == DB_REP_ELECTION ||
            bdb_settings.rep_start_policy == DB_REP_CLIENT) {
            sleep(5);
        }
    }
}


void bdb_db_open(void){
    int ret;
    int db_open = 0;
    /* for replicas to get a full master copy, then open db */
    while(!db_open) {
        /* if replica, just scratch the db file from a master */
        if (1 == bdb_settings.is_replicated){
            if (MDB_CLIENT == bdb_settings.rep_whoami) {
                bdb_settings.db_flags = DB_AUTO_COMMIT;
            }else if (MDB_MASTER == bdb_settings.rep_whoami) {
                bdb_settings.db_flags = DB_CREATE | DB_AUTO_COMMIT;
            }else{
                /* do nothing */
            }
        }

        bdb_db_close();

        if ((ret = db_create(&dbp, env, 0)) != 0) {
            fprintf(stderr, "db_create: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }
        /* set page size */
        if((ret = dbp->set_pagesize(dbp, bdb_settings.page_size)) != 0){
            fprintf(stderr, "dbp->set_pagesize: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }

        /* try to open db*/
        ret = dbp->open(dbp, NULL, bdb_settings.db_file, NULL, bdb_settings.db_type, bdb_settings.db_flags, 0664);         
        switch (ret){
        case 0:
            db_open = 1;
            break;
        case ENOENT:
        case DB_LOCK_DEADLOCK:
        case DB_REP_LOCKOUT:
            fprintf(stderr, "db_open: %s\n", db_strerror(ret));
            sleep(3);
            break;
        default:
            fprintf(stderr, "db_open: %s\n", db_strerror(ret));
            exit(EXIT_FAILURE);
        }
    }

}

void start_chkpoint_thread(void){
    if (bdb_settings.chkpoint_val > 0){
        /* Start a checkpoint thread. */
        if ((errno = pthread_create(
            &chk_ptid, NULL, bdb_chkpoint_thread, (void *)env)) != 0) {
            fprintf(stderr,
                "failed spawning checkpoint thread: %s\n",
                strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
}

void start_memp_trickle_thread(void){
    if (bdb_settings.memp_trickle_val > 0){
        /* Start a memp_trickle thread. */
        if ((errno = pthread_create(
            &mtri_ptid, NULL, bdb_memp_trickle_thread, (void *)env)) != 0) {
            fprintf(stderr,
                "failed spawning memp_trickle thread: %s\n",
                strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
}

void start_dl_detect_thread(void){
    if (bdb_settings.dldetect_val > 0){
        /* Start a deadlock detecting thread. */
        if ((errno = pthread_create(
            &dld_ptid, NULL, bdb_dl_detect_thread, (void *)env)) != 0) {
            fprintf(stderr,
                "failed spawning deadlock thread: %s\n",
                strerror(errno));
            exit(EXIT_FAILURE);
        }
    }
}

static void *bdb_chkpoint_thread(void *arg)
{
    DB_ENV *dbenv;
    int ret;
    dbenv = arg;
    if (settings.verbose > 1) {
        dbenv->errx(dbenv, "checkpoint thread created: %lu, every %d seconds", 
                           (u_long)pthread_self(), bdb_settings.chkpoint_val);
    }
    for (;; sleep(bdb_settings.chkpoint_val)) {
        if ((ret = dbenv->txn_checkpoint(dbenv, 0, 0, 0)) != 0) {
            dbenv->err(dbenv, ret, "checkpoint thread");
        }
        dbenv->errx(dbenv, "checkpoint thread: a txn_checkpoint is done");
    }
    return (NULL);
}

static void *bdb_memp_trickle_thread(void *arg)
{
    DB_ENV *dbenv;
    int ret, nwrotep;
    dbenv = arg;
    if (settings.verbose > 1) {
        dbenv->errx(dbenv, "memp_trickle thread created: %lu, every %d seconds, %d%% pages should be clean.", 
                           (u_long)pthread_self(), bdb_settings.memp_trickle_val,
                           bdb_settings.memp_trickle_percent);
    }
    for (;; sleep(bdb_settings.memp_trickle_val)) {
        if ((ret = dbenv->memp_trickle(dbenv, bdb_settings.memp_trickle_percent, &nwrotep)) != 0) {
            dbenv->err(dbenv, ret, "memp_trickle thread");
        }
        dbenv->errx(dbenv, "memp_trickle thread: writing %d dirty pages", nwrotep);
    }
    return (NULL);
}

static void *bdb_dl_detect_thread(void *arg)
{
    DB_ENV *dbenv;
    struct timeval t;
    dbenv = arg;
    if (settings.verbose > 1) {
        dbenv->errx(dbenv, "deadlock detecting thread created: %lu, every %d millisecond",
                           (u_long)pthread_self(), bdb_settings.dldetect_val);
    }
    while (!daemon_quit) {
        t.tv_sec = 0;
        t.tv_usec = bdb_settings.dldetect_val;
        (void)dbenv->lock_detect(dbenv, 0, DB_LOCK_YOUNGEST, NULL);
        /* select is a more accurate sleep timer */
        (void)select(0, NULL, NULL, NULL, &t);
    }
    return (NULL);
}

static void bdb_event_callback(DB_ENV *env, u_int32_t which, void *info)
{
    switch (which) {
    case DB_EVENT_PANIC:
        env->errx(env, "evnet: DB_EVENT_PANIC, we got panic, recovery should be run.");
        break;
    case DB_EVENT_REP_CLIENT:
        env->errx(env, "event: DB_EVENT_REP_CLIENT, I<%s:%d> am now a replication client.", 
                       bdb_settings.rep_localhost, bdb_settings.rep_localport);
        bdb_settings.rep_whoami = MDB_CLIENT;
        break;
    case DB_EVENT_REP_ELECTED:
        env->errx(env, "event: DB_EVENT_REP_ELECTED, I<%s:%d> has just won an election.", 
                       bdb_settings.rep_localhost, bdb_settings.rep_localport);
        break;
    case DB_EVENT_REP_MASTER:
        env->errx(env, "event: DB_EVENT_REP_MASTER, I<%s:%d> am now a replication master.", 
                       bdb_settings.rep_localhost, bdb_settings.rep_localport);
        bdb_settings.rep_whoami = MDB_MASTER;
        bdb_settings.rep_master_eid = BDB_EID_SELF;
        break;
    case DB_EVENT_REP_NEWMASTER:
        bdb_settings.rep_master_eid = *(int*)info;
        env->errx(env, "event: DB_EVENT_REP_NEWMASTER, a new master<eid: %d> has been established, "
                       "but not me<%s:%d>", bdb_settings.rep_master_eid,
                       bdb_settings.rep_localhost, bdb_settings.rep_localport);
        break;
    case DB_EVENT_REP_PERM_FAILED:
        env->errx(env, "event: DB_EVENT_REP_PERM_FAILED, insufficient acks, "
                       "the master will flush the txn log buffer");
        break;
    case DB_EVENT_REP_STARTUPDONE: 
        if (bdb_settings.rep_whoami == MDB_CLIENT){
            env->errx(env, "event: DB_EVENT_REP_STARTUPDONE, I has completed startup synchronization and"
                           " is now processing live log records received from the master.");
        }
        break;
    case DB_EVENT_WRITE_FAILED:
        env->errx(env, "event: DB_EVENT_WRITE_FAILED, I wrote to stable storage failed.");
        break;
    default:
        env->errx(env, "ignoring event %d", which);
    }
}

static void bdb_err_callback(const DB_ENV *dbenv, const char *errpfx, const char *msg){
	time_t curr_time = time(NULL);
	char time_str[32];
	strftime(time_str, 32, "%c", localtime(&curr_time));
	fprintf(stderr, "[%s] [%s] \"%s\"\n", errpfx, time_str, msg);
}

static void bdb_msg_callback(const DB_ENV *dbenv, const char *msg){
	time_t curr_time = time(NULL);
	char time_str[32];
	strftime(time_str, 32, "%c", localtime(&curr_time));
	fprintf(stderr, "[%s] [%s] \"%s\"\n", PACKAGE, time_str, msg);
}

/* for atexit cleanup */
void bdb_db_close(void){
    int ret = 0;

    if (dbp != NULL) {
        ret = dbp->close(dbp, 0);
        if (0 != ret){
            fprintf(stderr, "dbp->close: %s\n", db_strerror(ret));
        }else{
            dbp = NULL;
            fprintf(stderr, "dbp->close: OK\n");
        }
    }
}

/* for atexit cleanup */
void bdb_env_close(void){
    int ret = 0;
    if (env != NULL) {
        ret = env->close(env, 0);
        if (0 != ret){
            fprintf(stderr, "env->close: %s\n", db_strerror(ret));
        }else{
            env = NULL;
            fprintf(stderr, "env->close: OK\n");
        }
    }
}

/* for atexit cleanup */
void bdb_chkpoint(void)
{
    int ret = 0;
    if (env != NULL){
        ret = env->txn_checkpoint(env, 0, 0, 0); 
        if (0 != ret){
            fprintf(stderr, "env->txn_checkpoint: %s\n", db_strerror(ret));
        }else{
            fprintf(stderr, "env->txn_checkpoint: OK\n");
        }
    }
}
