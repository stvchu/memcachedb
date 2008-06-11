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
 *  $Id: item.c 2008-01-23 22:33:13Z steve $
 */

/*
 * == Item Buffer Management ==
 *
 * If item buffer size larger than 'settings.item_buf_size', 
 * then we use malloc instead.
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
#include <stdlib.h>

#define MAX_ITEM_FREELIST_LENGTH 4000
#define INIT_ITEM_FREELIST_LENGTH 500

static size_t item_make_header(const uint8_t nkey, const int flags, const int nbytes, char *suffix, uint8_t *nsuffix);

static item **freeitem;
static int freeitemtotal;
static int freeitemcurr;

void item_init(void) {
    freeitemtotal = INIT_ITEM_FREELIST_LENGTH;
    freeitemcurr  = 0;

    freeitem = (item **)malloc( sizeof(item *) * freeitemtotal );
    if (freeitem == NULL) {
        perror("malloc()");
    }
    return;
}

/*
 * Returns a item buffer from the freelist, if any. Sholud call
 * item_from_freelist for thread safty.
 * */
item *do_item_from_freelist(size_t ntotal) {
    item *s;
    if (ntotal > settings.item_buf_size){
        if (settings.verbose > 1) {
            fprintf(stderr, "nototal larger than freelist buf size, try to use malloc instead.\n");
        }
        return NULL;
    }
    if (settings.verbose > 1) {
        fprintf(stderr, "nototal smaller than freelist buf size, try to use freelist.\n");
    }

    if (freeitemcurr > 0) {
        s = freeitem[--freeitemcurr];
    } else {
        /* If malloc fails, let the logic fall through without spamming
         * STDERR on the server. */
        s = (item *)malloc( settings.item_buf_size );
    }

    return s;
}

/*
 * Adds a item to the freelist. Should call 
 * item_add_to_freelist for thread safty.
 */
int do_item_add_to_freelist(item *it) {
    if (freeitemcurr < freeitemtotal) {
        freeitem[freeitemcurr++] = it;
        return 0;
    } else {
        if (freeitemtotal >= MAX_ITEM_FREELIST_LENGTH){
            return 1;
        }
        /* try to enlarge free item buffer array */
        item **new_freeitem = (item **)realloc(freeitem, sizeof(item *) * freeitemtotal * 2);
        if (new_freeitem) {
            freeitemtotal *= 2;
            freeitem = new_freeitem;
            freeitem[freeitemcurr++] = it;
            return 0;
        }
    }
    return 1;
}

/**
 * Generates the variable-sized part of the header for an object.
 *
 * key     - The key
 * nkey    - The length of the key
 * flags   - key flags
 * nbytes  - Number of bytes to hold value and addition CRLF terminator
 * suffix  - Buffer for the "VALUE" line suffix (flags, size).
 * nsuffix - The length of the suffix is stored here.
 *
 * Returns the total size of the header.
 */
static size_t item_make_header(const uint8_t nkey, const int flags, const int nbytes,
                     char *suffix, uint8_t *nsuffix) {
    /* suffix is defined at 40 chars elsewhere.. */
    *nsuffix = (uint8_t) snprintf(suffix, 40, " %d %d\r\n", flags, nbytes - 2);
    return sizeof(item) + nkey + *nsuffix + nbytes;
}

/*
 * alloc a item buffer, and init it.
 */
item *item_alloc1(char *key, const size_t nkey, const int flags, const int nbytes) {
    uint8_t nsuffix;
    item *it;
    char suffix[40];
    size_t ntotal = item_make_header(nkey + 1, flags, nbytes, suffix, &nsuffix);

    it = item_from_freelist(ntotal);
    if (it == NULL && (it = (item *)malloc(ntotal)) == NULL){
        return NULL;
    }

    it->nkey = nkey;
    it->nbytes = nbytes;
    strcpy(ITEM_key(it), key);
    memcpy(ITEM_suffix(it), suffix, (size_t)nsuffix);
    it->nsuffix = nsuffix;
    return it;
}

/*
 * alloc a item buffer only.
 */
item *item_alloc2(size_t ntotal) {
    item *it;
    it = item_from_freelist(ntotal);
    if (it == NULL && (it = (item *)malloc(ntotal)) == NULL){
        return NULL;
    }
    return it;
}

/*
 * free a item buffer. 
 */

int item_free(item *it) {
    size_t ntotal;
    if (NULL == it)
        return 0;

    ntotal = ITEM_ntotal(it);
    if (ntotal > settings.item_buf_size){
        if (settings.verbose > 1) {
            fprintf(stderr, "nototal larger than freelist buf size, try to use free() instead.\n");
        }
        free(it);   
    }else{
        if (settings.verbose > 1) {
            fprintf(stderr, "nototal smaller than freelist buf size, try to add to freelist.\n");
        }
        if (0 != item_add_to_freelist(it)) {
            free(it);   
        }
    }
    return 0;
}
