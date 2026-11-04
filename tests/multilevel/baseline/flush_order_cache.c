/* Same translation unit as cache_test.c (actual block_cache.c) plus a read-only view of the dirty queue. */
#include "cache_test.c"
int test_dirty_order(struct s3backer_store *s, s3b_block_t *out, int max) {
    struct block_cache_private *p = s->data;
    struct cache_entry *e;
    int n = 0;
    pthread_mutex_lock(&p->mutex);
    TAILQ_FOREACH(e, &p->dirties, link)
        if (n < max) out[n++] = e->block_num;
    pthread_mutex_unlock(&p->mutex);
    return n;
}
