/* Inspect completion only; all writes and flushes use production entry points. */
#include "block_cache.c"
void test_wait_clean(struct s3backer_store *s) {
    struct block_cache_private *p=s->data;
    pthread_mutex_lock(&p->mutex);
    while(p->num_dirties) pthread_cond_wait(&p->write_complete,&p->mutex);
    pthread_mutex_unlock(&p->mutex);
}
