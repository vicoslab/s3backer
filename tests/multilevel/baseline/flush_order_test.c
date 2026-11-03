/* RED/GREEN behavior test for FLUSH queue ordering, using the actual block_cache.c/nbdkit.c via flush_test.c's harness.
 * Scenario: a flush waits on block 0 which is rewritten while its write is in flight (WRITING2). When the first write
 * completes block 0 is requeued at the TAIL with a full (long) write delay behind blocks 1,2 queued earlier with the
 * same long delay. The flush wait loop expedites block 0's deadline; the fix also moves it to the queue head so a worker
 * (which only inspects the head) services it. Without the fix the flush stays stuck behind the head until the delay. */
#define main flush_test_original_main
#include "flush_test.c"
#undef main
extern int test_dirty_order(struct s3backer_store *, s3b_block_t *, int);

static int wait_enter_bounded(int b, int count, int ms) {
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += ms / 1000; ts.tv_nsec += (long)(ms % 1000) * 1000000L; if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
    int ok = 1;
    pthread_mutex_lock(&gate);
    while (entered[b] < count && ok) ok = pthread_cond_timedwait(&changed, &gate, &ts) == 0;
    ok = entered[b] >= count; pthread_mutex_unlock(&gate); return ok;
}
static int ms_since(const struct timespec *a) { struct timespec n; clock_gettime(CLOCK_MONOTONIC, &n); return (int)((n.tv_sec - a->tv_sec) * 1000 + (n.tv_nsec - a->tv_nsec) / 1000000); }
static void *do_flush_blocks0(void *unused) {
    (void)unused; in_flush = 1; wire_error = 0; s3b_block_t b = 0;
    int r = cache->flush_blocks(cache, &b, 1, 5000);
    pthread_mutex_lock(&gate); flush_result = r; done = 1; pthread_cond_broadcast(&changed); pthread_mutex_unlock(&gate); return NULL;
}
int main(int argc, char **argv) {
    REQUIRE(argc == 2); signal(SIGALRM, watchdog); alarm(12);
    const char *mode = argv[1];
    const unsigned delay = !strcmp(mode, "head-maxdelay") ? 4294967232u : 3600000u;   /* ms; max-safe = (UINT_MAX-63) */
    struct s3backer_store inner = {.create_threads = simple, .read_block = backend_read, .write_block = backend_write, .flush_blocks = backend_flush, .shutdown = simple, .destroy = destroy};
    struct block_cache_conf cc = {.block_size = 4096, .cache_size = 8, .num_threads = 2, .write_delay = delay, .log = logger};
    cc.cache_file = getenv("S3BACKER_TEST_CACHE");
    cc.perform_flush = 1; cc.recover_dirty_blocks = 1;
    struct s3b_config cfg = {.block_size = 4096, .file_size = N * 4096};
    struct fuse_ops_private fp = {0}; config = &cfg; fuse_priv = &fp;
    cache = block_cache_create(&cc, &inner); REQUIRE(cache); fp.s3b = cache; REQUIRE(cache->create_threads(cache) == 0);

    pthread_t thread; struct timespec t0;
    write_value(0, 0x31);                                   /* DIRTY, long delay (flush entry will expedite + promote it) */
    if (!strcmp(mode, "head-fua")) use_fua = 1, thread = begin_flush();
    else if (!strcmp(mode, "head-flushall")) thread = begin_flush();
    else { waiting = done = 0; REQUIRE(pthread_create(&thread, NULL, do_flush_blocks0, NULL) == 0); }
    wait_enter(0, 1); require_waiting();                    /* write #1 of block 0 in flight, flush waiting */
    if (use_fua) { /* FUA pwrite wrote 0x72 itself; the rewrite below must still win */ }
    write_value(0, 0x32);                                   /* WRITING2: rewrite during in-flight flush write */
    write_value(1, 0x41); write_value(2, 0x51);             /* older-than-block-0's-requeue long-delay queue head [1,2] */
    { s3b_block_t o[4]; int n = test_dirty_order(cache, o, 4); REQUIRE(n == 2 && o[0] == 1 && o[1] == 2); }
    permit(0, 1);                                           /* write #1 completes: block 0 DIRTY at queue tail, flush wakes */
    if (!wait_enter_bounded(0, 2, 2000)) {
        fprintf(stderr, "FAIL %s: forced block 0 was not serviced within 2000ms (entered[0]=%d): flush stuck behind long-delay queue head\n", mode, entered[0]);
        return 1;
    }
    clock_gettime(CLOCK_MONOTONIC, &t0);
    permit(0, 2);
    REQUIRE(pthread_join(thread, NULL) == 0);
    REQUIRE(ms_since(&t0) < 2000);                          /* flush bounded after the forced write was released */
    REQUIRE(flush_result == 0); verify(0, 0x32);
    /* normal ordering outside the forced block is untouched: 1,2 still queued, in order, never written */
    { s3b_block_t o[4]; int n = test_dirty_order(cache, o, 4); REQUIRE(n == 2 && o[0] == 1 && o[1] == 2); }
    REQUIRE(entered[1] == 0 && entered[2] == 0 && entered[0] == 2);
    pthread_mutex_lock(&gate); release_all = 1; pthread_cond_broadcast(&changed); pthread_mutex_unlock(&gate);
    REQUIRE(cache->shutdown(cache) == 0); cache->destroy(cache); alarm(0);
    printf("PASS %s (delay=%ums; actual cache; forced block serviced, flush bounded, queue [1,2] intact)\n", mode, delay); return 0;
}
