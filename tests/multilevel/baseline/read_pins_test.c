/* Reader-pin tests against the production block_cache.c (included) and dcache.o (linked).
 * Disk hits are gated in a link-wrapped pread(); memory hits in block_cache.c's memcpy via
 * a macro local to this translation unit. Gates, wrapped cond waits and backend counters
 * synchronize; sleeps only let clean timeouts expire and bound negative observations.
 * Payload words encode (block, generation, word index), so torn, stale or backend data
 * is detected. Build with -DREAD_PINS_CANDIDATE to also check candidate-only state. */
#include "config.h"
#include <string.h>
#include <stdatomic.h>
void *test_memcpy(void *dst, const void *src, size_t n);
#define memcpy test_memcpy
#include "block_cache.c"
#undef memcpy

#define REQUIRE(c) do { if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); exit(1); } } while (0)
#define BS 4096
#define NB 16
#define WAIT_MS 5000

enum { NONE, VIA_PREAD, VIA_MEMCPY };
struct gate { int kind, hold, inject; size_t len; u_int off; int armed, entered, released, held; };
struct watch { int waits; pthread_cond_t *last; };
enum { OP_READ, OP_WRITE, OP_FLUSH, OP_SHUTDOWN };
struct op { int kind, b, gen, watched, r, done; u_int off, len; struct gate g; struct watch w; unsigned char buf[BS]; pthread_t t; };

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_cv = PTHREAD_COND_INITIALIZER;
static __thread struct gate *tl_gate;
static __thread struct watch *tl_watch;
static __thread int tl_test;                    // created by this test, not a cache worker
static atomic_uint worker_waits;
static int disk;
static struct s3backer_store *cache;
static struct block_cache_private *P;
static struct block_cache_conf cc;
static int bgen[NB], breads[NB], bwrites[NB], bverifies, cached[NB];
static int mixed_active;
static unsigned char mixed_expected[BS];
static atomic_uint direct_calls;
static int wgate_block = -1, wgate_entered, wgate_permit;

// Payload
static uint64_t mix(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL; x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL; return x ^ (x >> 31);
}
static void fill(unsigned char *buf, int b, int gen) {
    for (u_int i = 0; i < BS / 8; i++) {
        uint64_t w = (((uint64_t)b << 44) | ((uint64_t)gen << 20) | i) ^ mix(i);
        memcpy(buf + 8 * i, &w, 8);
    }
}
// Generation of block b that produced bytes [off, off+len) now at buf[0..len), or -1 if torn/foreign.
static int decode(const unsigned char *buf, int b, u_int off, u_int len) {
    int gen = -1;
    for (u_int i = off / 8; i < (off + len) / 8; i++) {
        uint64_t w; memcpy(&w, buf + 8 * i - off, 8); w ^= mix(i);
        if ((w & 0xfffff) != i || (int)(w >> 44) != b) return -1;
        int g = (int)((w >> 20) & 0xffffff);
        if (gen == -1) gen = g; else if (g != gen) return -1;
    }
    return gen;
}
static void etag_of(u_char *tag, int b, int gen) {
    uint64_t x = mix((uint64_t)b * 1000003ULL + (uint64_t)gen), y = mix(x);
    memcpy(tag, &x, 8); memcpy(tag + 8, &y, 8);
}

// Bounded waits on test state (all guarded by g_mu)
static struct timespec deadline(int ms) {
    struct timespec t; clock_gettime(CLOCK_REALTIME, &t);
    t.tv_sec += ms / 1000; t.tv_nsec += (long)(ms % 1000) * 1000000L;
    if (t.tv_nsec >= 1000000000L) { t.tv_sec++; t.tv_nsec -= 1000000000L; }
    return t;
}
#define AWAIT(cond, ms) ({ struct timespec _d = deadline(ms); int _r; pthread_mutex_lock(&g_mu); \
    while (!(_r = (cond)) && pthread_cond_timedwait(&g_cv, &g_mu, &_d) != ETIMEDOUT) ; \
    if (!_r) _r = (cond); pthread_mutex_unlock(&g_mu); _r; })
#define GET(expr) ({ pthread_mutex_lock(&g_mu); __typeof__(expr) _v = (expr); pthread_mutex_unlock(&g_mu); _v; })
static void nap(int ms) { struct timespec t = { ms / 1000, (long)(ms % 1000) * 1000000L }; while (nanosleep(&t, &t) != 0) ; }

// Copy gates: is the cache mutex held while this hit copies its data?
static int cache_mutex_held(void) {
    for (int i = 0; i < 200; i++) {
        if (pthread_mutex_trylock(&P->mutex) == 0) { pthread_mutex_unlock(&P->mutex); return 0; }
        nap(1);
    }
    return 1;
}
static int gate_hook(int kind, size_t n) {
    struct gate *const g = tl_gate;
    if (g == NULL || g->kind != kind || n != g->len || !GET(g->armed)) return 0;
    const int held = cache_mutex_held();
    pthread_mutex_lock(&g_mu);
    g->armed = 0; g->held = held; g->entered = 1; pthread_cond_broadcast(&g_cv);
    while (g->hold && !g->released) pthread_cond_wait(&g_cv, &g_mu);
    const int inject = g->inject;
    pthread_mutex_unlock(&g_mu);
    return inject;
}
void *test_memcpy(void *d, const void *s, size_t n) { gate_hook(VIA_MEMCPY, n); return memcpy(d, s, n); }
extern ssize_t __real_pread(int, void *, size_t, off_t);
ssize_t __wrap_pread(int fd, void *buf, size_t n, off_t off) {
    const int fl = fcntl(fd, F_GETFL);
    const int direct = fl != -1 && (fl & O_DIRECT) != 0;
    size_t logical = n;
    if (direct) {
        atomic_fetch_add(&direct_calls, 1);
        if (tl_gate != NULL) {
            const size_t start = tl_gate->off & ~(size_t)4095;
            const size_t end = (tl_gate->off + tl_gate->len + 4095) & ~(size_t)4095;
            // Match only this thread's requested, rounded subrange within a slot.
            // Directory/header reads, wrong offsets and wrong sizes cannot trip it.
            logical = off >= 0 && (size_t)off % BS == start && n == end - start ? tl_gate->len : 0;
        }
    }
    const int e = gate_hook(VIA_PREAD, logical);
    if (e) { errno = e; return -1; }
    return __real_pread(fd, buf, n, off);
}

// Cond-wait observation: watched test threads record their waits; cache workers are counted.
extern int __real_pthread_cond_wait(pthread_cond_t *, pthread_mutex_t *);
extern int __real_pthread_cond_timedwait(pthread_cond_t *, pthread_mutex_t *, const struct timespec *);
static void observe(pthread_cond_t *c, pthread_mutex_t *m) {
    if (m == &g_mu) return;
    if (tl_watch) { pthread_mutex_lock(&g_mu); tl_watch->waits++; tl_watch->last = c; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu); }
    else if (!tl_test) atomic_fetch_add(&worker_waits, 1);
}
int __wrap_pthread_cond_wait(pthread_cond_t *c, pthread_mutex_t *m) { observe(c, m); return __real_pthread_cond_wait(c, m); }
int __wrap_pthread_cond_timedwait(pthread_cond_t *c, pthread_mutex_t *m, const struct timespec *t) { observe(c, m); return __real_pthread_cond_timedwait(c, m, t); }

// Fake backend: counts downloads separately from ETag verifications.
static void logger(int level, const char *fmt, ...) { (void)level; (void)fmt; }
static int be_simple(struct s3backer_store *s) { (void)s; return 0; }
static void be_destroy(struct s3backer_store *s) { (void)s; }
static int be_flush(struct s3backer_store *s, const s3b_block_t *b, u_int n, long t) { (void)s; (void)b; (void)n; (void)t; return 0; }
static int be_read(struct s3backer_store *s, s3b_block_t b, void *dst, u_char *tag, const u_char *expect, int strict) {
    u_char t[MD5_DIGEST_LENGTH]; (void)s; (void)strict;
    pthread_mutex_lock(&g_mu);
    const int gen = bgen[b]; etag_of(t, b, gen);
    const int match = expect != NULL && memcmp(expect, t, sizeof(t)) == 0;
    if (match) bverifies++; else breads[b]++;
    pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu);
    if (tag) memcpy(tag, t, sizeof(t));
    if (match) return EEXIST;
    fill(dst, b, gen); return 0;
}
static int be_write(struct s3backer_store *s, s3b_block_t b, const void *src, u_char *tag, check_cancel_t *cancel, void *arg) {
    (void)s; (void)cancel; (void)arg;
    int gen = src != NULL ? decode(src, b, 0, BS) : -1;
    pthread_mutex_lock(&g_mu);
    if (mixed_active && b == 0) { REQUIRE(src && memcmp(src, mixed_expected, BS) == 0); gen = 900; }
    REQUIRE(gen > 0);                           // worker snapshot must be one whole generation
    if ((int)b == wgate_block) { wgate_entered++; pthread_cond_broadcast(&g_cv); while (!wgate_permit) pthread_cond_wait(&g_cv, &g_mu); }
    bgen[b] = gen; bwrites[b]++; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu);
    if (tag) etag_of(tag, b, gen);
    return 0;
}
static struct s3backer_store inner = { .create_threads = be_simple, .read_block = be_read, .write_block = be_write,
  .flush_blocks = be_flush, .shutdown = be_simple, .destroy = be_destroy };

static void watchdog(int sig) { (void)sig; static const char msg[] = "FAIL watchdog\n"; ssize_t ignored = write(2, msg, sizeof(msg) - 1); (void)ignored; _exit(124); }

// Cache lifecycle and inspection (inspection always takes the cache mutex)
static void open_cache(u_int size, u_int timeout_ms, u_int protect) {
    memset(&cc, 0, sizeof(cc));
    cc.block_size = BS; cc.cache_size = size; cc.num_threads = 2; cc.timeout = timeout_ms; cc.num_protected = protect;
    cc.log = logger; cc.cache_file = getenv("S3BACKER_TEST_CACHE"); cc.perform_flush = 1; cc.recover_dirty_blocks = 1;
    disk = cc.cache_file != NULL;
    REQUIRE((cache = block_cache_create(&cc, &inner)) != NULL); P = cache->data;
    REQUIRE(cache->create_threads(cache) == 0);
}
static void close_cache(void) { REQUIRE(cache->shutdown(cache) == 0); cache->destroy(cache); cache = NULL; }
static int has_block(int b) { pthread_mutex_lock(&P->mutex); int r = s3b_hash_get(P->hashtable, b) != NULL; pthread_mutex_unlock(&P->mutex); return r; }
static int state_of(int b) {
    pthread_mutex_lock(&P->mutex); struct cache_entry *e = s3b_hash_get(P->hashtable, b);
    int r = e ? ENTRY_GET_STATE(e) : -1; pthread_mutex_unlock(&P->mutex); return r;
}
static int poll_until_absent(int b, int ms) { for (int i = 0; i < ms; i += 5) { if (!has_block(b)) return 1; nap(5); } return !has_block(b); }
static void wait_clean(void) {
    pthread_mutex_lock(&P->mutex);
    while (P->num_dirties) pthread_cond_wait(&P->write_complete, &P->mutex);
    pthread_mutex_unlock(&P->mutex);
}
#ifdef READ_PINS_CANDIDATE
static void require_pins(int b, int readers, int excl, u_int total) {
    pthread_mutex_lock(&P->mutex); struct cache_entry *e = s3b_hash_get(P->hashtable, b);
    REQUIRE(e != NULL && e->readers == readers && e->excl_waiters == excl && P->num_pins == total);
    pthread_mutex_unlock(&P->mutex);
}
#endif
static int total_breads(void) { int n = 0; pthread_mutex_lock(&g_mu); for (int b = 0; b < NB; b++) n += breads[b]; pthread_mutex_unlock(&g_mu); return n; }

// Operations on test threads
static void *op_main(void *arg) {
    struct op *o = arg; int r = 0;
    tl_test = 1; tl_gate = o->g.kind ? &o->g : NULL; tl_watch = o->watched ? &o->w : NULL;
    switch (o->kind) {
    case OP_READ: r = o->len == BS ? cache->read_block(cache, o->b, o->buf, NULL, NULL, 0) : cache->read_block_part(cache, o->b, o->off, o->len, o->buf); break;
    case OP_WRITE: fill(o->buf, o->b, o->gen); r = cache->write_block(cache, o->b, o->buf, NULL, NULL, NULL); break;
    case OP_FLUSH: r = cache->flush_blocks(cache, NULL, 0, 0); break;
    case OP_SHUTDOWN: r = cache->shutdown(cache); break;
    }
    pthread_mutex_lock(&g_mu); o->r = r; o->done = 1; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu);
    return NULL;
}
static struct op *op_new(int kind, int b) { struct op *o = calloc(1, sizeof(*o)); REQUIRE(o); o->kind = kind; o->b = b; o->len = BS; return o; }
static struct op *reader(int b, u_int off, u_int len, int gated, int hold) {
    struct op *o = op_new(OP_READ, b); o->off = off; o->len = len;
    if (gated) { o->g.kind = disk ? VIA_PREAD : VIA_MEMCPY; o->g.len = len; o->g.off = off; o->g.hold = hold; o->g.armed = 1; }
    return o;
}
static void start(struct op *o) { REQUIRE(pthread_create(&o->t, NULL, op_main, o) == 0); }
static void release(struct op *o) { pthread_mutex_lock(&g_mu); o->g.released = 1; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu); }
static void finish(struct op *o) { REQUIRE(AWAIT(o->done, WAIT_MS)); REQUIRE(pthread_join(o->t, NULL) == 0); }
static int got(struct op *o) { return o->r == 0 ? decode(o->buf, o->b, o->off, o->len) : -1; }
static int read_sync(int b, u_int off, u_int len) { struct op *o = reader(b, off, len, 0, 0); start(o); finish(o); int g = got(o); free(o); return g; }
static void write_sync(int b, int gen) { struct op *o = op_new(OP_WRITE, b); o->gen = gen; start(o); finish(o); REQUIRE(o->r == 0); free(o); }
// Populate a CLEAN entry from the backend, then change the backend so a download would be visible.
static void warm(int b) {
    const int before = GET(breads[b]);
    cached[b] = GET(bgen[b]);
    REQUIRE(read_sync(b, 0, BS) == cached[b]); REQUIRE(GET(breads[b]) == before + 1); REQUIRE(state_of(b) == CLEAN);
}
static void poison(int b) { pthread_mutex_lock(&g_mu); bgen[b] = cached[b] + 100; pthread_mutex_unlock(&g_mu); }

// Cases
static void two_hits(int b1, u_int off2, u_int len2, int b2, const char *label) {
    struct block_cache_stats s0, s1;
    const int reads0 = total_breads();
    block_cache_get_stats(cache, &s0);
    struct op *a = reader(b1, 0, BS, 1, 1), *b = reader(b2, off2, len2, 1, 1);
    start(a); REQUIRE(AWAIT(a->g.entered, WAIT_MS));
    start(b);
    const int overlap = AWAIT(b->g.entered, 1500);
#ifdef READ_PINS_CANDIDATE
    if (overlap) { if (b1 == b2) require_pins(b1, 2, 0, 2); else { require_pins(b1, 1, 0, 2); require_pins(b2, 1, 0, 2); } }
#endif
    release(a); release(b); finish(a); finish(b);
    printf("observed %s: first_copy_under_cache_mutex=%d second_copy_overlapped=%d second_copy_under_cache_mutex=%d\n",
      label, a->g.held, overlap, b->g.held);
    REQUIRE(!a->g.held); REQUIRE(overlap); REQUIRE(!b->g.held);
    REQUIRE(got(a) == cached[b1]); REQUIRE(got(b) == cached[b2]);
    block_cache_get_stats(cache, &s1);
    REQUIRE(s1.read_hits == s0.read_hits + 2 && s1.read_misses == s0.read_misses); REQUIRE(total_breads() == reads0);
    free(a); free(b);
}
static void case_hit_overlap(void) {
    open_cache(8, 0, 0); warm(0); warm(1); poison(0); poison(1);
    two_hits(0, 1024, 2048, 1, "independent-blocks");
}
static void case_same_entry(void) {
    open_cache(8, 0, 0); warm(0); poison(0);
    two_hits(0, 8, 4000, 0, "same-block");
}
static void case_reader_vs_write(void) {
    open_cache(8, 0, 0); warm(0); poison(0);
    struct op *a = reader(0, 0, BS, 1, 1), *w = op_new(OP_WRITE, 0), *c = reader(0, 512, 1024, 1, 0);
    start(a); REQUIRE(AWAIT(a->g.entered, WAIT_MS));
    w->gen = 500; w->watched = 1; start(w);
    REQUIRE(AWAIT(w->w.waits >= 1, 1500));                     // writer blocks in a cond wait, not on the mutex
#ifdef READ_PINS_CANDIDATE
    REQUIRE(GET(w->w.last) == &P->unpinned); require_pins(0, 1, 1, 1);
#endif
    REQUIRE(!GET(w->done));
    start(c); finish(c);                                       // a later reader gets no pin while a writer waits
    REQUIRE(c->g.held == 1); REQUIRE(got(c) == cached[0]); REQUIRE(!GET(w->done));
    release(a); finish(a); finish(w);
    REQUIRE(got(a) == cached[0]); REQUIRE(w->r == 0);          // whole old generation, never torn
    REQUIRE(read_sync(0, 0, BS) == 500); wait_clean(); REQUIRE(GET(bgen[0]) == 500);
    free(a); free(w); free(c);
}
static void case_evict_pinned(void) {
    open_cache(4, 0, 0);
    for (int b = 0; b < 4; b++) { warm(b); poison(b); }
    struct op *a = reader(0, 0, BS, 1, 1);
    start(a); REQUIRE(AWAIT(a->g.entered, WAIT_MS));
    for (int b = 1; b < 4; b++) REQUIRE(read_sync(b, 0, BS) == cached[b]);   // pinned block 0 is now the LRU head
    const int g4 = GET(bgen[4]);
    struct op *m = reader(4, 0, BS, 0, 0); m->watched = 1; start(m); finish(m);
    REQUIRE(got(m) == g4); REQUIRE(has_block(0) && !has_block(1));
    cached[4] = g4;
    struct op *p[5] = { 0 };
    for (int b = 2; b <= 4; b++) { p[b] = reader(b, 0, BS, 1, 1); start(p[b]); REQUIRE(AWAIT(p[b]->g.entered, WAIT_MS)); }
    const int g5 = GET(bgen[5]);
    struct op *m2 = reader(5, 0, BS, 0, 0); m2->watched = 1; start(m2);
    REQUIRE(AWAIT(m2->w.waits >= 1, WAIT_MS));                 // every entry pinned: allocator sleeps
#ifdef READ_PINS_CANDIDATE
    REQUIRE(GET(m2->w.last) == &P->space_avail);
    pthread_mutex_lock(&P->mutex); REQUIRE(P->space_pin_waiters == 1 && P->num_pins == 4); pthread_mutex_unlock(&P->mutex);
#endif
    struct op *c = reader(2, 1024, 512, 1, 0); start(c); finish(c);   // no new pin while the allocator waits
    REQUIRE(c->g.held == 1); REQUIRE(got(c) == cached[2]);
    const int waits = GET(m2->w.waits);
    REQUIRE(!GET(m2->done)); REQUIRE(waits <= 2);
    release(p[2]); finish(m2);
    printf("observed allocator waits while all entries pinned: %d (after release: %d)\n", waits, m2->w.waits);
    REQUIRE(got(m2) == g5); REQUIRE(m2->w.waits <= waits + 2);
    REQUIRE(!has_block(2) && has_block(0) && has_block(3) && has_block(4));
    release(a); release(p[3]); release(p[4]); finish(a); finish(p[2]); finish(p[3]); finish(p[4]);
    REQUIRE(got(a) == cached[0]);
    for (int b = 2; b <= 4; b++) REQUIRE(got(p[b]) == cached[b]);
}
static void case_timeout_pinned(void) {
    open_cache(8, 500, NB);                                    // all blocks high priority: worker sleeps on their timeouts
    warm(0); warm(1); poison(0); poison(1);
    struct op *a = reader(0, 0, BS, 1, 1);
    start(a); REQUIRE(AWAIT(a->g.entered, WAIT_MS));
    nap(700);                                                  // entries 1 and pinned 0 have expired
    write_sync(2, 900);                                        // idle workers only scan timeouts when woken
    REQUIRE(poll_until_absent(1, WAIT_MS));                    // expired idle entry is evicted, pinned one skipped
    nap(100);
    const unsigned w0 = atomic_load(&worker_waits);
    nap(400);
    const unsigned w1 = atomic_load(&worker_waits);
    printf("observed worker cond waits in 400ms with expired pinned LRU entry: %u\n", w1 - w0);
    REQUIRE(has_block(0)); REQUIRE(w1 - w0 < 50); REQUIRE(!GET(a->done));
    release(a); finish(a); REQUIRE(got(a) == cached[0]);
    REQUIRE(poll_until_absent(0, WAIT_MS));                    // last unpin wakes the worker
    const int r0 = GET(breads[0]);
    REQUIRE(read_sync(0, 0, BS) == cached[0] + 100); REQUIRE(GET(breads[0]) == r0 + 1);
}
static void case_pread_error(void) {
    struct block_cache_stats s0, s1;
    open_cache(4, 0, 0); REQUIRE(disk); warm(0); poison(0);
    block_cache_get_stats(cache, &s0);
    struct op *a = reader(0, 0, BS, 1, 0); a->g.inject = EIO;
    start(a); finish(a); REQUIRE(a->g.entered); REQUIRE(a->r == EIO);
    block_cache_get_stats(cache, &s1); REQUIRE(s1.read_hits == s0.read_hits && s1.read_misses == s0.read_misses);
#ifdef READ_PINS_CANDIDATE
    require_pins(0, 0, 0, 0);
#endif
    REQUIRE(state_of(0) == CLEAN);
    write_sync(0, 600);                                        // would wait forever on a leaked pin
    REQUIRE(read_sync(0, 0, BS) == 600); wait_clean(); REQUIRE(GET(bgen[0]) == 600);
    for (int b = 1; b <= 4; b++) REQUIRE(read_sync(b, 0, BS) == GET(bgen[b]));
    REQUIRE(!has_block(0));                                    // and it can be evicted
    free(a);
}
static void case_shutdown_active(void) {
    open_cache(8, 0, 0); warm(0); warm(1); poison(0);
    struct op *a = reader(0, 0, BS, 1, 1), *s = op_new(OP_SHUTDOWN, 0);
    start(a); REQUIRE(AWAIT(a->g.entered, WAIT_MS));
    s->watched = 1; start(s);
#ifdef READ_PINS_CANDIDATE
    REQUIRE(AWAIT(s->w.last == &P->unpinned, WAIT_MS));
    pthread_mutex_lock(&P->mutex); REQUIRE(P->num_threads == 0 && P->stopping && P->num_pins == 1); pthread_mutex_unlock(&P->mutex);
#else
    REQUIRE(!AWAIT(s->done, 1000));
#endif
    REQUIRE(!GET(s->done));
    release(a); finish(a); finish(s);
    REQUIRE(s->r == 0); REQUIRE(got(a) == cached[0]);
    cache->destroy(cache); cache = NULL;
    free(a); free(s);
}
static void case_worker_flush(void) {
    open_cache(8, 0, 0); warm(0); warm(2); poison(0); poison(2);
    pthread_mutex_lock(&g_mu); wgate_block = 1; pthread_mutex_unlock(&g_mu);
    write_sync(1, 700); REQUIRE(AWAIT(wgate_entered >= 1, WAIT_MS));   // worker is uploading block 1
    REQUIRE(state_of(1) == WRITING);
    struct op *a = reader(0, 0, BS, 1, 1), *f = op_new(OP_FLUSH, 0), *c = reader(2, 2048, 2048, 1, 1);
    start(a); REQUIRE(AWAIT(a->g.entered, WAIT_MS));
    f->watched = 1; start(f); REQUIRE(AWAIT(f->w.waits >= 1, WAIT_MS)); REQUIRE(!GET(f->done));
    start(c); REQUIRE(AWAIT(c->g.entered, WAIT_MS)); REQUIRE(!a->g.held && !c->g.held);
    REQUIRE(read_sync(1, 0, BS) == 700);                       // WRITING hit
    write_sync(3, 701);
    pthread_mutex_lock(&g_mu); wgate_permit = 1; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu);
    finish(f); REQUIRE(f->r == 0); REQUIRE(!GET(a->done));     // flush does not wait for pinned clean readers
    REQUIRE(GET(bgen[1]) == 700 && GET(bgen[3]) == 701);
    release(a); release(c); finish(a); finish(c);
    REQUIRE(got(a) == cached[0]); REQUIRE(got(c) == cached[2]);
    REQUIRE(state_of(0) == CLEAN && state_of(1) == CLEAN && state_of(3) == CLEAN);
    free(a); free(f); free(c);
}
#define SB 12
static atomic_int started_gen[SB], done_gen[SB];
struct worker_arg { int id; unsigned seed; pthread_t t; };
static unsigned rnd(unsigned *s) { *s = *s * 1103515245u + 12345u; return *s >> 8; }
static void *stress_reader(void *arg) {
    struct worker_arg *w = arg; unsigned char buf[BS]; tl_test = 1;
    for (int k = 0; k < 400; k++) {
        const int b = rnd(&w->seed) % SB; u_int off = 0, len = BS;
        if (rnd(&w->seed) % 3) { off = 8 * (rnd(&w->seed) % (BS / 8 - 1)); len = 8 * (1 + rnd(&w->seed) % ((BS - off) / 8)); if (len == BS) len -= 8; }
        const int lo = atomic_load(&done_gen[b]);
        const int r = len == BS ? cache->read_block(cache, b, buf, NULL, NULL, 0) : cache->read_block_part(cache, b, off, len, buf);
        const int hi = atomic_load(&started_gen[b]);
        const int g = r == 0 ? decode(buf, b, off, len) : -1;
        if (g < lo || g > hi) { fprintf(stderr, "FAIL stress read block %d [%u,+%u): r=%d gen=%d expected [%d,%d]\n", b, off, len, r, g, lo, hi); exit(1); }
    }
    return NULL;
}
static void *stress_writer(void *arg) {
    struct worker_arg *w = arg; unsigned char buf[BS]; tl_test = 1;
    for (int k = 0; k < 150; k++) {
        const int b = 2 * (rnd(&w->seed) % (SB / 2)) + w->id;
        const int gen = atomic_load(&started_gen[b]) + 1;
        atomic_store(&started_gen[b], gen); fill(buf, b, gen);
        REQUIRE(cache->write_block(cache, b, buf, NULL, NULL, NULL) == 0);
        atomic_store(&done_gen[b], gen);
    }
    return NULL;
}
static void *stress_flusher(void *arg) {
    (void)arg; tl_test = 1;
    for (int k = 0; k < 30; k++) { REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == 0); sched_yield(); }
    return NULL;
}
static void case_stress(void) {
    struct worker_arg r[6], w[2], f;
    open_cache(6, 0, 0);
    for (int b = 0; b < SB; b++) { atomic_store(&started_gen[b], bgen[b]); atomic_store(&done_gen[b], bgen[b]); }
    for (int i = 0; i < 6; i++) { r[i].id = i; r[i].seed = 1000 + i; REQUIRE(pthread_create(&r[i].t, NULL, stress_reader, &r[i]) == 0); }
    for (int i = 0; i < 2; i++) { w[i].id = i; w[i].seed = 2000 + i; REQUIRE(pthread_create(&w[i].t, NULL, stress_writer, &w[i]) == 0); }
    REQUIRE(pthread_create(&f.t, NULL, stress_flusher, NULL) == 0);
    for (int i = 0; i < 6; i++) REQUIRE(pthread_join(r[i].t, NULL) == 0);
    for (int i = 0; i < 2; i++) REQUIRE(pthread_join(w[i].t, NULL) == 0);
    REQUIRE(pthread_join(f.t, NULL) == 0);
    REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == 0); wait_clean();
    for (int b = 0; b < SB; b++) { REQUIRE(GET(bgen[b]) == atomic_load(&done_gen[b])); REQUIRE(read_sync(b, 0, BS) == atomic_load(&done_gen[b])); }
    pthread_mutex_lock(&P->mutex);
    printf("observed stress: hits=%u misses=%u writes=%u\n", P->stats.read_hits, P->stats.read_misses, P->stats.write_hits + P->stats.write_misses);
    pthread_mutex_unlock(&P->mutex);
}
static void case_verify_relocation(void) {
    open_cache(8, 0, 0); REQUIRE(disk);
    for (int b = 0; b < 4; b++) warm(b);
    close_cache();
    open_cache(8, 0, 0);                                       // reload: entries are CLEAN2 until ETag-verified
    for (int b = 0; b < 4; b++) REQUIRE(state_of(b) == CLEAN2);
    const int reads0 = total_breads(), v0 = GET(bverifies);
    struct op *a = reader(0, 0, BS, 1, 1), *b = reader(1, 1024, 2048, 1, 1);
    start(a); REQUIRE(AWAIT(a->g.entered, WAIT_MS)); start(b);
    const int overlap = AWAIT(b->g.entered, 1500);
    release(a); release(b); finish(a); finish(b);
    printf("observed verified-relocated hits: first_copy_under_cache_mutex=%d second_copy_overlapped=%d\n", a->g.held, overlap);
    REQUIRE(overlap && !a->g.held && !b->g.held);
    REQUIRE(got(a) == cached[0] && got(b) == cached[1]);
    REQUIRE(GET(bverifies) == v0 + 2 && total_breads() == reads0);
    REQUIRE(state_of(0) == CLEAN && state_of(1) == CLEAN && state_of(2) == CLEAN2);
    write_sync(2, 800); REQUIRE(read_sync(2, 0, BS) == 800); wait_clean(); REQUIRE(GET(bgen[2]) == 800);
    free(a); free(b);
}

// Buffered partial overwrites followed by worker upload -> CLEAN -> pinned read.
// The backend checks the complete expected native block, including mixed generations.
static void case_partial_clean(void) {
    open_cache(4, 0, 0); warm(0);
    fill(mixed_expected, 0, cached[0]);
    for (int pass = 0; pass < 2; pass++) {
        const u_int off = pass ? 2001 : 8, len = pass ? 117 : 2048;
        unsigned char patch[BS]; fill(patch, 0, 901 + pass);
        pthread_mutex_lock(&g_mu); mixed_active = 1;
        memcpy(mixed_expected + off, patch + off, len); pthread_mutex_unlock(&g_mu);
        REQUIRE(cache->write_block_part(cache, 0, off, len, patch + off) == 0);
        REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == 0); wait_clean(); REQUIRE(state_of(0) == CLEAN);
        const unsigned dp = atomic_load(&direct_calls);
        struct op *o = reader(0, 3, BS - 7, 1, 1); start(o);
        REQUIRE(AWAIT(o->g.entered, WAIT_MS)); REQUIRE(!o->g.held);
        require_pins(0, 1, 0, 1); release(o); finish(o);
        REQUIRE(o->r == 0 && memcmp(o->buf, mixed_expected + 3, BS - 7) == 0);
        if (cc.read_direct) REQUIRE(atomic_load(&direct_calls) > dp);
        free(o);
    }
    printf("observed partial-clean: direct-preads=%u\n", atomic_load(&direct_calls));
}

int main(int argc, char **argv) {
    static const struct { const char *name; void (*fn)(void); } cases[] = {
        { "partial-clean", case_partial_clean }, { "hit-overlap", case_hit_overlap }, { "same-entry", case_same_entry }, { "reader-vs-write", case_reader_vs_write },
        { "evict-pinned", case_evict_pinned }, { "timeout-pinned", case_timeout_pinned }, { "pread-error", case_pread_error },
        { "shutdown-active", case_shutdown_active }, { "worker-flush", case_worker_flush }, { "stress", case_stress },
        { "verify-relocation", case_verify_relocation },
    };
    REQUIRE(argc == 2); tl_test = 1;
    signal(SIGALRM, watchdog); alarm(90);
    for (int b = 0; b < NB; b++) bgen[b] = 10 + b;
    for (size_t i = 0; i < sizeof(cases) / sizeof(*cases); i++) {
        if (strcmp(argv[1], cases[i].name) != 0) continue;
        cases[i].fn();
        if (cache != NULL) close_cache();
        alarm(0); printf("PASS %s (%s cache)\n", argv[1], disk ? "disk" : "memory"); return 0;
    }
    REQUIRE(0);
}
