/* DERIVED from baseline/read_pins_test.c (see BASELINE_SHA256SUMS) for the two-level locking work: adds worker-snapshot, pwrite and
 * rwlock-wait observation, a gated/failing backend read, delay/read-ahead options and the ml-* cases. Old-global-lock-specific assertions are
 * under #ifdef MULTILEVEL_CANDIDATE only (marked "ML-ADAPT"). Reader-pin tests against the production block_cache.c (included) and dcache.o (linked).
 * Disk hits are gated in a link-wrapped pread(); memory hits in block_cache.c's memcpy via
 * a macro local to this translation unit. Gates, wrapped cond waits and backend counters
 * synchronize; sleeps only let clean timeouts expire and bound negative observations.
 * Payload words encode (block, generation, word index), so torn, stale or backend data
 * is detected. Build with -DREAD_PINS_CANDIDATE to also check candidate-only state. */
#include <config.h> // use the runner's generated configuration, ahead of the checked-in NDEBUG header
#include <string.h>
#include <stdatomic.h>
#include <stdint.h>
#include <time.h>
#include <sys/wait.h>
static atomic_uint_fast64_t de_clock_offset;
static atomic_int de_clock_active;
static int de_clock_gettime(clockid_t id, struct timespec *t) {
    int r = clock_gettime(id, t);
    if (!r && id == CLOCK_MONOTONIC && atomic_load(&de_clock_active)) {
        t->tv_sec = 1000; t->tv_nsec = 0;
        uint64_t ms = atomic_load(&de_clock_offset);
        t->tv_sec += ms / 1000; t->tv_nsec += (ms % 1000) * 1000000;
        if (t->tv_nsec >= 1000000000) { t->tv_sec++; t->tv_nsec -= 1000000000; }
    }
    return r;
}
#define clock_gettime de_clock_gettime
void *test_memcpy(void *dst, const void *src, size_t n);
#define memcpy test_memcpy
#include "block_cache.c"
#undef memcpy
#undef clock_gettime

#if defined(ML_EXPECT_ASSERTIONS) && defined(NDEBUG)
#error "The real-source cache harness must have production invariant assertions enabled"
#endif

#define REQUIRE(c) do { if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); exit(1); } } while (0)
#define BS 4096
#define NB 32
#define WAIT_MS 5000

enum { NONE, VIA_PREAD, VIA_MEMCPY, VIA_PWRITE };
struct gate { int kind, hold, inject; size_t len; u_int off; int armed, entered, released, held; };
struct watch { int waits; void *last; };
enum { OP_READ, OP_WRITE, OP_FLUSH, OP_SHUTDOWN, OP_DRAIN, OP_INTERNAL_SYNC };
struct op { int kind, b, gen, watched, r, done; u_int off, len; struct gate g; struct watch w; unsigned char buf[BS]; pthread_t t; };

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_cv = PTHREAD_COND_INITIALIZER;
static __thread struct gate *tl_gate;
static __thread struct watch *tl_watch;
static __thread int tl_sync_allowed;
static __thread int tl_test;                    // created by this test, not a cache worker
static atomic_uint worker_waits, worker_space_waits, worker_reading_waits;
static int disk;
static int local_mode;
static u_int de_grace;
static u_int local_max_dirty;
static int local_threads = 2;
static atomic_int remote_read_fail;
static int sync_hold, sync_entered, sync_release, capture_sync;
static unsigned char durable_image[262144];
static size_t durable_len;
static int writer_hold, writer_entered, writer_release;
static atomic_int sync_fail;
static atomic_uint sync_calls;
static atomic_int erase_observer, synced_marker;
static atomic_int fsync_fail, metadata_fail, backend_fail;
static atomic_int backend_fail_block = -1;
static atomic_long fail_pwrite_offset = -1;
extern int __real_fsync(int);
int __wrap_fsync(int fd) {
    struct stat st; REQUIRE(fstat(fd, &st) == 0);
    int kind = atomic_load(&fsync_fail);
    if (kind && ((kind == 2) == !!S_ISDIR(st.st_mode))) { atomic_store(&fsync_fail, 0); errno = EIO; return -1; }
    return __real_fsync(fd);
}
extern int __real_fdatasync(int);
int __wrap_fdatasync(int fd) {
    atomic_fetch_add(&sync_calls, 1);
    if (atomic_exchange(&sync_fail, 0)) { errno = EIO; return -1; }
    pthread_mutex_lock(&g_mu);
    if (sync_hold && tl_sync_allowed) { sync_entered = 1; pthread_cond_broadcast(&g_cv); while (!sync_release) pthread_cond_wait(&g_cv,&g_mu); }
    pthread_mutex_unlock(&g_mu);
    int r = __real_fdatasync(fd);
    if (!r && capture_sync) { struct stat st; REQUIRE(fstat(fd,&st)==0 && st.st_size <= sizeof(durable_image)); durable_len=st.st_size; REQUIRE(pread(fd,durable_image,durable_len,0)==durable_len); }
    if (!r && atomic_load(&erase_observer)) { uint32_t flags; REQUIRE(pread(fd,&flags,4,84)==4); atomic_store(&synced_marker,flags); }
    return r;
}
static struct s3backer_store *cache;
static struct block_cache_private *P;
static struct block_cache_conf cc;
static int bgen[NB], breads[NB], bwrites[NB], bverifies, cached[NB];
static int mixed_active;
static unsigned char mixed_expected[BS];
static atomic_uint direct_calls;
static int wgate_block = -1, wgate_entered, wgate_permit;
// Gate every real backend PUT, before either success or injected failure.
static int puts_hold, puts_release, puts_entered, puts_inflight, puts_peak, puts_completed;
static int puts_seen[NB];
static int ra_gate_hold, ra_gate_entered, ra_gate_release; // park all workers at a speculative miss
static int rgate_block = -1, rgate_entered, rgate_permit, rgate_fail;   // backend read gate: call k proceeds when rgate_permit >= k; call 1 fails if rgate_fail

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
static struct gate wsnap;                       // armed gate for worker-thread (non-test) copies of one whole block
static int gate_hook(int kind, size_t n) {
    struct gate *g = tl_gate;
    if (g == NULL && !tl_test) g = &wsnap;
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

extern ssize_t __real_pwrite(int, const void *, size_t, off_t);
ssize_t __wrap_pwrite(int fd, const void *buf, size_t n, off_t off) {
    if (atomic_load(&erase_observer) && off==64 && n==24) {
        const unsigned char zero[24]={0};
        if (!memcmp(buf,zero,24)) REQUIRE(atomic_load(&synced_marker)==0);
    }
    if (off == atomic_load(&fail_pwrite_offset)) { atomic_store(&fail_pwrite_offset, -1); errno=EIO; return -1; }
    if (n < BS && atomic_exchange(&metadata_fail, 0)) { errno = EIO; return -1; }
    const int e = gate_hook(VIA_PWRITE, n);
    if (e) { errno = e; return -1; }
    return __real_pwrite(fd, buf, n, off);
}
// Cond-wait observation: watched test threads record their waits; cache workers are counted.
extern int __real_pthread_cond_wait(pthread_cond_t *, pthread_mutex_t *);
extern int __real_pthread_cond_timedwait(pthread_cond_t *, pthread_mutex_t *, const struct timespec *);
static void observe_lock(void *l) {
    if (tl_watch) { pthread_mutex_lock(&g_mu); tl_watch->waits++; tl_watch->last = l; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu); }
}
extern int __real_pthread_rwlock_rdlock(pthread_rwlock_t *);
extern int __real_pthread_rwlock_wrlock(pthread_rwlock_t *);
int __wrap_pthread_rwlock_rdlock(pthread_rwlock_t *l) { if (pthread_rwlock_tryrdlock(l) == 0) return 0; observe_lock(l); return __real_pthread_rwlock_rdlock(l); }
int __wrap_pthread_rwlock_wrlock(pthread_rwlock_t *l) {
    if (pthread_rwlock_trywrlock(l) == 0) return 0;
    observe_lock(l);
    pthread_mutex_lock(&g_mu);
    if (writer_hold && tl_test) { writer_entered=1; pthread_cond_broadcast(&g_cv); while(!writer_release) pthread_cond_wait(&g_cv,&g_mu); }
    pthread_mutex_unlock(&g_mu);
    return __real_pthread_rwlock_wrlock(l);
}
static void observe(pthread_cond_t *c, pthread_mutex_t *m) {
    if (m == &g_mu) return;
    if (tl_watch) { pthread_mutex_lock(&g_mu); tl_watch->waits++; tl_watch->last = c; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu); }
    else if (!tl_test) {
        atomic_fetch_add(&worker_waits, 1);
        if (P && c == &P->space_avail) atomic_fetch_add(&worker_space_waits, 1);
        if (P && c == &P->end_reading) atomic_fetch_add(&worker_reading_waits, 1);
    }
}
int __wrap_pthread_cond_wait(pthread_cond_t *c, pthread_mutex_t *m) { observe(c, m); return __real_pthread_cond_wait(c, m); }
int __wrap_pthread_cond_timedwait(pthread_cond_t *c, pthread_mutex_t *m, const struct timespec *t) { observe(c, m); return __real_pthread_cond_timedwait(c, m, t); }

// Gate the existing outer read-ahead lookup, before do_read takes its allocation path.
// Releasing the cache mutex lets every worker reach the same scheduling point.
extern void *__real_s3b_hash_get(struct s3b_hash *, s3b_block_t);
void *__wrap_s3b_hash_get(struct s3b_hash *h, s3b_block_t b) {
    if (!tl_test && ra_gate_hold && b>=4 && b<=6 && !__real_s3b_hash_get(h,b)) {
        pthread_mutex_lock(&g_mu);
        if (!ra_gate_release) {
            ra_gate_entered++; pthread_cond_broadcast(&g_cv);
            pthread_mutex_unlock(&P->mutex);
            while (!ra_gate_release) pthread_cond_wait(&g_cv,&g_mu);
            pthread_mutex_unlock(&g_mu); pthread_mutex_lock(&P->mutex);
        } else pthread_mutex_unlock(&g_mu);
    }
    return __real_s3b_hash_get(h,b);
}

// Fake backend: counts downloads separately from ETag verifications.
static void logger(int level, const char *fmt, ...) { (void)level; (void)fmt; }
static int be_simple(struct s3backer_store *s) { (void)s; return 0; }
static void be_destroy(struct s3backer_store *s) { (void)s; }
static int be_token(struct s3backer_store *s, int32_t *old, int32_t value) { (void)s; (void)value; if(old) *old=0; return 0; }
static int be_flush(struct s3backer_store *s, const s3b_block_t *b, u_int n, long t) { (void)s; (void)b; (void)n; (void)t; return 0; }
static int be_read(struct s3backer_store *s, s3b_block_t b, void *dst, u_char *tag, const u_char *expect, int strict) {
    u_char t[MD5_DIGEST_LENGTH]; (void)s; (void)strict;
    if (atomic_load(&remote_read_fail)) return EIO;
    pthread_mutex_lock(&g_mu);
    if ((int)b == rgate_block) {
        const int call = ++rgate_entered; pthread_cond_broadcast(&g_cv);
        while (rgate_permit < call) pthread_cond_wait(&g_cv, &g_mu);
        if (rgate_fail && call == 1) { pthread_mutex_unlock(&g_mu); return EIO; }
    }
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
    if (puts_hold) {
        puts_entered++; puts_seen[b]++; puts_inflight++;
        if (puts_inflight > puts_peak) puts_peak = puts_inflight;
        pthread_cond_broadcast(&g_cv);
        while (!puts_release) pthread_cond_wait(&g_cv, &g_mu);
        puts_inflight--; puts_completed++;
        pthread_cond_broadcast(&g_cv);
    }
    if (atomic_load(&backend_fail) || (int)b == atomic_load(&backend_fail_block)) {
        pthread_mutex_unlock(&g_mu); return EIO;
    }
    if (mixed_active && b == 0) { REQUIRE(src && memcmp(src, mixed_expected, BS) == 0); gen = 900; }
    REQUIRE(gen > 0);                           // worker snapshot must be one whole generation
    if ((int)b == wgate_block) { wgate_entered++; pthread_cond_broadcast(&g_cv); while (!wgate_permit) pthread_cond_wait(&g_cv, &g_mu); }
    bgen[b] = gen; bwrites[b]++; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu);
    if (tag) etag_of(tag, b, gen);
    return 0;
}
static struct s3backer_store inner = { .create_threads = be_simple, .read_block = be_read, .write_block = be_write,
  .set_mount_token = be_token, .flush_blocks = be_flush, .shutdown = be_simple, .destroy = be_destroy };

static void watchdog(int sig) { (void)sig; static const char msg[] = "FAIL watchdog\n"; ssize_t ignored = write(2, msg, sizeof(msg) - 1); (void)ignored; _exit(124); }

// Cache lifecycle and inspection (inspection always takes the cache mutex)
static void open_cache2(u_int size, u_int timeout_ms, u_int protect, u_int delay, u_int ra) {
    memset(&cc, 0, sizeof(cc));
    cc.block_size = BS; cc.cache_size = size; cc.num_threads = local_threads; cc.timeout = timeout_ms; cc.num_protected = protect;
    cc.max_dirty = local_max_dirty;
    cc.write_delay = delay; cc.read_ahead = ra; cc.read_ahead_trigger = ra ? 2 : 0;
    #ifdef ML_HAS_LOCAL_FLUSH
    cc.local_flush = local_mode;
#ifdef ML_HAS_DIRTY_EVICTION
    cc.dirty_eviction_grace = de_grace;
#endif
#endif
    cc.log = logger; cc.cache_file = getenv("S3BACKER_TEST_CACHE"); cc.perform_flush = 1; cc.recover_dirty_blocks = 1;
    disk = cc.cache_file != NULL;
    REQUIRE((cache = block_cache_create(&cc, &inner)) != NULL); P = cache->data;
    REQUIRE(cache->create_threads(cache) == 0);
}
static void open_cache(u_int size, u_int timeout_ms, u_int protect) { open_cache2(size, timeout_ms, protect, 0, 0); }
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
    tl_sync_allowed = o->kind==OP_WRITE || o->kind==OP_INTERNAL_SYNC;
    tl_test = 1; tl_gate = o->g.kind ? &o->g : NULL; tl_watch = o->watched ? &o->w : NULL;
    switch (o->kind) {
    case OP_READ: r = o->len == BS ? cache->read_block(cache, o->b, o->buf, NULL, NULL, 0) : cache->read_block_part(cache, o->b, o->off, o->len, o->buf); break;
    case OP_WRITE: fill(o->buf, o->b, o->gen); r = cache->write_block(cache, o->b, o->buf, NULL, NULL, NULL); break;
    case OP_INTERNAL_SYNC: r = s3b_dcache_fsync(P->dcache); break;
    case OP_FLUSH: r = cache->flush_blocks(cache, NULL, 0, 0); break;
    case OP_DRAIN:
#ifdef ML_HAS_LOCAL_FLUSH
        r = block_cache_remote_drain(cache,5000);
#else
        r = ENOSYS;
#endif
        break;
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
#ifdef MULTILEVEL_CANDIDATE
// ML-ADAPT: the baseline expects a later reader to copy the OLD generation under the cache mutex and the writer to wait on the
// 'unpinned' condition. With per-entry locks the writer waits on the entry lock and the later reader queues behind it, so the
// stronger assertion (writer first, A keeps one whole generation, no torn data) lives in case_ml_reader_vs_write.
static void case_ml_reader_vs_write(void);
static void case_reader_vs_write(void) { case_ml_reader_vs_write(); }
#else
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
#endif
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
#ifdef MULTILEVEL_CANDIDATE
    // ML-ADAPT: a new hit takes no reference while the allocator waits; it queues (it cannot copy under the mutex any more)
    struct op *c = reader(2, 1024, 512, 1, 0); start(c);
    REQUIRE(!AWAIT(c->g.entered, 400)); REQUIRE(!GET(c->done));
#else
    struct op *c = reader(2, 1024, 512, 1, 0); start(c); finish(c);   // no new pin while the allocator waits
    REQUIRE(c->g.held == 1); REQUIRE(got(c) == cached[2]);
#endif
    const int waits = GET(m2->w.waits);
    REQUIRE(!GET(m2->done)); REQUIRE(waits <= 2);
    release(p[2]); finish(m2);
    printf("observed allocator waits while all entries pinned: %d (after release: %d)\n", waits, m2->w.waits);
    REQUIRE(got(m2) == g5); REQUIRE(m2->w.waits <= waits + 2);
#ifdef MULTILEVEL_CANDIDATE
    finish(c); REQUIRE(got(c) == cached[2] + 100);              // block 2 was evicted: the queued hit re-reads it from the backend
    REQUIRE(has_block(0) && has_block(3) && has_block(4));      // pinned entries were never evicted
#else
    REQUIRE(!has_block(2) && has_block(0) && has_block(3) && has_block(4));
#endif
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
#ifdef ML_HAS_DRAINING
    // ML-ADAPT: shutdown first stops admission and waits for the active read (workers still run); stopping is set afterwards
    pthread_mutex_lock(&P->mutex); REQUIRE(P->draining && !P->stopping && P->active_ops == 1 && P->num_pins == 1); pthread_mutex_unlock(&P->mutex);
#else
    pthread_mutex_lock(&P->mutex); REQUIRE(P->num_threads == 0 && P->stopping && P->num_pins == 1); pthread_mutex_unlock(&P->mutex);
#endif
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
    for (int i = 0; i < WAIT_MS && !(state_of(0) == CLEAN && state_of(1) == CLEAN && state_of(3) == CLEAN); i += 5) nap(5);   // review N2: CLEAN follows the backend write asynchronously
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

// ---------------------------------------------------------------------------------------------
// Two-level locking cases (ml-*). Every "RED on baseline" assertion is behavioural: it observes, from a gated
// copy/write/snapshot, whether the cache-wide mutex is held, whether unrelated operations complete meanwhile,
// or whether a same-block operation can run concurrently. No source text is inspected.
// ---------------------------------------------------------------------------------------------
#define XDELAY 4294967232u                      // maximum safe write delay used by the protected profile
#define SHORT_NEG_MS 400                        // bounds negative observations only

static void open_x(u_int size) { open_cache2(size, 0, 0, XDELAY, 0); }
static struct op *writer(int b, int gen, int gated, int hold) {
    struct op *o = op_new(OP_WRITE, b); o->gen = gen; o->watched = 1;
    if (gated) { o->g.kind = disk ? VIA_PWRITE : VIA_MEMCPY; o->g.len = BS; o->g.hold = hold; o->g.armed = 1; }
    return o;
}
static struct op *flusher(void) { struct op *f = op_new(OP_FLUSH, 0); f->watched = 1; return f; }
static void arm_wsnap(int hold, int inject) {
    pthread_mutex_lock(&g_mu); memset(&wsnap, 0, sizeof(wsnap));
    wsnap.kind = disk ? VIA_PREAD : VIA_MEMCPY; wsnap.len = BS; wsnap.hold = hold; wsnap.inject = inject; wsnap.armed = 1;
    pthread_mutex_unlock(&g_mu);
}
static void release_wsnap(void) { pthread_mutex_lock(&g_mu); wsnap.released = 1; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu); }
static void wait_state(int b, int st) {
    for (int i = 0; i < WAIT_MS; i += 5) { if (state_of(b) == st) return; nap(5); }
    fprintf(stderr, "FAIL block %d state %d != %d\n", b, state_of(b), st); exit(1);
}
static void close_cache_keep(void) { REQUIRE(cache->shutdown(cache) == 0); }
static void flush_all(void) { REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == 0); }
static void rgate_permit_to(int n) { pthread_mutex_lock(&g_mu); rgate_permit = n; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu); }
static void wgate_open(int b) { pthread_mutex_lock(&g_mu); wgate_block = b; wgate_permit = 0; wgate_entered = 0; pthread_mutex_unlock(&g_mu); }
static void wgate_release(void) { pthread_mutex_lock(&g_mu); wgate_permit = 1; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu); }

// Two copies on DIRTY data must overlap (same entry and different entries): DIRTY hits share and do not hold the cache mutex.
static void dirty_two_reads(int b1, int b2, u_int off2, u_int len2, const char *label) {
    open_x(8); write_sync(b1, 500); if (b2 != b1) write_sync(b2, 501);
    REQUIRE(state_of(b1) == DIRTY && state_of(b2) == DIRTY);
    struct op *a = reader(b1, 0, BS, 1, 1), *b = reader(b2, off2, len2, 1, 1);
    start(a); REQUIRE(AWAIT(a->g.entered, WAIT_MS));
    start(b); const int overlap = AWAIT(b->g.entered, 1500);
    release(a); release(b);
    printf("observed %s: first_copy_under_cache_mutex=%d second_copy_overlapped=%d second_copy_under_cache_mutex=%d\n", label, a->g.held, overlap, b->g.held);
    REQUIRE(!a->g.held); REQUIRE(overlap); REQUIRE(!b->g.held);
    finish(a); finish(b);
    REQUIRE(got(a) == 500); REQUIRE(got(b) == (b2 == b1 ? 500 : 501));
    free(a); free(b);
}
static void case_ml_dirty_read_same(void) { dirty_two_reads(0, 0, 8, 4000, "dirty-same-entry"); }
static void case_ml_dirty_read_diff(void) { dirty_two_reads(0, 1, 1024, 2048, "dirty-different-entries"); }

// Writers to different blocks overlap; a hit elsewhere completes while one payload write is stalled.
static void write_overlap(int warm_first) {
    open_x(8);
    if (warm_first) { warm(0); warm(1); } else { write_sync(0, 500); write_sync(1, 501); }
    warm(2); poison(2);
    struct op *a = writer(0, 600, 1, 1), *b = writer(1, 601, 0, 0);
    start(a); REQUIRE(AWAIT(a->g.entered, WAIT_MS));
    printf("observed write-overlap(%s): stalled_payload_write_holds_cache_mutex=%d\n", warm_first ? "clean" : "dirty", a->g.held);
    REQUIRE(!a->g.held);
    start(b); REQUIRE(AWAIT(b->done, 3000));                    // different block completes while A is stalled
    finish(b); REQUIRE(b->r == 0); REQUIRE(read_sync(2, 0, BS) == cached[2]);
    struct op *c = reader(0, 0, BS, 1, 0); start(c);            // same block: must not enter its copy (no torn read)
    REQUIRE(!AWAIT(c->g.entered, SHORT_NEG_MS)); REQUIRE(!GET(c->done));
    release(a); finish(a); finish(c);
    REQUIRE(a->r == 0); REQUIRE(got(c) == 600);                 // whole new generation
    REQUIRE(read_sync(1, 0, BS) == 601);
    free(a); free(b); free(c);
}
static void case_ml_write_overlap_dirty(void) { write_overlap(0); }
static void case_ml_write_overlap_clean(void) { write_overlap(1); }

// Same-block exclusion: a stalled writer excludes readers and other writers on the same block, never the cache mutex.
static void case_ml_same_block_excl(void) {
    open_x(8); write_sync(0, 500);
    struct op *w = writer(0, 501, 1, 1), *w2 = writer(0, 502, 0, 0), *c = reader(0, 0, BS, 1, 0);
    start(w); REQUIRE(AWAIT(w->g.entered, WAIT_MS)); REQUIRE(!w->g.held);
    start(c); start(w2);
    REQUIRE(!AWAIT(c->g.entered, SHORT_NEG_MS)); REQUIRE(!GET(w2->done)); REQUIRE(!GET(c->done));
    release(w); finish(w); finish(c); finish(w2);
    REQUIRE(w->r == 0 && w2->r == 0); const int g = got(c); REQUIRE(g == 501 || g == 502);   // whole generation, never torn
    REQUIRE(read_sync(0, 0, BS) == 502); flush_all(); REQUIRE(GET(bgen[0]) == 502);
    free(w); free(w2); free(c);
}

// Writer preference/fairness: a reader arriving behind a waiting writer must not overtake it (Linux PREFER_WRITER rwlock).
static void case_ml_reader_vs_write(void) {
    open_x(8); warm(0); poison(0);
    struct op *a = reader(0, 0, BS, 1, 1), *w = writer(0, 500, 0, 0), *c = reader(0, 512, 1024, 1, 0);
    start(a); REQUIRE(AWAIT(a->g.entered, WAIT_MS));
    start(w); REQUIRE(AWAIT(w->w.waits >= 1, 3000)); REQUIRE(!GET(w->done));
#ifdef MULTILEVEL_CANDIDATE
    require_pins(0, 1, 1, 1);                                   // one shared reference (A), one exclusive (W)
#endif
    pthread_mutex_lock(&P->mutex); struct cache_entry *locked_entry=s3b_hash_get(P->hashtable,0); pthread_mutex_unlock(&P->mutex);
    int queued=0;
    for(int i=0;i<WAIT_MS;i++) { int q=pthread_rwlock_tryrdlock(&locked_entry->lock); if(q==EBUSY) { queued=1; break; } REQUIRE(q==0); pthread_rwlock_unlock(&locked_entry->lock); nap(1); }
    REQUIRE(queued); printf("observed native queued writer: libc tryrdlock=EBUSY while initial reader held\n");
    start(c);
    printf("observed reader-vs-write: later_reader_entered_while_writer_waits=%d\n", AWAIT(c->g.entered, SHORT_NEG_MS));
    REQUIRE(!GET(c->g.entered)); REQUIRE(!GET(c->done));
    release(a); finish(a); finish(w); finish(c);
    REQUIRE(got(a) == cached[0]); REQUIRE(w->r == 0);          // A kept one whole old generation
#ifdef __GLIBC__
    REQUIRE(got(c) == 500);                                     // writer first, reader sees the writer's generation
#else
    REQUIRE(got(c) == 500 || got(c) == cached[0]);
#endif
    flush_all(); REQUIRE(GET(bgen[0]) == 500);
    free(a); free(w); free(c);
}

// Bounded writer progress while readers hit the same block continuously.
static atomic_int fair_stop;
static void *fair_reader(void *arg) {
    struct worker_arg *w = arg; unsigned char buf[BS]; tl_test = 1;
    while (!atomic_load(&fair_stop)) {
        const int lo = atomic_load(&done_gen[0]);
        const int r = cache->read_block(cache, 0, buf, NULL, NULL, 0);
        const int hi = atomic_load(&started_gen[0]), g = r == 0 ? decode(buf, 0, 0, BS) : -1;
        if (g < lo || g > hi) { fprintf(stderr, "FAIL fair read gen=%d expected [%d,%d] r=%d\n", g, lo, hi, r); exit(1); }
    }
    (void)w; return NULL;
}
static void *fair_writer(void *arg) {
    (void)arg; unsigned char buf[BS]; tl_test = 1;
    for (int k = 1; k <= 25; k++) {
        atomic_store(&started_gen[0], 100 + k); fill(buf, 0, 100 + k);
        REQUIRE(cache->write_block(cache, 0, buf, NULL, NULL, NULL) == 0);
        atomic_store(&done_gen[0], 100 + k);
    }
    return NULL;
}
static void case_ml_writer_progress(void) {
    struct worker_arg r[4]; pthread_t wt;
    open_cache2(6, 0, 0, XDELAY, 0); write_sync(0, 100);
    atomic_store(&started_gen[0], 100); atomic_store(&done_gen[0], 100); atomic_store(&fair_stop, 0);
    for (int i = 0; i < 4; i++) REQUIRE(pthread_create(&r[i].t, NULL, fair_reader, &r[i]) == 0);
    nap(50);
    struct timespec t0, t1; clock_gettime(CLOCK_MONOTONIC, &t0);
    REQUIRE(pthread_create(&wt, NULL, fair_writer, NULL) == 0);
    REQUIRE(AWAIT(atomic_load(&done_gen[0]) == 125, 10000));  // writers finish despite continuous readers
    clock_gettime(CLOCK_MONOTONIC, &t1);
    atomic_store(&fair_stop, 1);
    for (int i = 0; i < 4; i++) REQUIRE(pthread_join(r[i].t, NULL) == 0);
    REQUIRE(pthread_join(wt, NULL) == 0);
    printf("observed writer-progress: 25 writes under 4 continuous readers in %ld ms\n", (long)((t1.tv_sec - t0.tv_sec) * 1000 + (t1.tv_nsec - t0.tv_nsec) / 1000000));
    REQUIRE(read_sync(0, 0, BS) == 125);
}

// Upload snapshot copies without the cache mutex; same-block writers wait for the snapshot, others do not.
static void case_ml_snapshot(void) {
    open_x(8); warm(2); poison(2); write_sync(0, 500); REQUIRE(state_of(0) == DIRTY);
    arm_wsnap(1, 0);
    struct op *f = flusher(); start(f);
    REQUIRE(AWAIT(wsnap.entered, WAIT_MS));
    const int held = GET(wsnap.held);
    printf("observed worker snapshot: copy_under_cache_mutex=%d\n", held);
    REQUIRE(!held);
    write_sync(1, 501); REQUIRE(read_sync(2, 0, BS) == cached[2]);          // unrelated blocks proceed
    REQUIRE(read_sync(0, 0, BS) == 500);                                       // a reader shares with the snapshot
    struct op *w = writer(0, 502, 0, 0); start(w);
    REQUIRE(!AWAIT(w->done, SHORT_NEG_MS));                                    // same-block writer waits for the snapshot
    release_wsnap(); finish(w); finish(f);
    REQUIRE(w->r == 0 && f->r == 0);
    flush_all();                                                               // the flush above may predate the rewrite; flush again
    REQUIRE(GET(bgen[0]) == 502 && GET(bwrites[0]) == 2);                      // old PUT (500) then the rewrite (502)
    free(w); free(f);
}

// Pending writer versus FLUSH: FLUSH must not finish before the writer's payload is committed and unlocked.
static void case_ml_flush_pending_write(void) {
    open_x(8); warm(0);
    struct op *w = writer(0, 800, 1, 1); start(w); REQUIRE(AWAIT(w->g.entered, WAIT_MS));
    REQUIRE(!w->g.held);
#ifdef MULTILEVEL_CANDIDATE
    REQUIRE(state_of(0) == DIRTY);
#endif
    struct op *f = flusher(); start(f);
    REQUIRE(AWAIT(f->w.waits >= 1, WAIT_MS)); REQUIRE(!AWAIT(f->done, SHORT_NEG_MS));
    REQUIRE(GET(bgen[0]) == cached[0]);                                        // nothing was uploaded under the writer
    release(w); finish(w); finish(f);
    REQUIRE(w->r == 0 && f->r == 0); REQUIRE(GET(bgen[0]) == 800); REQUIRE(state_of(0) == CLEAN);
    free(w); free(f);
}

// Shutdown must not stop workers before an in-flight write can publish its dirty state.
static void case_ml_shutdown_pending_write(void) {
    open_x(8);
    struct op *w = writer(5, 810, 1, 1), *s = op_new(OP_SHUTDOWN, 0); start(w); REQUIRE(AWAIT(w->g.entered, WAIT_MS));
    REQUIRE(!w->g.held);
    start(s); REQUIRE(!AWAIT(s->done, SHORT_NEG_MS));
    release(w); finish(w); finish(s);
    REQUIRE(w->r == 0 && s->r == 0); REQUIRE(GET(bgen[5]) == 810);            // the write was drained by the workers
    cache->destroy(cache); cache = NULL; free(w); free(s);
}

// Old PUT versus new rewrite: nothing waits on the network and the older PUT never marks the newer generation clean.
static void case_ml_upload_rewrite(void) {
    open_cache(8, 0, 0); wgate_open(1);
    write_sync(1, 700); REQUIRE(AWAIT(wgate_entered >= 1, WAIT_MS)); REQUIRE(state_of(1) == WRITING);
    write_sync(1, 701);                                                        // completes while the PUT is in flight
    REQUIRE(state_of(1) == WRITING2); REQUIRE(read_sync(1, 0, BS) == 701);
    wgate_release();
    wait_clean(); REQUIRE(GET(bgen[1]) == 701 && GET(bwrites[1]) == 2); REQUIRE(state_of(1) == CLEAN);
}

// Miss singleflight (including read-ahead) and failed-fill retry.
static void case_ml_singleflight(void) {
    open_cache(8, 0, 0); rgate_block = 5;
    struct op *a = reader(5, 0, BS, 0, 0), *b = reader(5, 0, BS, 0, 0); b->watched = 1;
    start(a); REQUIRE(AWAIT(rgate_entered == 1, WAIT_MS));
    start(b); REQUIRE(AWAIT(b->w.waits >= 1, WAIT_MS)); REQUIRE(!GET(b->done));
    nap(100); REQUIRE(GET(rgate_entered) == 1);
    rgate_permit_to(1); finish(a); finish(b);
    REQUIRE(got(a) == GET(bgen[5]) && got(b) == GET(bgen[5])); REQUIRE(GET(breads[5]) == 1);
    free(a); free(b);
}
static void case_ml_fill_error(void) {
    open_cache(8, 0, 0); rgate_block = 5; rgate_fail = 1;
    struct op *a = reader(5, 0, BS, 0, 0), *b = reader(5, 0, BS, 0, 0); b->watched = 1;
    start(a); REQUIRE(AWAIT(rgate_entered == 1, WAIT_MS));
    start(b); REQUIRE(AWAIT(b->w.waits >= 1, WAIT_MS));
    rgate_permit_to(2);                                                        // first read fails, the waiter becomes the owner
    finish(a); finish(b);
    REQUIRE(a->r == EIO && b->r == 0 && got(b) == GET(bgen[5])); REQUIRE(GET(rgate_entered) == 2); REQUIRE(state_of(5) == CLEAN);
#ifdef MULTILEVEL_CANDIDATE
    require_pins(5, 0, 0, 0);
#endif
    for (int x = 0; x < 3; x++) REQUIRE(read_sync(5, 0, BS) == GET(bgen[5]));
    free(a); free(b);
}
static void case_ml_readahead(void) {
    open_cache2(8, 0, 0, 0, 2); rgate_block = 3;
    REQUIRE(read_sync(1, 0, BS) == GET(bgen[1])); REQUIRE(read_sync(2, 0, BS) == GET(bgen[2]));   // two sequential reads trigger read-ahead
    REQUIRE(AWAIT(rgate_entered == 1, WAIT_MS));                               // read-ahead of block 3 is in the backend
    struct op *a = reader(3, 0, BS, 0, 0); a->watched = 1; start(a); REQUIRE(AWAIT(a->w.waits >= 1, WAIT_MS));
    rgate_permit_to(1); finish(a);
    REQUIRE(got(a) == GET(bgen[3])); REQUIRE(GET(breads[3]) == 1);             // one backend read for both
    free(a);
}

// Full cache, entries busy: an allocator sleeps without spinning and new hits do not starve it; slots are reused safely.
static void case_ml_evict_gate(void) {
    open_cache(4, 0, 0);
    for (int b = 0; b < 4; b++) { warm(b); poison(b); }
    struct op *p[6] = { 0 };
    for (int b = 0; b < 4; b++) { p[b] = reader(b, 0, BS, 1, 1); start(p[b]); REQUIRE(AWAIT(p[b]->g.entered, WAIT_MS)); }
    const int g5 = GET(bgen[5]);
    struct op *m = reader(5, 0, BS, 0, 0); m->watched = 1; start(m);
    REQUIRE(AWAIT(m->w.waits >= 1, WAIT_MS)); REQUIRE(!GET(m->done));
    pthread_mutex_lock(&P->mutex); REQUIRE(P->space_pin_waiters == 1 && P->num_pins == 4); pthread_mutex_unlock(&P->mutex);
    struct op *c = reader(2, 1024, 512, 1, 0); start(c);                       // must not take a new pin and starve the allocator
    REQUIRE(!AWAIT(c->g.entered, SHORT_NEG_MS)); REQUIRE(!GET(c->done));
    const int waits = GET(m->w.waits); REQUIRE(waits <= 2);
    release(p[2]); finish(p[2]); finish(m);
    REQUIRE(got(m) == g5);
    finish(c);                                                                  // evicted block: C misses and reads the backend
    REQUIRE(got(c) == cached[2] + 100); REQUIRE(m->w.waits <= waits + 2);
    release(p[0]); release(p[1]); release(p[3]);
    for (int b = 0; b < 4; b++) if (b != 2) { finish(p[b]); REQUIRE(got(p[b]) == cached[b]); }
    free(m); free(c);
}
// Every entry DIRTY with the PUT stalled: a miss waits without spinning the workers, then completes.
static void case_ml_full_dirty(void) {
    open_x(1); wgate_open(0);
    write_sync(0, 500);
    struct op *m = reader(7, 0, BS, 0, 0); m->watched = 1; start(m);
    REQUIRE(AWAIT(wgate_entered >= 1, WAIT_MS));                               // nudged head PUT is stalled
    REQUIRE(AWAIT(m->w.waits >= 1, WAIT_MS)); REQUIRE(!GET(m->done));
    const unsigned w0 = atomic_load(&worker_waits); nap(SHORT_NEG_MS); const unsigned w1 = atomic_load(&worker_waits);
    printf("observed worker cond waits while every entry is DIRTY and a miss waits: %u\n", w1 - w0);
    REQUIRE(w1 - w0 < 50); REQUIRE(!GET(m->done));
    wgate_release(); finish(m); REQUIRE(got(m) == GET(bgen[7]));
    free(m);
}
// Reload leaves CLEAN2 entries: concurrent readers and a writer share one ETag verification and see whole generations.
static void case_ml_clean2_shared(void) {
    open_cache(8, 0, 0); REQUIRE(disk); warm(0); close_cache(); open_cache(8, 0, 0);
    REQUIRE(state_of(0) == CLEAN2);
    const int v0 = GET(bverifies);
    struct op *r1 = reader(0, 0, BS, 0, 0), *r2 = reader(0, 0, BS, 0, 0), *w = writer(0, 850, 0, 0);
    start(r1); start(r2); start(w); finish(r1); finish(r2); finish(w);
    REQUIRE(GET(bverifies) <= v0 + 1); REQUIRE(w->r == 0);
    REQUIRE(got(r1) == cached[0] || got(r1) == 850); REQUIRE(got(r2) == cached[0] || got(r2) == 850);
    REQUIRE(read_sync(0, 0, BS) == 850); wait_clean(); REQUIRE(GET(bgen[0]) == 850);
    free(r1); free(r2); free(w);
}
// Lower-layer failures: payload pwrite error and snapshot pread error keep accounting exact and retry.
static void case_ml_pwrite_error(void) {
    open_x(4); REQUIRE(disk); write_sync(0, 500);
    struct op *w = writer(0, 501, 1, 0); w->g.inject = EIO; start(w); finish(w); REQUIRE(w->g.entered); REQUIRE(w->r == 0);   // error is logged, as before
#ifdef MULTILEVEL_CANDIDATE
    require_pins(0, 0, 0, 0);
#endif
    write_sync(0, 502); REQUIRE(read_sync(0, 0, BS) == 502);
    REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == 0); wait_clean(); REQUIRE(GET(bgen[0]) == 502);
    free(w);
}
static void case_ml_snapshot_error(void) {
    open_cache(4, 0, 0); REQUIRE(disk);                                        // write delay 0: immediate upload
    arm_wsnap(0, EIO);
    write_sync(0, 500); REQUIRE(AWAIT(wsnap.entered, WAIT_MS));
    wait_clean();                                                              // retried after the (short) delay: accounting returns to zero
    REQUIRE(GET(bgen[0]) == 500 && state_of(0) == CLEAN);
#ifdef MULTILEVEL_CANDIDATE
    require_pins(0, 0, 0, 0);
#endif
}
// Restart: dirty data flushed by shutdown reloads as CLEAN2 and verifies against the backend.
static void case_ml_restart(void) {
    open_x(8); REQUIRE(disk); for (int b = 0; b < 3; b++) write_sync(b, 900 + b);
    close_cache(); for (int b = 0; b < 3; b++) REQUIRE(GET(bgen[b]) == 900 + b);
    open_x(8); for (int b = 0; b < 3; b++) { REQUIRE(state_of(b) == CLEAN2); REQUIRE(read_sync(b, 0, BS) == 900 + b); }
}
// Slot reuse under writes: more blocks than entries; everything reads back and survives a restart (no stale/duplicate records).
static void case_ml_slot_reuse(void) {
    open_cache(3, 0, 0);
    for (int pass = 0; pass < 3; pass++) for (int b = 0; b < 8; b++) { write_sync(b, 1000 + 10 * pass + b); wait_clean(); }
    for (int b = 0; b < 8; b++) REQUIRE(read_sync(b, 0, BS) == 1020 + b);
    close_cache();
    if (disk) { open_cache(3, 0, 0); for (int b = 0; b < 8; b++) REQUIRE(read_sync(b, 0, BS) == 1020 + b); }
}

// Concurrent payload writes of fresh dslots extend the cache file together (dcache file_size / size_mutex).
static void *extend_writer(void *arg) {
    struct worker_arg *w = arg; unsigned char buf[BS]; tl_test = 1;
    fill(buf, w->id, 300 + w->id); REQUIRE(cache->write_block(cache, w->id, buf, NULL, NULL, NULL) == 0);
    return NULL;
}
static void case_ml_dcache_extend(void) {
    struct worker_arg w[12];
    open_x(NB); REQUIRE(disk);
    for (int i = 0; i < 12; i++) { w[i].id = i; REQUIRE(pthread_create(&w[i].t, NULL, extend_writer, &w[i]) == 0); }
    for (int i = 0; i < 12; i++) REQUIRE(pthread_join(w[i].t, NULL) == 0);
    for (int i = 0; i < 12; i++) REQUIRE(read_sync(i, 0, BS) == 300 + i);
    flush_all(); for (int i = 0; i < 12; i++) REQUIRE(GET(bgen[i]) == 300 + i);
    close_cache(); open_x(NB);                                                  // restart: every record is valid, data intact
    for (int i = 0; i < 12; i++) REQUIRE(read_sync(i, 0, BS) == 300 + i);
}

// Shutdown versus requests that arrive or are active while it drains (review B1/B2). A request is either rejected with ENOTCONN
// (nothing acknowledged) or admitted, in which case shutdown must not return before its payload/fill is committed and any
// acknowledged write has reached the backend. destroy() afterwards must be safe (assert builds abort on a leftover DIRTY entry).
static int shutdown_begun(void) {
    pthread_mutex_lock(&P->mutex);
#ifdef ML_HAS_DRAINING
    int v = P->draining || P->stopping;
#else
    int v = P->stopping;
#endif
    pthread_mutex_unlock(&P->mutex); return v;
}
static void wait_shutdown_begun(void) {
    for (int i = 0; i < WAIT_MS; i += 2) { if (shutdown_begun()) return; nap(2); }
    fprintf(stderr, "FAIL setup: shutdown did not begin\n"); exit(2);
}
static void wait_no_workers(void) {
    for (int i = 0; i < WAIT_MS; i += 2) { pthread_mutex_lock(&P->mutex); const u_int n = P->num_threads; pthread_mutex_unlock(&P->mutex); if (n == 0) return; nap(2); }
}
static void case_ml_late_write_hit(void) {
    open_x(8); warm(0); wgate_open(1); write_sync(1, 600);
    struct op *r = reader(0, 0, BS, 1, 1), *s = op_new(OP_SHUTDOWN, 0), *w = writer(0, 820, 0, 0);
    start(r); REQUIRE(AWAIT(r->g.entered, WAIT_MS));                           // a reader holds the entry's lock
    start(s); REQUIRE(AWAIT(wgate_entered >= 1, WAIT_MS)); wait_shutdown_begun();   // drain begun, one worker held in a PUT
    start(w); REQUIRE(AWAIT(w->done, 400) || GET(w->w.waits) >= 1);            // late write: rejected, or admitted and waiting
    wgate_release(); nap(100);
    release(r); finish(r); finish(w); finish(s);
    REQUIRE(s->r == 0); REQUIRE(w->r == 0 || w->r == ENOTCONN);
    printf("late-write-hit: write_r=%d shutdown_r=%d backend_gen=%d dirties=%u\n", w->r, s->r, GET(bgen[0]), P->num_dirties);
    if (w->r == 0) REQUIRE(GET(bgen[0]) == 820);                                // acknowledged => uploaded by the successful shutdown
    else REQUIRE(GET(bgen[0]) == cached[0]);
    REQUIRE(P->num_dirties == 0); REQUIRE(P->num_refs == 0);
    cache->destroy(cache); cache = NULL; free(r); free(s); free(w);
}
static void case_ml_late_write_miss(void) {
    open_x(8); wgate_open(1); write_sync(1, 600);
    struct op *s = op_new(OP_SHUTDOWN, 0), *w = writer(5, 830, 1, 1);
    start(s); REQUIRE(AWAIT(wgate_entered >= 1, WAIT_MS)); wait_shutdown_begun();
    start(w); REQUIRE(AWAIT(w->g.entered || w->done, WAIT_MS));
    const int admitted = GET(w->g.entered);
    wgate_release();
    const int early = AWAIT(s->done, 1000) && !GET(w->done);                    // shutdown returned while the payload was in flight
    release(w); finish(w); finish(s);
    printf("late-write-miss: admitted=%d shutdown_returned_while_payload_in_flight=%d write_r=%d shutdown_r=%d backend_gen=%d\n",
      admitted, early, w->r, s->r, GET(bgen[5]));
    REQUIRE(!early); REQUIRE(s->r == 0); REQUIRE(w->r == 0 || w->r == ENOTCONN);
    if (w->r == 0) REQUIRE(GET(bgen[5]) == 830); else REQUIRE(GET(bgen[5]) == 15);
    REQUIRE(P->num_dirties == 0 && P->num_refs == 0);
    cache->destroy(cache); cache = NULL; free(s); free(w);
}
// A miss owner is in the lower layer while shutdown runs: shutdown must wait for it (it holds no reference).
static void case_ml_read_owner(void) {
    open_cache(8, 0, 0); rgate_block = 5;
    struct op *a = reader(5, 0, BS, 0, 0), *s = op_new(OP_SHUTDOWN, 0);
    start(a); REQUIRE(AWAIT(rgate_entered == 1, WAIT_MS));
    start(s); const int early = AWAIT(s->done, 1000);
    rgate_permit_to(1); finish(a); finish(s);
    printf("read-owner: shutdown_returned_while_owner_in_backend=%d read_r=%d shutdown_r=%d\n", early, a->r, s->r);
    REQUIRE(!early); REQUIRE(a->r == 0 && s->r == 0);
    cache->destroy(cache); cache = NULL; free(a); free(s);
}
// A request after shutdown completed is rejected, never admitted.
static void case_ml_after_shutdown(void) {
    open_x(8); warm(0); write_sync(1, 700); close_cache_keep();
    unsigned char buf[BS]; fill(buf, 2, 710);
    REQUIRE(P->num_threads == 0);
    REQUIRE(cache->write_block(cache, 2, buf, NULL, NULL, NULL) == ENOTCONN);
    REQUIRE(cache->read_block(cache, 0, buf, NULL, NULL, 0) == ENOTCONN);
    REQUIRE(GET(bgen[1]) == 700);
    cache->destroy(cache); cache = NULL;
}

// Local durability tests execute the actual cache and disk-cache sources.
static void lf_open(void) { local_mode = 1; open_cache2(8, 0, 0, 64, 0); REQUIRE(disk); }
static void case_lf_local(void) {
    lf_open(); write_sync(3, 501);
    unsigned before = atomic_load(&sync_calls);
    REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == 0);
    REQUIRE(atomic_load(&sync_calls) > before);
    REQUIRE(GET(bwrites[3]) == 0); REQUIRE(state_of(3) == DIRTY);
    s3b_block_t b = 3; write_sync(3, 502);
    REQUIRE(cache->flush_blocks(cache, &b, 1, 0) == 0);
    nap(800); REQUIRE(GET(bwrites[3]) == 0); REQUIRE(state_of(3) == DIRTY);
    REQUIRE(read_sync(3, 0, BS) == 502);
}
static void case_lf_default(void) {
    open_cache2(8, 0, 0, 60000, 0); write_sync(3, 501);
    REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == 0); REQUIRE(GET(bwrites[3]) == 1);
}
static void case_lf_sync_error(void) {
    lf_open(); write_sync(3, 501); atomic_store(&sync_fail, 1);
    REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == EIO);
    REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == EIO);
    struct op *w = writer(4, 502, 0, 0); start(w); finish(w); REQUIRE(w->r == EIO); free(w);
    // A poisoned mount intentionally retains dirty entries; process exit simulates loss of the mount.
    _exit(0);
}
static void case_lf_pwrite_error(void) {
    lf_open(); struct op *w = writer(4, 502, 1, 0); w->g.inject = EIO;
    start(w); finish(w); REQUIRE(w->r == EIO); REQUIRE(!has_block(4)); free(w);
    REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == EIO); _exit(0);
}
static void case_lf_drain(void) {
    lf_open(); write_sync(3, 501);
#ifdef ML_HAS_LOCAL_FLUSH
    REQUIRE(block_cache_remote_drain(cache, 5000) == 0);
#else
    REQUIRE(0 && "local drain API absent");
#endif
    REQUIRE(GET(bwrites[3]) == 1); REQUIRE(GET(bgen[3]) == 501);
}
static void case_lf_pending(void) {
    lf_open(); struct op *w = writer(3, 501, 1, 1), *f = op_new(OP_FLUSH, 0);
    start(w); REQUIRE(AWAIT(w->g.entered, WAIT_MS)); start(f);
    REQUIRE(!AWAIT(f->done, 200)); release(w); finish(w); finish(f);
    REQUIRE(w->r == 0 && f->r == 0); REQUIRE(GET(bwrites[3]) == 0); free(w); free(f);
}

static void case_lf_signal(void) {
    lf_open(); write_sync(3, 505); struct block_cache_stats st;
    REQUIRE(kill(getpid(), SIGUSR1) == 0);
    for (int i=0; i<WAIT_MS; i+=5) { block_cache_get_stats(cache, &st);
#ifdef ML_HAS_LOCAL_FLUSH
        if (st.drain_completed) break;
#endif
        nap(5);
    }
#ifdef ML_HAS_LOCAL_FLUSH
    REQUIRE(st.local_flush && st.drain_pid == (u_int)getpid());
    REQUIRE(st.drain_requested == 1 && st.drain_completed == 1 && st.drain_error == 0);
    block_cache_clear_stats(cache); block_cache_get_stats(cache, &st); REQUIRE(st.drain_completed == 1);
#else
    REQUIRE(0);
#endif
    REQUIRE(GET(bgen[3]) == 505);
}
static void case_lf_drain_error(void) {
    lf_open(); write_sync(3, 505); atomic_store(&backend_fail, 1);
#ifdef ML_HAS_LOCAL_FLUSH
    REQUIRE(block_cache_remote_drain(cache, 5000) == EIO);
    struct block_cache_stats st; block_cache_get_stats(cache, &st);
    REQUIRE(st.drain_completed == 1 && st.drain_error == EIO);
#else
    REQUIRE(0);
#endif
    REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == 0);
    atomic_store(&backend_fail, 0);
}
static void case_lf_shutdown_error(void) {
    lf_open(); write_sync(3, 505); atomic_store(&backend_fail, 1);
    REQUIRE(cache->shutdown(cache) == EIO); cache->destroy(cache); cache = NULL;
    int fd=open(getenv("S3BACKER_TEST_CACHE"),O_RDONLY); uint32_t flags=0;
    REQUIRE(fd>=0 && pread(fd,&flags,4,24)==4); REQUIRE(flags & 2); close(fd);
}
static void case_lf_pressure(void) {
    lf_open(); for (int b=0;b<7;b++) write_sync(b, 500+b);
    for (int b=0;b<7;b++) REQUIRE(GET(bwrites[b])==0);
    write_sync(7,507); write_sync(8,508);
    REQUIRE(read_sync(8,0,BS)==508); int n=0; for(int b=0;b<8;b++) n+=GET(bwrites[b]); REQUIRE(n>0);
}
static void case_lf_metadata_error(void) {
    lf_open(); atomic_store(&metadata_fail,1); struct op *w=writer(3,505,0,0); start(w); finish(w);
    REQUIRE(w->r==EIO && !has_block(3)); free(w); REQUIRE(cache->flush_blocks(cache,NULL,0,0)==EIO); _exit(0);
}
static void case_lf_create_error(void) {
#ifdef ML_HAS_LOCAL_FLUSH
    struct block_cache_conf c={.block_size=BS,.cache_size=8,.num_threads=2,.local_flush=1,.recover_dirty_blocks=1,.perform_flush=1,.cache_file=getenv("S3BACKER_TEST_CACHE"),.log=logger};
    atomic_store(&fsync_fail,1); errno=0; REQUIRE(block_cache_create(&c,&inner)==NULL && errno==EIO);
    atomic_store(&fsync_fail,2); errno=0; REQUIRE(block_cache_create(&c,&inner)==NULL && errno==EIO);
#else
    REQUIRE(0);
#endif
}
static void case_lf_invalid(void) {
#ifdef ML_HAS_LOCAL_FLUSH
    struct block_cache_conf c={.block_size=BS,.cache_size=8,.num_threads=2,.local_flush=1,.recover_dirty_blocks=1,.log=logger};
    REQUIRE(block_cache_create(&c,&inner)==NULL && errno==EINVAL);
    c.cache_file=getenv("S3BACKER_TEST_CACHE"); c.recover_dirty_blocks=0;
    REQUIRE(block_cache_create(&c,&inner)==NULL && errno==EINVAL); c.recover_dirty_blocks=1; c.no_verify=1;
    REQUIRE(block_cache_create(&c,&inner)==NULL && errno==EINVAL); c.no_verify=0; c.synchronous=1;
    REQUIRE(block_cache_create(&c,&inner)==NULL && errno==EINVAL);
#else
    REQUIRE(0);
#endif
}
static void case_lf_crash(void) {
    int fds[2], status; REQUIRE(pipe(fds)==0); pid_t child=fork(); REQUIRE(child>=0);
    if (!child) {
        close(fds[0]); lf_open(); warm(2); write_sync(2,601); write_sync(3,602);
        REQUIRE(cache->flush_blocks(cache,NULL,0,0)==0); write_sync(2,603);
        unsigned char buf[BS]; fill(buf,3,604);
        REQUIRE(cache->write_block_part(cache,3,512,1024,buf+512)==0);
        REQUIRE(cache->flush_blocks(cache,NULL,0,0)==0); REQUIRE(write(fds[1],"!",1)==1);
        for (;;) pause();
    }
    close(fds[1]); char ack; REQUIRE(read(fds[0],&ack,1)==1); close(fds[0]);
    REQUIRE(kill(child,SIGKILL)==0 && waitpid(child,&status,0)==child && WIFSIGNALED(status));
#ifdef ML_HAS_LOCAL_FLUSH
    struct block_cache_conf c={.block_size=BS,.cache_size=8,.num_threads=2,.recover_dirty_blocks=1,.log=logger,.cache_file=getenv("S3BACKER_TEST_CACHE")};
    struct s3backer_store *bad=block_cache_create(&c,&inner); REQUIRE(!bad && errno==EBUSY);
#endif
    lf_open(); REQUIRE(read_sync(2,0,BS)==603);
    REQUIRE(read_sync(3,0,512)==602); REQUIRE(read_sync(3,512,1024)==604); REQUIRE(read_sync(3,1536,BS-1536)==602);
    nap(300); REQUIRE(GET(bwrites[2])==0 && GET(bwrites[3])==0);
    // Mixed partial payload is intentionally not passed to generation-only backend: simulate a second crash.
    _exit(0);
}
// Enumerate every byte-prefix tear of an invalid miss identity in sector-straddling slot 18.
// Payload deliberately belongs to block 7, identity to block 9: no tear may publish either as DIRTY.
static int tear_visits;
static int tear_visit(void *arg,s3b_block_t slot,s3b_block_t block,const u_char *tag) { (void)arg;(void)slot;(void)block;(void)tag; tear_visits++; return 0; }
static void case_lf_identity_tears(void) {
#ifdef ML_HAS_LOCAL_FLUSH
    struct block_cache_conf c={.block_size=BS,.cache_size=32,.num_threads=2,.local_flush=1,.recover_dirty_blocks=1,.perform_flush=1,.log=logger,.cache_file=getenv("S3BACKER_TEST_CACHE")};
    for(int prefix=0;prefix<=24;prefix++) {
        unlink(c.cache_file); struct s3b_dcache *d; REQUIRE(s3b_dcache_open(&d,&c,tear_visit,NULL,1)==0);
        unsigned char rec[24], zero[24]={0}, buf[BS]; fill(buf,7,800);
        REQUIRE(s3b_dcache_reserve(d,18,9)==0);
        REQUIRE(s3b_dcache_write_block(d,18,buf,0,BS)==0);
        s3b_dcache_close(d); int fd=open(c.cache_file,O_RDWR); REQUIRE(fd>=0);
        REQUIRE(pread(fd,rec,24,64+18*24)==24); // actual reserve record, no invented producer
        REQUIRE(pwrite(fd,zero,24,64+18*24)==24);
        REQUIRE(pwrite(fd,rec,prefix,64+18*24)==prefix); REQUIRE(fdatasync(fd)==0); close(fd);
        tear_visits=0; REQUIRE(s3b_dcache_open(&d,&c,tear_visit,NULL,1)==0); REQUIRE(tear_visits==0); s3b_dcache_close(d);
    }
#else
    REQUIRE(0);
#endif
}

static void case_lf_marker_error(void) {
    lf_open(); warm(3); atomic_store(&fail_pwrite_offset, 64+20);
    struct op *w=writer(3,505,0,0); start(w); finish(w); REQUIRE(w->r==EIO); free(w);
    REQUIRE(cache->flush_blocks(cache,NULL,0,0)==EIO); _exit(0);
}
static void case_lf_header_error(void) {
#ifdef ML_HAS_LOCAL_FLUSH
    struct block_cache_conf c={.block_size=BS,.cache_size=8,.num_threads=2,.recover_dirty_blocks=1,.log=logger,.cache_file=getenv("S3BACKER_TEST_CACHE")};
    struct s3b_dcache *d; REQUIRE(s3b_dcache_open(&d,&c,tear_visit,NULL,1)==0); s3b_dcache_close(d);
    c.local_flush=1; atomic_store(&fail_pwrite_offset,24); REQUIRE(s3b_dcache_open(&d,&c,tear_visit,NULL,1)==EIO);
#else
    REQUIRE(0);
#endif
}
static void case_lf_clean2_write(void) {
    lf_open(); warm(3); close_cache();
    int fd=open(getenv("S3BACKER_TEST_CACHE"),O_RDWR); uint32_t stale=0;
    REQUIRE(fd>=0 && pwrite(fd,&stale,4,64)==4 && fdatasync(fd)==0); close(fd);
    lf_open(); REQUIRE(state_of(0)==CLEAN2); int before=GET(breads[0]); write_sync(0,700);
    REQUIRE(GET(breads[0])==before+1); REQUIRE(read_sync(0,0,BS)==700);
}
static void case_lf_sync_concurrent(void) {
    lf_open(); write_sync(3,505); atomic_store(&sync_fail,1);
    struct op *a=op_new(OP_FLUSH,0), *b=op_new(OP_FLUSH,0); start(a); start(b); finish(a); finish(b);
    REQUIRE(a->r==EIO && b->r==EIO); free(a); free(b); _exit(0);
}
static void case_lf_reopen_gates(void) {
#ifdef ML_HAS_LOCAL_FLUSH
    lf_open(); write_sync(3,505); REQUIRE(cache->flush_blocks(cache,NULL,0,0)==0);
    REQUIRE(cache->shutdown(cache)==0); cache->destroy(cache); cache=NULL;
    struct block_cache_conf c=cc; struct s3b_dcache *d;
    c.local_flush=0; REQUIRE(s3b_dcache_open(&d,&c,tear_visit,NULL,0)==EBUSY);
    c.local_flush=1; c.recover_dirty_blocks=0; REQUIRE(s3b_dcache_open(&d,&c,tear_visit,NULL,0)==EBUSY);
    c.recover_dirty_blocks=1; c.cache_size=9; REQUIRE(s3b_dcache_open(&d,&c,tear_visit,NULL,1)==EINVAL);
    c.cache_size=8; REQUIRE(s3b_dcache_open(&d,&c,tear_visit,NULL,0)==0); s3b_dcache_close(d); // all entries are now clean
#else
    REQUIRE(0);
#endif
}

static void case_lf_erase_order(void) {
    lf_open(); write_sync(3,505); REQUIRE(cache->flush_blocks(cache,NULL,0,0)==0);
    atomic_store(&erase_observer,1); REQUIRE(s3b_dcache_fsync(P->dcache)==0); REQUIRE(atomic_load(&synced_marker)==1);
    u_char tag[16]; etag_of(tag,3,505); REQUIRE(s3b_dcache_record_block(P->dcache,0,3,tag)==0);
    REQUIRE(s3b_dcache_erase_block(P->dcache,0)==0); atomic_store(&erase_observer,0);
    // Direct directory experiment has retired its entry; do not use the live hash again.
    _exit(0);
}
static void case_lf_generation(void) {
    lf_open(); wgate_open(1); write_sync(1,700);
    struct op *f=op_new(OP_DRAIN,0); start(f); REQUIRE(AWAIT(wgate_entered>=1,WAIT_MS));
    write_sync(1,701); REQUIRE(state_of(1)==WRITING2); REQUIRE(read_sync(1,0,BS)==701);
    REQUIRE(cache->flush_blocks(cache,NULL,0,0)==0); wgate_release(); finish(f); REQUIRE(f->r==0); free(f);
    REQUIRE(GET(bgen[1])==701 && GET(bwrites[1])==2 && state_of(1)==CLEAN);
}
static void case_lf_shutdown_pending(void) { local_mode=1; case_ml_shutdown_pending_write(); }
static void case_lf_stress(void) {
    lf_open(); struct worker_arg r[6], w[2], f;
    for(int b=0;b<SB;b++){ atomic_store(&started_gen[b],bgen[b]); atomic_store(&done_gen[b],bgen[b]); }
    for(int i=0;i<6;i++){ r[i].id=i; r[i].seed=1000+i; REQUIRE(pthread_create(&r[i].t,NULL,stress_reader,&r[i])==0); }
    for(int i=0;i<2;i++){ w[i].id=i; w[i].seed=2000+i; REQUIRE(pthread_create(&w[i].t,NULL,stress_writer,&w[i])==0); }
    REQUIRE(pthread_create(&f.t,NULL,stress_flusher,NULL)==0);
    for(int i=0;i<6;i++) REQUIRE(pthread_join(r[i].t,NULL)==0);
    for(int i=0;i<2;i++) REQUIRE(pthread_join(w[i].t,NULL)==0);
    REQUIRE(pthread_join(f.t,NULL)==0);
#ifdef ML_HAS_LOCAL_FLUSH
    REQUIRE(block_cache_remote_drain(cache,5000)==0);
#else
    REQUIRE(0);
#endif
    for(int b=0;b<SB;b++) REQUIRE(GET(bgen[b])==atomic_load(&done_gen[b]));
}

static void case_lf_read_miss_error(void) {
    lf_open(); struct op *r=reader(3,0,BS,0,0); r->g.kind=VIA_PWRITE; r->g.len=BS; r->g.armed=1; r->g.inject=EIO;
    start(r); finish(r); REQUIRE(r->r==EIO && !has_block(3)); free(r);
    REQUIRE(s3b_dcache_size(P->dcache)==1); REQUIRE(cache->flush_blocks(cache,NULL,0,0)==EIO); _exit(0);
}
static void case_lf_erase_error(void) {
    lf_open(); for(int b=0;b<8;b++) warm(b); atomic_store(&fail_pwrite_offset,84);
    struct op *w=writer(9,505,0,0); start(w); finish(w); REQUIRE(w->r==EIO && !has_block(9)); free(w);
    REQUIRE(s3b_dcache_size(P->dcache)==8); REQUIRE(cache->flush_blocks(cache,NULL,0,0)==EIO); _exit(0);
}
static void case_lf_clean_migration(void) {
    lf_open(); write_sync(3,505); REQUIRE(cache->shutdown(cache)==0);
    REQUIRE(cache->set_mount_token(cache,NULL,0)==0); cache->destroy(cache); cache=NULL;
    int fd=open(getenv("S3BACKER_TEST_CACHE"),O_RDONLY); uint32_t flags=0;
    REQUIRE(fd>=0 && pread(fd,&flags,4,24)==4); REQUIRE(flags==1); close(fd);
    local_mode=0; open_cache(8,0,0); REQUIRE(read_sync(3,0,BS)==505);
}

static void case_lf_maxdirty(void) {
    local_max_dirty=2; lf_open(); write_sync(0,700); REQUIRE(GET(bwrites[0])==0);
    write_sync(1,701); write_sync(2,702); REQUIRE(read_sync(2,0,BS)==702);
    REQUIRE(GET(bwrites[0])+GET(bwrites[1])>0);
}
static void case_lf_slot_reuse(void) {
    local_mode=1; open_cache(3,0,0);
    for(int pass=0;pass<3;pass++) for(int b=0;b<8;b++) {
        write_sync(b,1000+10*pass+b);
#ifdef ML_HAS_LOCAL_FLUSH
        REQUIRE(block_cache_remote_drain(cache,5000)==0);
#else
        REQUIRE(0);
#endif
    }
    for(int b=0;b<8;b++) REQUIRE(read_sync(b,0,BS)==1020+b);
    close_cache(); open_cache(3,0,0); for(int b=0;b<8;b++) REQUIRE(read_sync(b,0,BS)==1020+b);
}

static void case_lf_helper(void) {
#ifdef ML_HAS_LOCAL_FLUSH
    lf_open(); write_sync(3,777);
    char path[4096], temp[4096], lock[4096];
    REQUIRE(snprintf(path,sizeof(path),"%s.stats",cc.cache_file)<(int)sizeof(path));
    REQUIRE(snprintf(temp,sizeof(temp),"%s.new",path)<(int)sizeof(temp));
    REQUIRE(snprintf(lock,sizeof(lock),"%s.drain.lock",path)<(int)sizeof(lock));
    pid_t child=-1; int status=0,done=0;
    const char *helper=getenv("S3BACKER_TEST_HELPER"); REQUIRE(helper);
    for(int i=0;i<WAIT_MS;i+=10) {
        struct block_cache_stats st; block_cache_get_stats(cache,&st);
        FILE *fp=fopen(temp,"w"); REQUIRE(fp);
        fprintf(fp,"block_cache_local_flush %u\nblock_cache_drain_pid %u\nblock_cache_drain_start_ticks %ju\nblock_cache_drain_requested %ju\nblock_cache_drain_completed %ju\nblock_cache_drain_errors %ju\nblock_cache_drain_error %d\nblock_cache_local_error %d\nblock_cache_read_hit_ratio 0.5\n",
          st.local_flush,st.drain_pid,(uintmax_t)st.drain_start_ticks,(uintmax_t)st.drain_requested,
          (uintmax_t)st.drain_completed,(uintmax_t)st.drain_errors,st.drain_error,st.local_error);
        REQUIRE(fclose(fp)==0 && rename(temp,path)==0);
        if(child<0) { child=fork(); REQUIRE(child>=0); if(!child) { execl("/usr/bin/python3","python3",helper,path,"--timeout","3",(char*)NULL); _exit(127); } }
        if(waitpid(child,&status,WNOHANG)==child) { done=1; break; }
        nap(10);
    }
    if(!done) { kill(child,SIGKILL); waitpid(child,&status,0); }
    REQUIRE(done && WIFEXITED(status) && WEXITSTATUS(status)==0); REQUIRE(GET(bgen[3])==777);
    unlink(path); unlink(temp); unlink(lock);
#else
    REQUIRE(0);
#endif
}


// This deliberately holds the writer AFTER the old wait counter, BEFORE libc's blocking call.
static void case_ml_writer_observation_gap(void) {
    open_x(8); warm(0);
    struct op *a=reader(0,0,BS,1,1), *w=writer(0,777,0,0), *c=reader(0,512,1024,1,0);
    start(a); REQUIRE(AWAIT(a->g.entered,WAIT_MS)); writer_hold=1;
    start(w); REQUIRE(AWAIT(writer_entered,WAIT_MS)); REQUIRE(GET(w->w.waits)>0);
    start(c); finish(c); REQUIRE(c->g.entered && got(c)==cached[0]);
    printf("observed old wait counter before native queue: later reader overtakes gated writer\n");
    pthread_mutex_lock(&g_mu); writer_release=1; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu);
    release(a); finish(a); finish(w); REQUIRE(w->r==0); free(a); free(w); free(c);
}
static void case_lf_remote_read_errors(void) {
    local_mode=1; open_cache(2,0,0); atomic_store(&remote_read_fail,1);
    for(int i=0;i<7;i++) { struct op *r=reader(3,0,BS,0,0); start(r); finish(r); REQUIRE(r->r==EIO && !has_block(3)); free(r); }
    REQUIRE(s3b_dcache_size(P->dcache)==0); atomic_store(&remote_read_fail,0);
    warm(3); close_cache(); open_cache(2,0,0); REQUIRE(state_of(3)==CLEAN2);
    atomic_store(&remote_read_fail,1); struct op *r=reader(3,0,BS,0,0); start(r); finish(r); REQUIRE(r->r==EIO); free(r);
    REQUIRE(s3b_dcache_size(P->dcache)==0); atomic_store(&remote_read_fail,0);
    warm(3); write_sync(4,777); REQUIRE(cache->flush_blocks(cache,NULL,0,0)==0); close_cache();
}
static void case_lf_shutdown_snapshot_error(void) {
    local_threads=1; lf_open(); write_sync(3,777); REQUIRE(cache->flush_blocks(cache,NULL,0,0)==0);
    wsnap=(struct gate){.kind=VIA_PREAD,.len=BS,.armed=1,.inject=EIO};
    struct op *s=op_new(OP_SHUTDOWN,0); start(s); finish(s); REQUIRE(s->r==EIO); REQUIRE(state_of(3)==DIRTY);
    int fd=open(cc.cache_file,O_RDONLY); uint32_t flags,marker; REQUIRE(fd>=0);
    REQUIRE(pread(fd,&flags,4,24)==4 && (flags&2)); REQUIRE(pread(fd,&marker,4,84)==4 && marker==1); close(fd);
    cache->destroy(cache); cache=NULL; free(s);
}
static void case_lf_shutdown_snapshot_short(void) {
    local_threads=1; lf_open(); write_sync(3,777); REQUIRE(cache->flush_blocks(cache,NULL,0,0)==0);
    int fd=open(cc.cache_file,O_RDWR); REQUIRE(fd>=0 && ftruncate(fd,4096)==0); close(fd);
    struct op *s=op_new(OP_SHUTDOWN,0); start(s); finish(s); REQUIRE(s->r!=0 && state_of(3)==DIRTY);
    fd=open(cc.cache_file,O_RDONLY); uint32_t flags,marker; REQUIRE(fd>=0);
    REQUIRE(pread(fd,&flags,4,24)==4 && (flags&2)); REQUIRE(pread(fd,&marker,4,84)==4 && marker==1); close(fd);
    cache->destroy(cache); cache=NULL; free(s);
}
static void case_lf_poison_during_sync(void) {
    lf_open(); write_sync(3,777);
    sync_hold=1; struct op *f=op_new(OP_INTERNAL_SYNC,0); start(f); REQUIRE(AWAIT(sync_entered,WAIT_MS));
    struct op *w=writer(3,778,1,0); w->g.inject=EIO; start(w); REQUIRE(AWAIT(w->g.entered,WAIT_MS));
    // The I/O error must publish without waiting for slow sync, and the pending sync must see it.
    REQUIRE(AWAIT(w->done,1000)); REQUIRE(s3b_dcache_error(P->dcache)==EIO);
    pthread_mutex_lock(&g_mu); sync_release=1; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu);
    finish(w); finish(f); REQUIRE(w->r==EIO && f->r==EIO); REQUIRE(cache->flush_blocks(cache,NULL,0,0)==EIO); _exit(0);
}
static void case_lf_poison_space_waiter(void) {
    local_mode=1; open_cache(2,0,0); wgate_open(0); write_sync(0,777);
    struct op *d=op_new(OP_DRAIN,0); start(d); REQUIRE(AWAIT(wgate_entered>=1,WAIT_MS));
    write_sync(1,778);
    struct op *r=reader(2,0,BS,0,0); r->watched=1; start(r); REQUIRE(AWAIT(r->w.last==&P->space_avail,WAIT_MS));
    struct op *w=writer(1,779,1,0); w->g.inject=EIO; start(w); finish(w); REQUIRE(w->r==EIO);
    finish(r); REQUIRE(r->r==EIO); REQUIRE(has_block(0) && has_block(1) && !has_block(2));
    REQUIRE(s3b_dcache_size(P->dcache)==2); wgate_release(); finish(d); REQUIRE(d->r==EIO); _exit(0);
}
static int fill_tear_foreign;
static int fill_tear_visit(void *arg,s3b_block_t slot,s3b_block_t block,const u_char *tag) {
    (void)arg; if(!tag && (slot!=18 || block!=25)) fill_tear_foreign++; return 0;
}
static void read_fill_tears(int verify) {
    local_mode=1; open_cache(32,0,0); capture_sync=1;
    for(int b=0;b<18;b++) warm(b); warm(25);
    if (verify) { capture_sync=0; close_cache(); open_cache(32,0,0); REQUIRE(state_of(25)==CLEAN2);
        if(verify==2) { pthread_mutex_lock(&g_mu); bgen[25]++; pthread_mutex_unlock(&g_mu); }
        capture_sync=1; REQUIRE(read_sync(25,0,BS)==GET(bgen[25])); }
    capture_sync=0;
    pthread_mutex_lock(&P->mutex); struct cache_entry *e=s3b_hash_get(P->hashtable,25); REQUIRE(e && e->u.dslot==18); pthread_mutex_unlock(&P->mutex);
    sync_hold=1; struct op *w=writer(25,999,0,0); start(w); REQUIRE(AWAIT(sync_entered,WAIT_MS));
    unsigned char actual[24]; int fd=open(cc.cache_file,O_RDONLY); REQUIRE(fd>=0 && pread(fd,actual,24,496)==24); close(fd);
    uint32_t block,flags; memcpy(&block,actual,4); memcpy(&flags,actual+20,4); REQUIRE(block==25 && flags==1);
    char path[4096]; REQUIRE(snprintf(path,sizeof(path),"%s.tear",cc.cache_file)<sizeof(path));
    struct block_cache_conf c=cc; c.cache_file=path;
    // Actual issued marker/record bytes, persisted after a 512-byte sector boundary, with each tail prefix.
    for(int n=0;n<=8;n++) {
        fd=open(path,O_CREAT|O_TRUNC|O_RDWR,0600); REQUIRE(fd>=0);
        REQUIRE(write(fd,durable_image,durable_len)==durable_len); REQUIRE(pwrite(fd,actual+16,n,512)==n); close(fd);
        struct s3b_dcache *d; fill_tear_foreign=0; REQUIRE(s3b_dcache_open(&d,&c,fill_tear_visit,NULL,1)==0);
        REQUIRE(fill_tear_foreign==0); s3b_dcache_close(d);
    }
    unlink(path); pthread_mutex_lock(&g_mu); sync_release=1; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu);
    finish(w); REQUIRE(w->r==0); free(w);
}

static void case_lf_read_fill_tears(void) { read_fill_tears(0); }
static void case_lf_read_verify_tears(void) { read_fill_tears(1); }
static void case_lf_read_refill_tears(void) { read_fill_tears(2); }
static void case_lf_alloc_terminal(void) {
    local_mode=1; open_cache(2,0,0); u_int slots[2];
    pthread_mutex_lock(&P->mutex); for(int i=0;i<2;i++) REQUIRE(s3b_dcache_alloc_block(P->dcache,&slots[i])==0); pthread_mutex_unlock(&P->mutex);
    struct op *r=reader(3,0,BS,0,0); start(r); finish(r); REQUIRE(r->r==ENOMEM);
    REQUIRE(s3b_dcache_size(P->dcache)==2 && !has_block(3)); REQUIRE(cache->flush_blocks(cache,NULL,0,0)==ENOMEM);
    REQUIRE(cache->shutdown(cache)==ENOMEM); cache->destroy(cache); cache=NULL; free(r);
}
#include "dirty_eviction_test.c"

int main(int argc, char **argv) {
    static const struct { const char *name; void (*fn)(void); } cases[] = {
        { "de-parallel", case_de_parallel },
        { "de-parallel-error", case_de_parallel_error },
        { "de-selected-read", case_de_selected_read }, { "de-selected-rewrite", case_de_selected_rewrite },
        { "de-ra-reading-one", case_de_ra_reading_one }, { "de-ra-reading-all", case_de_ra_reading_all },
        { "de-metrics", case_de_metrics }, { "de-metrics-off", case_de_metrics_off },
        { "de-metrics-rewrite", case_de_metrics_rewrite },
        { "de-ra-one", case_de_ra_one }, { "de-ra-all", case_de_ra_all },
        { "de-put-shutdown", case_de_put_shutdown },
        { "de-reordered-error", case_de_reordered_error },
        { "de-drain-selected", case_de_drain_selected },
        { "de-boundary", case_de_boundary }, { "de-restart-grace", case_de_restart_grace }, { "de-snapshot-error", case_de_snapshot_error },
        { "de-shared", case_de_shared }, { "de-rewrite", case_de_rewrite }, { "de-read-recency", case_de_read_recency },
        { "de-idle", case_de_idle }, { "de-young", case_de_young }, { "de-put-rewrite", case_de_put_rewrite },
        { "de-upload-error", case_de_upload_error }, { "de-busy-dirty", case_de_busy_dirty }, { "de-busy-clean", case_de_busy_clean },
        { "de-config", case_de_config }, { "de-crash", case_de_crash }, { "de-stress", case_de_stress },
        { "de-hog", case_de_hog }, { "de-default", case_de_default },
        { "lf-read-verify-tears", case_lf_read_verify_tears }, { "lf-read-refill-tears", case_lf_read_refill_tears },
        { "lf-alloc-terminal", case_lf_alloc_terminal },
        { "lf-read-fill-tears", case_lf_read_fill_tears }, { "lf-remote-read-errors", case_lf_remote_read_errors },
        { "lf-shutdown-snapshot-error", case_lf_shutdown_snapshot_error }, { "lf-shutdown-snapshot-short", case_lf_shutdown_snapshot_short },
        { "lf-poison-during-sync", case_lf_poison_during_sync }, { "lf-poison-space-waiter", case_lf_poison_space_waiter },
        { "ml-writer-observation-gap", case_ml_writer_observation_gap },
        { "lf-helper", case_lf_helper },
        { "lf-maxdirty", case_lf_maxdirty }, { "lf-slot-reuse", case_lf_slot_reuse },
        { "lf-read-miss-error", case_lf_read_miss_error }, { "lf-erase-error", case_lf_erase_error }, { "lf-clean-migration", case_lf_clean_migration },
        { "lf-erase-order", case_lf_erase_order }, { "lf-generation", case_lf_generation },
        { "lf-shutdown-pending", case_lf_shutdown_pending }, { "lf-stress", case_lf_stress },
        { "lf-marker-error", case_lf_marker_error }, { "lf-header-error", case_lf_header_error }, { "lf-clean2-write", case_lf_clean2_write },
        { "lf-sync-concurrent", case_lf_sync_concurrent }, { "lf-reopen-gates", case_lf_reopen_gates },
        { "lf-signal", case_lf_signal }, { "lf-drain-error", case_lf_drain_error }, { "lf-shutdown-error", case_lf_shutdown_error },
        { "lf-pressure", case_lf_pressure }, { "lf-metadata-error", case_lf_metadata_error }, { "lf-create-error", case_lf_create_error },
        { "lf-invalid", case_lf_invalid }, { "lf-crash", case_lf_crash }, { "lf-identity-tears", case_lf_identity_tears },
        { "lf-local", case_lf_local }, { "lf-default", case_lf_default }, { "lf-sync-error", case_lf_sync_error },
        { "lf-pwrite-error", case_lf_pwrite_error }, { "lf-drain", case_lf_drain }, { "lf-pending", case_lf_pending },
        { "partial-clean", case_partial_clean }, { "hit-overlap", case_hit_overlap }, { "same-entry", case_same_entry }, { "reader-vs-write", case_reader_vs_write },
        { "evict-pinned", case_evict_pinned }, { "timeout-pinned", case_timeout_pinned }, { "pread-error", case_pread_error },
        { "shutdown-active", case_shutdown_active }, { "worker-flush", case_worker_flush }, { "stress", case_stress },
        { "verify-relocation", case_verify_relocation },
        { "ml-dirty-read-same", case_ml_dirty_read_same }, { "ml-dirty-read-diff", case_ml_dirty_read_diff },
        { "ml-write-overlap-dirty", case_ml_write_overlap_dirty }, { "ml-write-overlap-clean", case_ml_write_overlap_clean },
        { "ml-same-block-excl", case_ml_same_block_excl }, { "ml-reader-vs-write", case_ml_reader_vs_write }, { "ml-writer-progress", case_ml_writer_progress },
        { "ml-snapshot", case_ml_snapshot }, { "ml-flush-pending-write", case_ml_flush_pending_write }, { "ml-shutdown-pending-write", case_ml_shutdown_pending_write },
        { "ml-upload-rewrite", case_ml_upload_rewrite }, { "ml-singleflight", case_ml_singleflight }, { "ml-fill-error", case_ml_fill_error },
        { "ml-readahead", case_ml_readahead }, { "ml-evict-gate", case_ml_evict_gate }, { "ml-full-dirty", case_ml_full_dirty },
        { "ml-clean2-shared", case_ml_clean2_shared }, { "ml-pwrite-error", case_ml_pwrite_error }, { "ml-snapshot-error", case_ml_snapshot_error },
        { "ml-restart", case_ml_restart }, { "ml-late-write-hit", case_ml_late_write_hit }, { "ml-late-write-miss", case_ml_late_write_miss },
        { "ml-read-owner", case_ml_read_owner }, { "ml-after-shutdown", case_ml_after_shutdown }, { "ml-dcache-extend", case_ml_dcache_extend }, { "ml-slot-reuse", case_ml_slot_reuse },
    };
    REQUIRE(argc == 2); tl_test = 1;
    fprintf(stderr, "TEST-START %s assertions=%s\n", argv[1],
#ifdef NDEBUG
      "off"
#else
      "on"
#endif
    ); // lets the runner tell pre-main startup failures from test crashes and verify assertion mode
    signal(SIGALRM, watchdog); alarm(60);
    for (int b = 0; b < NB; b++) bgen[b] = 10 + b;
    for (size_t i = 0; i < sizeof(cases) / sizeof(*cases); i++) {
        if (strcmp(argv[1], cases[i].name) != 0) continue;
        cases[i].fn();
        if (cache != NULL) close_cache();
        alarm(0); printf("PASS %s (%s cache)\n", argv[1], disk ? "disk" : "memory"); return 0;
    }
    REQUIRE(0);
}
