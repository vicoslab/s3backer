/* Direct-read tests against the production dcache.o / block_cache.o (linked, pread wrapped).
 * Usage: direct-test <case>; the cache file comes from $S3BACKER_TEST_CACHE and is recreated.
 * Build with -DDIRECT_RED against the read-pins candidate without direct-read.patch: the
 * direct entry points are weak and absent there, so every case reports FAIL (RED), not a
 * link error. Payload bytes encode (slot, generation, offset), so stale, foreign-slot or
 * shifted bytes are detected. */
#include "config.h"
#include "s3backer.h"
#include "block_cache.h"
#include "dcache.h"
#include <dirent.h>
#include <limits.h>
#include <stdatomic.h>

#ifdef DIRECT_RED
struct s3b_dcache_direct_stats { u_int enabled; uint64_t aligned, bounced, buffered; };
#define SET_READ_DIRECT(c, v) ((void)(c), (void)(v), 0)
#else
#define SET_READ_DIRECT(c, v) ((c)->read_direct = (v), 1)
#endif
extern int s3b_dcache_read_block_direct(struct s3b_dcache *, u_int, void *, u_int, u_int) __attribute__((weak));
extern void s3b_dcache_get_direct_stats(struct s3b_dcache *, struct s3b_dcache_direct_stats *) __attribute__((weak));

#define REQUIRE(c) do { if (!(c)) { fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); exit(1); } } while (0)
#define BS (64u * 1024u)
#define NSLOTS 8
#define A 4096u

static struct block_cache_conf conf;
static struct s3b_dcache *dc;
static u_int slots[NSLOTS];                     // slots[i] = dslot holding logical slot i (sorted)
static int gens[NSLOTS];
static atomic_int log_errs, fallback_notices;

// pread injection (only on O_DIRECT descriptors) and observation
static atomic_int inject_errno, inject_count, direct_preads, buffered_preads;
extern ssize_t __real_pread(int, void *, size_t, off_t);
ssize_t __wrap_pread(int fd, void *buf, size_t n, off_t off) {
    const int fl = fcntl(fd, F_GETFL);
    if (fl != -1 && (fl & O_DIRECT) != 0) {
        atomic_fetch_add(&direct_preads, 1);
        if (atomic_load(&inject_count) > 0 && atomic_fetch_sub(&inject_count, 1) > 0) {
            errno = atomic_load(&inject_errno);
            return -1;
        }
    } else
        atomic_fetch_add(&buffered_preads, 1);
    return __real_pread(fd, buf, n, off);
}

// open() wrapper: optionally fail the next O_DIRECT open with EPERM
static atomic_int fail_direct_open, swap_direct_open, bad_direct_flags;
static const char *alternate_path;
extern int __real_open(const char *, int, ...);
int __wrap_open(const char *path, int flags, ...) {
    mode_t mode = 0;
    if (flags & O_CREAT) { va_list ap; va_start(ap, flags); mode = (mode_t)va_arg(ap, int); va_end(ap); }
    if ((flags & O_DIRECT) != 0 && atomic_exchange(&fail_direct_open, 0)) { errno = EPERM; return -1; }
    if ((flags & O_DIRECT) && atomic_exchange(&swap_direct_open, 0)) path = alternate_path;
    if (flags & O_DIRECT) {
        int bad = atomic_exchange(&bad_direct_flags, 0);
        if (bad == 1) flags &= ~O_DIRECT;
        if (bad == 2) flags &= ~O_CLOEXEC;
        if (bad == 3) flags = (flags & ~O_ACCMODE) | O_RDWR;
    }
    return __real_open(path, flags, mode);
}

static void logger(int level, const char *fmt, ...) { if (level <= LOG_ERR) atomic_fetch_add(&log_errs, 1);
    if (level == LOG_NOTICE && strstr(fmt, "direct reads disabled")) atomic_fetch_add(&fallback_notices, 1); }
static int visit(void *arg, s3b_block_t dslot, s3b_block_t block_num, const u_char *etag) { (void)arg; (void)dslot; (void)block_num; (void)etag; return 0; }

static uint64_t mix(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL; x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL; return x ^ (x >> 31);
}
static u_char pat(int slot, int gen, u_int i) { return (u_char)(mix((uint64_t)slot * 1000003u + (uint64_t)gen * 7919u * 131071u + i / 8) >> ((i % 8) * 8)); }
static void fill(u_char *buf, int slot, int gen, u_int off, u_int len) { for (u_int i = 0; i < len; i++) buf[i] = pat(slot, gen, off + i); }
static int check(const u_char *buf, int slot, int gen, u_int off, u_int len) {
    for (u_int i = 0; i < len; i++) if (buf[i] != pat(slot, gen, off + i)) return 0;
    return 1;
}
static int count_fds(void) {
    int n = 0; DIR *d = opendir("/proc/self/fd"); struct dirent *e; REQUIRE(d != NULL);
    while ((e = readdir(d)) != NULL) if (e->d_name[0] != '.') n++;
    closedir(d); return n - 1;                  // minus the DIR's own fd
}
static struct s3b_dcache_direct_stats stats(void) {
    struct s3b_dcache_direct_stats s; memset(&s, 0, sizeof(s));
    REQUIRE(s3b_dcache_get_direct_stats != NULL);
    s3b_dcache_get_direct_stats(dc, &s); return s;
}
static int cmp_u(const void *a, const void *b) { u_int x = *(const u_int *)a, y = *(const u_int *)b; return x < y ? -1 : x > y; }

static void write_slot(int i, int gen, u_int off, u_int len) {
    u_char *buf = malloc(BS); REQUIRE(buf);
    fill(buf, i, gen, off, len);
    REQUIRE(s3b_dcache_write_block(dc, slots[i], buf, off, len) == 0);
    free(buf);
}
// Open a fresh cache file and write generation 1 into every slot (buffered, no fsync)
static void open_dc(int read_direct) {
    const char *path = getenv("S3BACKER_TEST_CACHE");
    REQUIRE(path != NULL);
    unlink(path);
    memset(&conf, 0, sizeof(conf));
    conf.block_size = BS; conf.cache_size = NSLOTS; conf.cache_file = path; conf.log = logger;
    if (read_direct)
        REQUIRE(SET_READ_DIRECT(&conf, 1));
    REQUIRE(s3b_dcache_open(&dc, &conf, visit, NULL, 0) == 0);
    for (int i = 0; i < NSLOTS; i++) REQUIRE(s3b_dcache_alloc_block(dc, &slots[i]) == 0);
    qsort(slots, NSLOTS, sizeof(*slots), cmp_u);
    for (int i = 0; i < NSLOTS; i++) { gens[i] = 1; write_slot(i, 1, 0, BS); }
    atomic_store(&log_errs, 0);
}
static void close_dc(void) { s3b_dcache_close(dc); dc = NULL; }
static void require_direct(void) { REQUIRE(s3b_dcache_read_block_direct != NULL); }

// Read [off, off+len) of slot i through the direct path into dest+mis with canaries around it
static int dread(int i, u_int off, u_int len, u_int mis, int *okp) {
    const u_int pad = 64;
    u_char *raw; REQUIRE(posix_memalign((void **)&raw, A, BS + 2 * A + pad) == 0);
    u_char *dest = raw + A + mis;
    memset(raw, 0xa5, BS + 2 * A + pad);
    const int r = s3b_dcache_read_block_direct(dc, slots[i], dest, off, len);
    int ok = r == 0 ? check(dest, i, gens[i], off, len) : 1;
    for (u_char *p = raw; p < dest; p++) ok &= *p == 0xa5;                          // nothing before
    for (u_char *p = dest + len; p < raw + BS + 2 * A + pad; p++) ok &= *p == 0xa5;   // nothing after
    free(raw);
    *okp = ok;
    return r;
}
static void ok_read(int i, u_int off, u_int len, u_int mis) {
    int ok; const int r = dread(i, off, len, mis, &ok);
    if (r != 0 || !ok) fprintf(stderr, "read slot=%d off=%u len=%u mis=%u: r=%d ok=%d\n", i, off, len, mis, r, ok);
    REQUIRE(r == 0); REQUIRE(ok);
}

// Cases
static void c_aligned(void) {
    require_direct();
    const int before = count_fds(); open_dc(1);
    REQUIRE(count_fds() == before + 2);         // buffered fd + separate O_DIRECT fd
    REQUIRE(stats().enabled == 1);
    const int dp = atomic_load(&direct_preads);
    ok_read(0, 0, BS, 0); ok_read(3, A, 2 * A, 0); ok_read(NSLOTS - 1, BS - A, A, 0);
    struct s3b_dcache_direct_stats s = stats();
    REQUIRE(s.aligned == 3 && s.bounced == 0 && s.buffered == 0);
    REQUIRE(atomic_load(&direct_preads) - dp >= 3);
    close_dc(); REQUIRE(count_fds() == before);
}
static void c_unaligned_dest(void) {
    require_direct(); open_dc(1);
    ok_read(1, 0, BS, 1); ok_read(2, A, A, 8); ok_read(4, 0, 2 * A, 2048);
    struct s3b_dcache_direct_stats s = stats(); REQUIRE(s.aligned == 0 && s.bounced == 3 && s.buffered == 0);
    close_dc();
}
static void c_unaligned_off(void) {
    require_direct(); open_dc(1);
    ok_read(1, 1000, 8192, 0); ok_read(2, A - 1, 2, 0); ok_read(5, 3 * A + 17, A, 0); ok_read(6, 1, BS - 1, 0);
    struct s3b_dcache_direct_stats s = stats(); REQUIRE(s.aligned == 0 && s.bounced == 4 && s.buffered == 0);
    close_dc();
}
static void c_unaligned_len(void) {
    require_direct(); open_dc(1);
    ok_read(1, 0, 5000, 0); ok_read(2, A, A + 1, 0); ok_read(3, 0, BS - 1, 0); ok_read(4, 2 * A, 7, 0);
    struct s3b_dcache_direct_stats s = stats(); REQUIRE(s.aligned == 0 && s.bounced == 4 && s.buffered == 0);
    close_dc();
}
static void c_tiny(void) {
    require_direct(); open_dc(1);
    for (int i = 0; i < NSLOTS; i += NSLOTS - 1) {
        ok_read(i, 0, 1, 0); ok_read(i, BS - 1, 1, 0); ok_read(i, A, 1, 3); ok_read(i, A - 1, 1, 0); ok_read(i, 12345, 1, 1);
    }
    struct s3b_dcache_direct_stats s = stats(); REQUIRE(s.bounced == 10 && s.buffered == 0);
    close_dc();
}
static void c_full_block(void) {
    require_direct(); open_dc(1);
    for (int i = 0; i < NSLOTS; i++) ok_read(i, 0, BS, (u_int)(i % 2) * 513);
    struct s3b_dcache_direct_stats s = stats(); REQUIRE(s.aligned == NSLOTS / 2 && s.bounced == NSLOTS / 2 && s.buffered == 0);
    close_dc();
}
static void c_slot_boundary(void) {
    require_direct(); open_dc(1);
    // Neighbours carry different generations: any cross-slot byte would fail the check
    gens[3] = 7; write_slot(3, 7, 0, BS); gens[5] = 9; write_slot(5, 9, 0, BS);
    gens[NSLOTS - 1] = 11; write_slot(NSLOTS - 1, 11, 0, BS);
    for (int i = 3; i <= 5; i++) {
        ok_read(i, BS - 1, 1, 0); ok_read(i, BS - 100, 100, 0); ok_read(i, BS - A - 5, A + 5, 1); ok_read(i, 0, 1, 0); ok_read(i, 0, 100, 2);
    }
    ok_read(NSLOTS - 1, BS - 1, 1, 0); ok_read(NSLOTS - 1, BS - 3000, 3000, 5); ok_read(NSLOTS - 1, BS - A, A, 0);
    REQUIRE(stats().buffered == 0);
    close_dc();
}
static void c_coherence(void) {
    require_direct(); open_dc(1);
    ok_read(2, 0, BS, 0);                       // populate nothing in page cache via dfd; dfd bypasses it
    // Buffered overwrite (dirty page cache, no fsync) then direct read must see it
    gens[2] = 2; write_slot(2, 2, 0, BS);
    ok_read(2, 0, BS, 0); ok_read(2, 100, 5000, 3);
    // Partial buffered overwrite: mixed generations within the slot
    u_char *buf = malloc(BS), *exp = malloc(BS); REQUIRE(buf && exp);
    fill(buf, 2, 3, 100, 5000); REQUIRE(s3b_dcache_write_block(dc, slots[2], buf, 100, 5000) == 0);
    fill(exp, 2, 2, 0, BS); fill(exp + 100, 2, 3, 100, 5000);
    u_char *d; REQUIRE(posix_memalign((void **)&d, A, BS) == 0);
    REQUIRE(s3b_dcache_read_block_direct(dc, slots[2], d, 0, BS) == 0); REQUIRE(memcmp(d, exp, BS) == 0);
    REQUIRE(s3b_dcache_read_block_direct(dc, slots[2], d + 1, 50, 6000) == 0); REQUIRE(memcmp(d + 1, exp + 50, 6000) == 0);
    struct s3b_dcache_direct_stats s = stats(); REQUIRE(s.buffered == 0 && s.aligned >= 3);
    free(buf); free(exp); free(d);
    close_dc();
}
static void c_tail(void) {
    require_direct(); open_dc(1);
    const char *path = getenv("S3BACKER_TEST_CACHE"); struct stat sb;
    REQUIRE(stat(path, &sb) == 0);
    const off_t last = sb.st_size - BS;         // file is a whole number of slots; last slot is the tail
    REQUIRE(last % A == 0);
    const int L = NSLOTS - 1;
    REQUIRE(truncate(path, last + A + 100) == 0);   // file ends inside the last slot, unaligned
    ok_read(L, 0, A + 100, 0); ok_read(L, A, 100, 0); ok_read(L, A + 50, 50, 1); ok_read(L, 0, 1, 0); ok_read(L, 0, A, 0);
    // Uncovered: same error as the buffered path
    u_char *d; REQUIRE(posix_memalign((void **)&d, A, BS) == 0);
    const int e0 = atomic_load(&log_errs);
    REQUIRE(s3b_dcache_read_block(dc, slots[L], d, 0, A + 101) == EINVAL);
    REQUIRE(s3b_dcache_read_block_direct(dc, slots[L], d, 0, A + 101) == EINVAL);
    REQUIRE(s3b_dcache_read_block_direct(dc, slots[L], d, 2 * A, 10) == EINVAL);
    REQUIRE(s3b_dcache_read_block_direct(dc, slots[L], d, 0, BS) == EINVAL);
    REQUIRE(atomic_load(&log_errs) - e0 >= 4);
    REQUIRE(truncate(path, last + 2 * A) == 0);     // aligned end: second direct pread returns 0
    REQUIRE(s3b_dcache_read_block_direct(dc, slots[L], d, 0, 3 * A) == EINVAL);
    ok_read(L, 0, A + 100, 0);                  // bytes past the old end are now zero-filled
    free(d);
    close_dc();
}
static void c_einval_fallback(void) {
    require_direct(); open_dc(1);
    atomic_store(&inject_errno, EINVAL); atomic_store(&inject_count, 1);
    const int bp = atomic_load(&buffered_preads);
    ok_read(3, 0, BS, 0);
    REQUIRE(atomic_load(&inject_count) == 0 && atomic_load(&buffered_preads) > bp);
    ok_read(4, 77, 3000, 1);
    struct s3b_dcache_direct_stats s = stats(); REQUIRE(s.buffered == 2 && s.aligned == 0 && s.bounced == 0);
    REQUIRE(atomic_load(&log_errs) == 0);
    ok_read(4, 77, 3000, 1); REQUIRE(stats().bounced == 0 && stats().buffered == 3);
    close_dc();
}
static void c_eintr(void) {
    open_dc(1); atomic_store(&inject_errno, EINTR); atomic_store(&inject_count, 1);
    ok_read(0, 77, 3000, 1); REQUIRE(stats().enabled && stats().bounced == 1 && stats().buffered == 0);
    REQUIRE(!atomic_load(&log_errs)); close_dc();
}
static void c_eio_error(void) {
    require_direct(); open_dc(1);
    atomic_store(&inject_errno, EIO); atomic_store(&inject_count, 1);
    int ok; REQUIRE(dread(3, 0, BS, 0, &ok) == EIO);
    atomic_store(&inject_count, 1); REQUIRE(dread(3, 10, 20, 1, &ok) == EIO);
    REQUIRE(atomic_load(&log_errs) == 2);
    struct s3b_dcache_direct_stats s = stats(); REQUIRE(s.buffered == 0 && s.aligned == 0 && s.bounced == 0);
    ok_read(3, 10, 20, 1);
    close_dc();
}
#define NTHREADS 16
#define NCLEAN 5
static atomic_int stop_writer, conc_fail;
static void *conc_reader(void *arg) {
    unsigned seed = (unsigned)(uintptr_t)arg * 2654435761u + 1;
    u_char *raw; REQUIRE(posix_memalign((void **)&raw, A, BS + 64) == 0);
    for (int it = 0; it < 3000 && !atomic_load(&conc_fail); it++) {
        const int i = rand_r(&seed) % NCLEAN;
        const u_int mis = (rand_r(&seed) % 4 == 0) ? 0 : (u_int)(rand_r(&seed) % 64);
        u_int off = (u_int)rand_r(&seed) % BS, len = 1 + (u_int)rand_r(&seed) % (BS - off);
        if (rand_r(&seed) % 4 == 0) { off &= ~(A - 1); len = A * (1 + (u_int)rand_r(&seed) % ((BS - off) / A)); }
        u_char *d = raw + mis;
        if (s3b_dcache_read_block_direct(dc, slots[i], d, off, len) != 0 || !check(d, i, 1, off, len)) { atomic_store(&conc_fail, 1); break; }
    }
    free(raw);
    return NULL;
}
static void *conc_writer(void *arg) {
    (void)arg; int gen = 2;
    u_char *buf = malloc(BS); REQUIRE(buf);
    while (!atomic_load(&stop_writer)) {
        for (int i = NCLEAN; i < NSLOTS; i++) { fill(buf, i, gen, 0, BS); REQUIRE(s3b_dcache_write_block(dc, slots[i], buf, 0, BS) == 0); }
        gen++;
    }
    free(buf);
    return NULL;
}
static void c_concurrency(void) {
    require_direct(); open_dc(1);
    pthread_t r[NTHREADS], w;
    REQUIRE(pthread_create(&w, NULL, conc_writer, NULL) == 0);
    for (int t = 0; t < NTHREADS; t++) REQUIRE(pthread_create(&r[t], NULL, conc_reader, (void *)(uintptr_t)(t + 1)) == 0);
    for (int t = 0; t < NTHREADS; t++) REQUIRE(pthread_join(r[t], NULL) == 0);
    atomic_store(&stop_writer, 1); REQUIRE(pthread_join(w, NULL) == 0);
    REQUIRE(!atomic_load(&conc_fail));
    struct s3b_dcache_direct_stats s = stats();
    printf("observed concurrency: aligned=%ju bounced=%ju buffered=%ju\n", (uintmax_t)s.aligned, (uintmax_t)s.bounced, (uintmax_t)s.buffered);
    REQUIRE(s.buffered == 0 && s.aligned > 0 && s.bounced > 0 && s.aligned + s.bounced == (uint64_t)NTHREADS * 3000);
    REQUIRE(atomic_load(&log_errs) == 0);
    close_dc();
}
static void c_disabled(void) {
    require_direct();
    const int before = count_fds(); open_dc(0);
    REQUIRE(count_fds() == before + 1);         // no O_DIRECT descriptor
    const int dp = atomic_load(&direct_preads);
    ok_read(0, 0, BS, 0); ok_read(1, 5, 100, 1); ok_read(2, A, A, 0);
    struct s3b_dcache_direct_stats s = stats();
    REQUIRE(s.enabled == 0 && s.aligned == 0 && s.bounced == 0 && s.buffered == 0);
    REQUIRE(atomic_load(&direct_preads) == dp);
    close_dc();
}
static int bc_simple(struct s3backer_store *s) { (void)s; return 0; }
static void bc_destroy(struct s3backer_store *s) { (void)s; }
static void c_config(void) {
    require_direct();
    struct s3backer_store inner = { .create_threads = bc_simple, .shutdown = bc_simple, .destroy = bc_destroy };
    struct block_cache_conf c; memset(&c, 0, sizeof(c));
    c.block_size = BS; c.cache_size = NSLOTS; c.num_threads = 1; c.log = logger;
    REQUIRE(SET_READ_DIRECT(&c, 1));
    errno = 0;
    REQUIRE(block_cache_create(&c, &inner) == NULL && errno == EINVAL);   // no cache file: rejected
    // With a cache file the dcache opens the separate descriptor; open fails closed if O_DIRECT can't be opened
    const int before = count_fds(); open_dc(1); REQUIRE(count_fds() == before + 2); close_dc();
    // Injected O_DIRECT open failure on an existing valid cache file: open fails closed, leaking nothing
    atomic_store(&fail_direct_open, 1); errno = 0;
    REQUIRE(s3b_dcache_open(&dc, &conf, visit, NULL, 0) == EPERM);
    REQUIRE(atomic_load(&fail_direct_open) == 0 && count_fds() == before && atomic_load(&log_errs) >= 1);
}

static void c_identity(void) {
    open_dc(1); close_dc();
    const char *path = getenv("S3BACKER_TEST_CACHE");
    char alt[4096]; snprintf(alt, sizeof(alt), "%s.foreign", path); unlink(alt);
    int fd = __real_open(alt, O_CREAT|O_RDWR|O_EXCL, 0600); REQUIRE(fd >= 0);
    REQUIRE(ftruncate(fd, 1024*1024) == 0); close(fd);
    alternate_path = alt; atomic_store(&swap_direct_open, 1);
    const int before = count_fds();
    REQUIRE(s3b_dcache_open(&dc, &conf, visit, NULL, 0) == EINVAL);
    REQUIRE(count_fds() == before); unlink(alt);
}
static void c_symlink(void) {
    open_dc(1); close_dc();
    const char *path = getenv("S3BACKER_TEST_CACHE"); char alt[4096];
    snprintf(alt, sizeof(alt), "%s.symlink", path); unlink(alt); REQUIRE(symlink(path, alt) == 0);
    alternate_path = alt; atomic_store(&swap_direct_open, 1);
    const int before = count_fds();
    REQUIRE(s3b_dcache_open(&dc, &conf, visit, NULL, 0) == ELOOP);
    REQUIRE(count_fds() == before); unlink(alt);
}
static void c_hardlink(void) {
    open_dc(1); close_dc();
    const char *path = getenv("S3BACKER_TEST_CACHE"); char alt[4096];
    snprintf(alt, sizeof(alt), "%s.hardlink", path); unlink(alt); REQUIRE(link(path, alt) == 0);
    alternate_path = alt; atomic_store(&swap_direct_open, 1);
    REQUIRE(s3b_dcache_open(&dc, &conf, visit, NULL, 0) == 0);
    REQUIRE(stats().enabled == 1); close_dc(); unlink(alt);
}
static void c_range(void) {
    open_dc(1); unsigned char x[4096];
    REQUIRE(s3b_dcache_read_block_direct(dc, slots[0], x, BS-1, UINT_MAX) == EINVAL);
    REQUIRE(s3b_dcache_read_block_direct(dc, UINT_MAX, x, 0, 1) == EINVAL);
    REQUIRE(s3b_dcache_read_block_direct(dc, slots[0], x, UINT_MAX, 1) == EINVAL);
    REQUIRE(s3b_dcache_read_block_direct(dc, slots[0], x, BS, 0) == 0);
    close_dc();
}
static void c_capability(void) {
    open_dc(1); atomic_store(&inject_errno, EINVAL); atomic_store(&inject_count, 1);
    ok_read(3, 0, BS, 0); const int dp = atomic_load(&direct_preads);
    REQUIRE(stats().enabled == 0 && atomic_load(&fallback_notices) == 1);
    ok_read(3, 77, 3000, 1);
    REQUIRE(atomic_load(&direct_preads) == dp && stats().buffered == 2 && atomic_load(&fallback_notices) == 1);
    close_dc();
}
static void c_bound(void) {
    open_dc(1); close_dc(); unlink(conf.cache_file); conf.block_size = 32u*1024u*1024u;
    REQUIRE(s3b_dcache_open(&dc, &conf, visit, NULL, 0) == EINVAL);
}
// All flag checks inspect the descriptor returned by open(), not the requested flags.
static void c_fdmode(void) {
    open_dc(1); close_dc(); const int before = count_fds();
    for (int bad = 1; bad <= 3; bad++) {
        atomic_store(&bad_direct_flags, bad);
        REQUIRE(s3b_dcache_open(&dc, &conf, visit, NULL, 0) == EINVAL);
        REQUIRE(count_fds() == before);
    }
}
extern void s3b_dcache_get_direct_memory_stats(size_t *, size_t *, size_t *) __attribute__((weak));
static pthread_mutex_t mem_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t mem_cv = PTHREAD_COND_INITIALIZER;
static int mem_ready, mem_release;
static void *memory_reader(void *arg) {
    const size_t n = (size_t)(uintptr_t)arg;
    u_char *buf = calloc(1, n + 1); REQUIRE(buf);
    REQUIRE(s3b_dcache_read_block_direct(dc, slots[0], buf + 1, 0, n) == 0);
    pthread_mutex_lock(&mem_mu); mem_ready++; pthread_cond_broadcast(&mem_cv);
    while (!mem_release) pthread_cond_wait(&mem_cv, &mem_mu);
    pthread_mutex_unlock(&mem_mu); free(buf); return NULL;
}
static void memory_wave(int workers, size_t n) {
    pthread_t ts[48]; size_t bytes, buffers, peak;
    REQUIRE(s3b_dcache_get_direct_memory_stats != NULL);
    s3b_dcache_get_direct_memory_stats(&bytes, &buffers, &peak); REQUIRE(bytes == 0 && buffers == 0);
    mem_ready = mem_release = 0;
    for (int i = 0; i < workers; i++) REQUIRE(pthread_create(&ts[i], NULL, memory_reader, (void *)(uintptr_t)n) == 0);
    pthread_mutex_lock(&mem_mu);
    while (mem_ready != workers) pthread_cond_wait(&mem_cv, &mem_mu);
    s3b_dcache_get_direct_memory_stats(&bytes, &buffers, &peak);
    REQUIRE(bytes == workers * n && buffers == workers && peak >= bytes);
    printf("observed TLS: workers=%d bytes=%zu peak=%zu\n", workers, bytes, peak);
    mem_release = 1; pthread_cond_broadcast(&mem_cv); pthread_mutex_unlock(&mem_mu);
    for (int i = 0; i < workers; i++) REQUIRE(pthread_join(ts[i], NULL) == 0);
    s3b_dcache_get_direct_memory_stats(&bytes, &buffers, &peak); REQUIRE(bytes == 0 && buffers == 0);
}
static void c_memory(void) {
    open_dc(1); memory_wave(48, BS); close_dc();
    unlink(conf.cache_file); conf.block_size = 16u*1024u*1024u; conf.cache_size = 1;
    REQUIRE(s3b_dcache_open(&dc, &conf, visit, NULL, 0) == 0);
    REQUIRE(s3b_dcache_alloc_block(dc, &slots[0]) == 0);
    void *zero = calloc(1, conf.block_size); REQUIRE(zero);
    REQUIRE(s3b_dcache_write_block(dc, slots[0], zero, 0, conf.block_size) == 0); free(zero);
    memory_wave(1, conf.block_size); close_dc();
}
static const struct { const char *name; void (*fn)(void); } cases[] = {
    { "eintr", c_eintr }, { "fdmode", c_fdmode }, { "memory", c_memory }, { "identity", c_identity }, { "symlink", c_symlink }, { "hardlink", c_hardlink },
    { "range", c_range }, { "capability", c_capability }, { "bound", c_bound },
    { "aligned", c_aligned }, { "unaligned-dest", c_unaligned_dest }, { "unaligned-off", c_unaligned_off },
    { "unaligned-len", c_unaligned_len }, { "tiny", c_tiny }, { "full-block", c_full_block },
    { "slot-boundary", c_slot_boundary }, { "coherence", c_coherence }, { "tail", c_tail },
    { "einval-fallback", c_einval_fallback }, { "eio-error", c_eio_error }, { "concurrency", c_concurrency },
    { "disabled", c_disabled }, { "config", c_config },
};
int main(int argc, char **argv) {
    REQUIRE(argc == 2);
    alarm(120);
    for (size_t i = 0; i < sizeof(cases) / sizeof(*cases); i++) {
        if (strcmp(argv[1], cases[i].name) == 0) { cases[i].fn(); printf("PASS %s\n", argv[1]); return 0; }
    }
    fprintf(stderr, "FAIL unknown case %s\n", argv[1]);
    return 2;
}
