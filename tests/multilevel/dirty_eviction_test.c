/* Real-source age eviction regressions; included after the multilevel harness helpers. */
static void de_tick(uint64_t ms) { atomic_fetch_add(&de_clock_offset, ms); }
static void de_open(u_int size, u_int grace) {
    atomic_store(&de_clock_active, 1); local_mode = 1; de_grace = grace; open_cache(size, 0, 0); REQUIRE(disk);
}
static void de_hog(int enabled) {
    de_open(20, enabled ? 120000 : 0);
    for (int b = 0; b < 16; b++) { de_tick(1); write_sync(b, 500 + b); }
    REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == 0);
    de_tick(120000);
    for (int b = 16; b < 20; b++) { de_tick(1); warm(b); }
    for (int b = 16; b < 23; b++) { de_tick(1); REQUIRE(read_sync(b, 0, BS) == 10 + b); }
    int before = total_breads();
    for (int b = 16; b < 23; b++) { de_tick(1); REQUIRE(read_sync(b, 0, BS) == 10 + b); }
    int hits = 7 - (total_breads() - before);
    printf("dirty-hog: enabled=%d dirty=16/20 readset=7/20 second-pass-hits=%d/7\n", enabled, hits); fflush(stdout);
    REQUIRE(hits == (enabled ? 7 : 0));
    if (!enabled) for (int b = 0; b < 16; b++) REQUIRE(GET(bwrites[b]) == 0);
}
static void case_de_hog(void) { de_hog(1); }
static void case_de_default(void) { de_hog(0); }

static void de_fill_four(void) {
    de_tick(1); write_sync(0, 500);
    for (int b = 1; b < 4; b++) { de_tick(1); warm(b); }
    de_tick(120000);
}
static int de_total_puts(void) {
    int n = 0; for (int b = 0; b < NB; b++) n += GET(bwrites[b]); return n;
}
static void case_de_shared(void) {
    de_open(4, 120000);
    de_tick(1); warm(0); de_tick(1); write_sync(1, 501);
    de_tick(1); warm(2); de_tick(1); write_sync(3, 503); de_tick(120000);
    REQUIRE(read_sync(4, 0, BS) == 14); REQUIRE(!has_block(0)); REQUIRE(de_total_puts() == 0);
    de_tick(1); REQUIRE(read_sync(5, 0, BS) == 15);
    REQUIRE(!has_block(1) && has_block(2) && has_block(3)); REQUIRE(GET(bwrites[1]) == 1);
}
static void case_de_rewrite(void) {
    de_open(4, 120000); de_fill_four();
    write_sync(0, 501); // Successful rewrite restarts grace even though its first write is old.
    for (int b = 4; b < 8; b++) { de_tick(1); REQUIRE(read_sync(b, 0, BS) == 10 + b); }
    REQUIRE(state_of(0) == DIRTY && GET(bwrites[0]) == 0);
    de_tick(120000); REQUIRE(read_sync(8, 0, BS) == 18);
    REQUIRE(!has_block(0) && GET(bwrites[0]) == 1 && GET(bgen[0]) == 501);
}
static void case_de_read_recency(void) {
    de_open(4, 120000); write_sync(0, 500); de_tick(1); write_sync(1, 501);
    de_tick(1); warm(2); de_tick(1); warm(3); de_tick(120000);
    REQUIRE(read_sync(0, 0, BS) == 500); // Refresh recency, preserving original dirty age.
    REQUIRE(read_sync(4, 0, BS) == 14);
    REQUIRE(has_block(0) && !has_block(1)); REQUIRE(GET(bwrites[0]) == 0 && GET(bwrites[1]) == 1);
    for (int b = 2; b < 5; b++) REQUIRE(read_sync(b, 0, BS) == 10 + b);
    REQUIRE(read_sync(5, 0, BS) == 15); // All CLEANs are newer; the just-read DIRTY is still age eligible.
    REQUIRE(!has_block(0) && GET(bwrites[0]) == 1);
}
static void case_de_idle(void) {
    de_open(8, 120000); write_sync(0, 500); de_tick(UINT64_C(1) << 34);
    unsigned before = atomic_load(&worker_waits);
    pthread_mutex_lock(&P->mutex); pthread_cond_broadcast(&P->worker_work); pthread_mutex_unlock(&P->mutex);
    for (int i = 0; i < 1000 && atomic_load(&worker_waits) == before; i++) nap(1);
    REQUIRE(atomic_load(&worker_waits) > before);
    REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == 0);
    REQUIRE(de_total_puts() == 0 && state_of(0) == DIRTY);
    REQUIRE(block_cache_remote_drain(cache, 5000) == 0);
    REQUIRE(GET(bgen[0]) == 500 && state_of(0) == CLEAN);
    write_sync(1, 501); close_cache(); REQUIRE(GET(bgen[1]) == 501);
}
static void case_de_young(void) {
    de_open(4, 120000);
    for (int b = 0; b < 12; b++) write_sync(b, 500 + b);
    REQUIRE(de_total_puts() > 0); // Existing pressure fallback must progress without advancing the grace clock.
    for (int b = 0; b < 12; b++) REQUIRE(read_sync(b, 0, BS) == 500 + b);
}
static void case_de_put_rewrite(void) {
    de_open(4, 120000); de_fill_four(); wgate_open(0);
    struct op *a = reader(4, 0, BS, 0, 0), *b = reader(5, 0, BS, 0, 0);
    a->watched = b->watched = 1; start(a); REQUIRE(AWAIT(wgate_entered == 1, WAIT_MS));
    start(b); REQUIRE(AWAIT(b->w.waits >= 1, WAIT_MS));
    REQUIRE(state_of(0) == WRITING); // Both allocators wait; neither owns a naked victim pointer.
    write_sync(0, 501); REQUIRE(state_of(0) == WRITING2);
    REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == 0);
    REQUIRE(read_sync(0, 0, BS) == 501); wgate_release(); finish(a); finish(b);
    REQUIRE(got(a) == 14 && got(b) == 15); free(a); free(b);
    REQUIRE(state_of(0) == DIRTY && GET(bwrites[0]) == 1 && GET(bgen[0]) == 500);
    REQUIRE(read_sync(0, 0, BS) == 501); // Successful old PUT did not clean/reuse the newer generation.
    REQUIRE(block_cache_remote_drain(cache, 5000) == 0); REQUIRE(GET(bgen[0]) == 501);
}
static void case_de_upload_error(void) {
    de_open(4, 120000); de_fill_four(); atomic_store(&backend_fail, 1);
    REQUIRE(read_sync(4, 0, BS) == 14); // Failed victim retained; an available CLEAN provides fallback.
    REQUIRE(state_of(0) == DIRTY && GET(bwrites[0]) == 0 && read_sync(0, 0, BS) == 500);
    REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == 0);
    REQUIRE(block_cache_remote_drain(cache, 5000) == EIO);
    atomic_store(&backend_fail, 0);
    // The existing drain reports a remembered error while also scheduling its retry.
    int retry = block_cache_remote_drain(cache, 5000); REQUIRE(retry == 0 || retry == EIO);
    wait_state(0, CLEAN); REQUIRE(block_cache_remote_drain(cache, 5000) == 0);
    REQUIRE(GET(bgen[0]) == 500);
}
static void case_de_busy_dirty(void) {
    de_open(4, 120000); de_fill_four();
    struct op *pin = reader(0, 0, BS, 1, 1); start(pin); REQUIRE(AWAIT(pin->g.entered, WAIT_MS));
    REQUIRE(!pin->g.held); REQUIRE(read_sync(4, 0, BS) == 14);
    REQUIRE(GET(bwrites[0]) == 0 && state_of(0) == DIRTY);
    release(pin); finish(pin); REQUIRE(got(pin) == 500); free(pin);
}
static void case_de_busy_clean(void) {
    de_open(4, 120000); de_fill_four();
    struct op *pins[4];
    for (int b = 0; b < 4; b++) { pins[b] = reader(b, 0, BS, 1, 1); start(pins[b]); REQUIRE(AWAIT(pins[b]->g.entered, WAIT_MS)); }
    struct op *a = reader(4, 0, BS, 0, 0), *b = reader(5, 0, BS, 0, 0);
    a->watched = b->watched = 1; start(a); start(b);
    REQUIRE(AWAIT(a->w.waits && b->w.waits, WAIT_MS));
    REQUIRE(!GET(a->done) && !GET(b->done));
    unsigned before = atomic_load(&worker_waits); nap(80);
    REQUIRE(atomic_load(&worker_waits) - before < 20); // Busy entries cannot cause a worker retry spin.
    for (int i = 0; i < 4; i++) { release(pins[i]); finish(pins[i]); free(pins[i]); }
    finish(a); finish(b); REQUIRE(got(a) == 14 && got(b) == 15); free(a); free(b);
}
static void case_de_config(void) {
#ifdef ML_HAS_DIRTY_EVICTION
    struct block_cache_conf c = { .block_size=BS, .cache_size=4, .num_threads=2, .dirty_eviction_grace=120000,
        .recover_dirty_blocks=1, .cache_file=getenv("S3BACKER_TEST_CACHE"), .log=logger };
    REQUIRE(block_cache_create(&c, &inner) == NULL && errno == EINVAL);
    c.local_flush=1; c.cache_file=NULL; REQUIRE(block_cache_create(&c, &inner) == NULL && errno == EINVAL);
    c.cache_file=getenv("S3BACKER_TEST_CACHE"); c.num_protected=1;
    REQUIRE(block_cache_create(&c, &inner) == NULL && errno == EINVAL);
    c.num_protected=0; c.cache_size=0; REQUIRE(block_cache_create(&c, &inner) == NULL && errno == EINVAL);
#else
    REQUIRE(0 && "dirty eviction option absent");
#endif
}
static void case_de_crash(void) { de_grace=120000; case_lf_crash(); }
static atomic_int de_aging_stop;
static void *de_age_main(void *arg) {
    (void)arg;
    while (!atomic_load(&de_aging_stop)) { de_tick(120000); nap(1); }
    return NULL;
}
static void case_de_stress(void) {
    pthread_t aging; atomic_store(&de_clock_active, 1); de_grace=120000;
    REQUIRE(pthread_create(&aging, NULL, de_age_main, NULL) == 0);
    case_lf_stress(); atomic_store(&de_aging_stop, 1); REQUIRE(pthread_join(aging, NULL) == 0);
}

static void case_de_boundary(void) {
    de_tick(UINT64_C(1) << 34); de_open(4, 120000); write_sync(0, 500);
    for (int b = 1; b < 4; b++) warm(b);
    de_tick(119999); REQUIRE(read_sync(4, 0, BS) == 14);
    REQUIRE(state_of(0) == DIRTY && GET(bwrites[0]) == 0);
    de_tick(1); REQUIRE(read_sync(5, 0, BS) == 15);
    REQUIRE(!has_block(0) && GET(bwrites[0]) == 1);
}
static void case_de_restart_grace(void) {
    int fds[2], status; REQUIRE(pipe(fds) == 0); pid_t child = fork(); REQUIRE(child >= 0);
    if (!child) {
        close(fds[0]); de_open(4, 120000); de_fill_four();
        REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == 0);
        REQUIRE(write(fds[1], "!", 1) == 1); for (;;) pause();
    }
    close(fds[1]); char ack; REQUIRE(read(fds[0], &ack, 1) == 1); close(fds[0]);
    REQUIRE(kill(child, SIGKILL) == 0 && waitpid(child, &status, 0) == child && WIFSIGNALED(status));
    de_tick(UINT64_C(1) << 34); de_open(4, 120000);
    REQUIRE(state_of(0) == DIRTY); REQUIRE(read_sync(4, 0, BS) == 14);
    REQUIRE(state_of(0) == DIRTY && GET(bwrites[0]) == 0); // RAM-only age starts anew at recovery.
    de_tick(120000); REQUIRE(read_sync(5, 0, BS) == 15);
    REQUIRE(!has_block(0) && GET(bgen[0]) == 500);
}
static void case_de_snapshot_error(void) {
    de_open(4, 120000); de_fill_four(); arm_wsnap(0, EIO);
    struct op *r = reader(4, 0, BS, 0, 0); start(r); finish(r); REQUIRE(r->r == EIO); free(r);
    REQUIRE(GET(wsnap.entered) && !GET(wsnap.held));
    REQUIRE(state_of(0) == DIRTY && GET(bwrites[0]) == 0 && s3b_dcache_error(P->dcache) == EIO);
    REQUIRE(cache->flush_blocks(cache, NULL, 0, 0) == EIO);
    REQUIRE(block_cache_remote_drain(cache, 5000) == EIO);
    REQUIRE(cache->shutdown(cache) == EIO); cache->destroy(cache); cache = NULL;
    int fd = open(cc.cache_file, O_RDONLY); uint32_t flags, marker;
    REQUIRE(fd >= 0 && pread(fd, &flags, 4, 24) == 4 && (flags & 2));
    REQUIRE(pread(fd, &marker, 4, 84) == 4 && marker == 1); close(fd);
}

// A read-ahead worker is outside the mutex when allocation requests a DIRTY victim.
// A later read changes its recency, but an explicit drain must still upload it.
static void case_de_drain_selected(void) {
    atomic_store(&de_clock_active, 1); local_mode=1; de_grace=120000; local_threads=1;
    rgate_block=3; open_cache2(4, 0, 0, 0, 1); REQUIRE(disk);
    write_sync(0, 500); warm(1); warm(2); REQUIRE(AWAIT(rgate_entered == 1, WAIT_MS));
    de_tick(120000);
    struct op *miss=reader(4, 0, BS, 0, 0); miss->watched=1; start(miss);
    REQUIRE(AWAIT(miss->w.last == &P->space_avail, WAIT_MS));
    struct op *pin=reader(0, 0, BS, 1, 1); start(pin); REQUIRE(AWAIT(pin->g.entered, WAIT_MS));
    struct op *drain=op_new(OP_DRAIN, 0); drain->watched=1; start(drain);
    REQUIRE(AWAIT(drain->w.last == &P->write_complete, WAIT_MS));
    rgate_permit_to(1); finish(drain);
    printf("selected-drain: drain_r=%d victim_puts=%d\n", drain->r, GET(bwrites[0])); fflush(stdout);
    REQUIRE(drain->r == 0 && GET(bwrites[0]) == 1);
    finish(miss); REQUIRE(got(miss) == 14);
    release(pin); finish(pin); REQUIRE(got(pin) == 500);
    free(drain); free(miss); free(pin);
}

static void *de_ordered_drain_main(void *arg) {
    struct op *o=arg; s3b_block_t blocks[2]={1, 0}; tl_test=1; tl_watch=&o->w;
    int r=block_cache_remote_flush(cache, blocks, 2, 5000);
    pthread_mutex_lock(&g_mu); o->r=r; o->done=1; pthread_cond_broadcast(&g_cv); pthread_mutex_unlock(&g_mu);
    return NULL;
}
// A barrier can reorder the dirty queue while its age-selected victim awaits a worker.
// A failed different block at the head must not strand the allocation request behind it.
static void case_de_reordered_error(void) {
    atomic_store(&de_clock_active, 1); local_mode=1; de_grace=120000; local_threads=1;
    rgate_block=3; open_cache2(4, 0, 0, 0, 1); REQUIRE(disk);
    write_sync(0, 500); write_sync(1, 501); REQUIRE(read_sync(1, 0, BS) == 501); warm(2);
    REQUIRE(AWAIT(rgate_entered == 1, WAIT_MS)); de_tick(120000);
    struct op *miss=reader(4, 0, BS, 0, 0); miss->watched=1; start(miss);
    REQUIRE(AWAIT(miss->w.last == &P->space_avail, WAIT_MS));
    atomic_store(&backend_fail_block, 1);
    struct op *drain=op_new(OP_DRAIN, 0);
    REQUIRE(pthread_create(&drain->t, NULL, de_ordered_drain_main, drain) == 0);
    REQUIRE(AWAIT(drain->w.last == &P->write_complete, WAIT_MS));
    rgate_permit_to(1); finish(drain); REQUIRE(drain->r == EIO);
    finish(miss); REQUIRE(got(miss) == 14 && GET(bwrites[0]) == 1 && state_of(1) == DIRTY);
    REQUIRE(read_sync(1, 0, BS) == 501);
    atomic_store(&backend_fail_block, -1);
    int retry=block_cache_remote_drain(cache, 5000); REQUIRE(retry == 0 || retry == EIO);
    wait_state(1, CLEAN); REQUIRE(block_cache_remote_drain(cache, 5000) == 0);
    free(miss); free(drain);
}

static void case_de_put_shutdown(void) {
    de_open(4, 120000); de_fill_four(); wgate_open(0);
    struct op *miss=reader(4, 0, BS, 0, 0); start(miss); REQUIRE(AWAIT(wgate_entered == 1, WAIT_MS));
    write_sync(0, 501);
    struct op *shutdown=op_new(OP_SHUTDOWN, 0); start(shutdown); wait_shutdown_begun();
    wgate_release(); finish(miss); finish(shutdown);
    REQUIRE(got(miss) == 14 && shutdown->r == 0 && GET(bgen[0]) == 501);
    REQUIRE(P->num_dirties == 0 && P->num_refs == 0);
    cache->destroy(cache); cache=NULL; free(miss); free(shutdown);
}
