# tests/multilevel

Native C regressions for two-level block-cache locking. They compile the real `block_cache.c`/`dcache.c` with fake lower layers
(no network, VM, mounts or S3).

* `multilevel_test.c`: derived from `baseline/read_pins_test.c` (adds worker-snapshot/pwrite/rwlock-wait observation, gated and
  failing backend reads, and the `ml-*` cases; old-global-lock-specific assertions are under `MULTILEVEL_CANDIDATE`, marked `ML-ADAPT`).
* `baseline/`: unmodified copies of the earlier harnesses (read-pins, direct, FLUSH, flush-order). `BASELINE_SHA256SUMS` lists them.
* `config.h`: autoconf output used for the build (NDEBUG removed for assertion builds).
* `run_multilevel.py`: builds `base-*`/`cand-*` variants (assert, `-O2 NDEBUG`, ASAN+UBSAN, TSAN), runs memory / disk / disk+`read_direct`
  modes with per-case `timeout`, records `receipt.json` and per-case logs, and (for `cand-opt`) links the host and optional guest plugin.

```
/usr/bin/python3 -I tests/multilevel/run_multilevel.py --out /some/new/dir \
  --deps /prefix/with/include+lib/x86_64-linux-gnu --variants base-assert,cand-assert,cand-opt,cand-asan,cand-tsan \
  [--guest-fuse /path/libfuse3.so.4] [--cc /usr/bin/gcc] [--base-commit REV] [--budget SECONDS]
```

Environment fallbacks: `S3B_DEPS`, `S3B_GUEST_FUSE`, `S3B_CC`. `--out` must not exist. Baseline variants build `git archive <base-commit>`; candidate variants
build the working tree. Expected: baseline fails the ten RED `ml-*` cases (`ml-dirty-read-*`, `ml-write-overlap-*`, `ml-same-block-excl`,
`ml-reader-vs-write`, `ml-snapshot`, `ml-flush-pending-write`, `ml-shutdown-pending-write`, `ml-evict-gate`) on commit 444f902; the candidate passes everything except the
three superseded unmodified baseline cases per mode (reported under `pins-superseded/`, expected to fail). `ml-late-write-hit`, `ml-late-write-miss` and `ml-read-owner`
are the shutdown-admission regressions: `ml-late-write-*` fail on the earlier two-level commit a1d7663 (they pass on 444f902), `ml-read-owner` fails on both. TSan startup retries are bounded and recorded separately from successful test executions. The multilevel harness emits a flushed `TEST-START` marker; legacy harnesses without that marker leave some crash-at-startup classifications unverified. Preserve those attempts and any exhausted cells as non-passes, not evidence of flawless sanitizer operation. Reported races and actual test failures are not retried. See [the validation record](../../docs/multilevel-validation.md) for the observed counts and limitations.

The `local` suite extends the real C harness for opt-in persistent local FLUSH.
It link-wraps fdatasync/fsync/pwrite for errors and barrier observation, exercises
SIGUSR1 and request/completion stats, pressure, sticky failures, SIGKILL/reopen
with known dirty rewrite/partial patterns, invalid identity byte-prefix images,
and marker-before-erase ordering. Absent features fail assertions (never skip).
Run with `--variants cand-assert,cand-asan,cand-tsan,cand-opt --suites local`.
TSan receives exactly one attempt; environment startup failures remain failures.

The `dirty` suite includes `dirty_eviction_test.c` in the same real-source harness.
It injects a deterministic CLOCK_MONOTONIC clock only for the grace policy (the
existing realtime worker/barrier clock is unchanged), and runs buffered/direct
FILE variants. `de-hog` fills 80% DIRTY / 20% CLEAN, reads a 35% working set, and
requires 7/7 second-pass hits. On b33f81c it fails behaviorally at 0/7;
`de-default` requires the original 0/7 with grace disabled. Other cases cover
shared recency, rewrite grace, read recency without grace reset, exact boundaries
beyond 32-bit time, idle zero PUTs, young-dirty pressure progress, two allocators
waiting during a victim PUT and rewrite, upload/snapshot errors, busy references,
invalid configurations, SIGKILL/LOCAL_ACK recovery with a fresh grace interval,
drain/shutdown and concurrent stress. `de-ra-one` gates a real worker prefetch;
`de-ra-all` gates the outer speculative hash lookup until all three workers have
claimed misses. Both require PUT, foreground allocation, drain and shutdown
progress with an old DIRTY victim and newer CLEAN entries. Workers may not wait
on `space_avail`. `de-metrics*` assert policy counters, reset, upload failure,
rewrite invalidation and default-off behavior. Gated read-ahead/drain tests cover a later
drain retaining its upload request and a barrier reordering a selected victim
behind a different failed block. Run with `--suites dirty,local,ml,flush,order,direct,pins`.

The harness uses `<config.h>` so the runner's generated header wins over the
checked-in header. Assertion variants define `ML_EXPECT_ASSERTIONS` and fail to
compile if NDEBUG is active; `TEST-START` also prints `assertions=on/off`. This
keeps the included production cache invariant checks active in assertion and
sanitizer builds. Each test runs under its build directory, compiler scratch uses
`--out/tmp`, and receipts record the per-case command, working directory and environment.
