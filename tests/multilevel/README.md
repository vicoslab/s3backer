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
are the shutdown-admission regressions: `ml-late-write-*` fail on the earlier two-level commit a1d7663 (they pass on 444f902), `ml-read-owner` fails on both. TSAN startup crashes caused by address-space randomization are retried
only when the process printed nothing; a sanitizer report or test failure is never retried.
