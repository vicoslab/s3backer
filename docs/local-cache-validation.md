# Local persistent FLUSH validation

Validated native source: `fc4e813a7f0e366dbd704be74cac10e6c7005577`, following implementation commit `6a548ea6825a3a84c9c871ec1b717085b28017f3` on `persistent-cache-flush`. No upstream push was performed. See [operation and compatibility](local-cache-flush.md) and [compact machine evidence](local-cache-validation.json).

Implementation and both independent reviews used actual Codex CLI sessions explicitly selecting `gpt-6.1-sol` with `high` effort. Rollout records verified the selected model, effort and shell tool execution; no Astra fallback was used.

## Native acceptance

The final real-C harness includes actual `block_cache.c` and links actual `dcache.c`; it does not substitute a Python storage model. Every final source/test file matches the committed Git blobs and compiled-source/input hashes in the receipts.

| Final variant | Passed | Failed/skipped |
|---|---:|---:|
| Assertion | 211 | 0 / 0 |
| ASAN + UBSAN | 211 | 0 / 0 |
| Optimized | 211 | 0 / 0 |

Each variant includes 39 local-persistence, 107 multilevel-locking, 30 FLUSH, 12 ordering and 23 direct-read cases. Independent exact-delta review accepted the source and passed 54 additional focused executions. The full suite was executed before committing; byte hashes establish exact equality with the resulting commit. No code/test edits followed that execution.

Final TSan: **87 passes and 124 non-passes**, one attempt each: 114 explicit memory-mapping failures, seven crashes without TEST-START, and three unverified crashes from harnesses without that marker. This is **not** an all-pass or race-freedom result. Earlier receipts remain distinct; no reported race/assertion failure was retried into a pass.

The first independent review rejected three defects, all reproduced using compiled native code: an unsynced read-fill CLEAN identity could become a foreign DIRTY mapping, remote read errors could consume all slots, and a shutdown snapshot error could put a worker to sleep indefinitely. The correction persists every CLEAN producer before DIRTY publication, returns only provably safe slots, propagates poison/allocation errors and wakes existing waiters, and revisits shutdown after snapshot failure. Atomic first-error publication and a post-sync latch check cover concurrent payload errors. Regressions include initial fill and both CLEAN2 verification/refill producers, actual sector-prefix crash images, repeated backend errors, poison while space waiters exist, and one-worker pread/truncation shutdown failures.

A predecessor optimized fairness test failed (198/199). Its observation was before libc queued the writer. A deterministic gated test demonstrated that gap; the corrected test requires native try-read-lock EBUSY before starting the later reader. Original no-overtake/generation assertions and production locking remain unchanged. The original failed logs and source identities are retained.

Reproduce the final matrix using the existing runner with node-local output:

```
/usr/bin/python3 -I tests/multilevel/run_multilevel.py \
  --deps /path/to/dependencies --guest-fuse /path/to/guest/libfuse3.so.4 \
  --variants cand-assert,cand-asan,cand-opt --suites local,ml,flush,order,direct \
  --case-timeout 20 --budget 300 --out /var/tmp/local-flush-native
```

Guest plugin SHA-256: `60db8f12194364bca9b15c413a4ca651fd5a10ed417f18c956638e956c193910` (libfuse3.so.4). Host plugin: `da6d2e6513dc066922d43c244ce1c305d5b40c1175313ecee0567c14e570ba3e` (libfuse3.so.3). Prior baseline/manual plugins were preserved.

## Actual isolated VM checks

A fresh identity-bound 8 GiB cache-filesystem LV supported a 10 GiB standalone export, 320 slots × 16 MiB = 5 GiB payload cache, four request/cache threads, and a real 5000 ms write delay. Existing manual endpoints were authenticated and preserved before and after each stage. No reboot, queue tuning, global cache drops, forced detach, repair or backend deletion occurred.

- Actual raw NBD FLUSH/FUA replies matched handles and returned errno zero. Two DIRTY blocks remained with zero data PUTs through a six-second hold.
- A controlled userspace daemon kill occurred with no kernel NBD client attached. Restart recovered the known partial-object write and sector rewrite, still with zero data PUTs.
- Explicit SIGUSR1 drain completed request 1 with error zero, uploaded two blocks, and reached zero DIRTY. A fresh readonly daemon with the block cache disabled fetched both expected blocks from S3 with zero PUTs.
- A separate four-slot namespace accepted five distinct patterns under pressure, uploaded blocks, and verified every pattern.
- Mounted XFS completed a 64 MiB known write, fsync, 1 MiB rewrite, fsync, full expected readback, and actual raw NBD FLUSH. Eight slots remained DIRTY, with zero data PUTs.
- A separate locally acknowledged DIRTY write was uploaded by normal shutdown without explicit drain. Direct S3 body comparison matched the expected bytes, normal exit succeeded, and header flags became 1 (LOCAL_ACK cleared).

Startup creates one mount-token metadata PUT. Zero-PUT claims above mean **zero additional PUTs after that captured startup baseline**, also requiring zero native normal/zero/empty-block writes and unchanged prefix counts. Counters were never reset. The initial impossible absolute-zero guard failed before payload; its receipt was preserved and an authenticated unexecuted tail completed. A cache-disabled reader's omitted block-cache statistics also required an authenticated existing-process continuation; the successful backend-read proof is separate from that retained adapter failure.

Final readback: no owned live storage/workload processes, NBD3 detached, scratch top filesystem unmounted, manual endpoint generations/configuration/cache identities unchanged, and no new relevant XFS/NBD kernel errors. The lower LV/mount, caches and owned S3 namespaces remain retained; they were not deleted.

## Tiny performance-only smoke

One 128 MiB write, one worker, 50 MiB split files, two consecutive tar/pv reads, listing and owned deletion per arm. The original dd/pv/split and tar/pv pipeline helpers were reused unchanged. Hashing was excluded from timed performance and correctness was checked separately. Geometry/plugin/threads/delay matched; ACK policy differed. ON retained prior qualification history; OFF was fresh with a matching unmeasured 64 MiB allocation fixture. These warm, short, single-repetition observations do **not** establish a general speedup.

| Operation | OFF | ON |
|---|---:|---:|
| Buffered write ACK | 308.62 MiB/s (0.415 s) | 308.73 MiB/s (0.415 s) |
| Separate `sync -f` | 3.572 s; 10 PUTs | 0.365 s; 0 PUTs, DIRTY retained |
| Read 1, payload-equivalent | 2002.59 MiB/s (0.064 s) | 1122.56 MiB/s (0.114 s) |
| Read 2, payload-equivalent | 2000.99 MiB/s (0.064 s) | 2001.49 MiB/s (0.064 s) |
| List | 0.0037 s | 0.0159 s |
| Owned delete | 0.0077 s | 0.0641 s |

## Limits and evidence

No physical power-loss or VM hard-reset experiment was performed. Process-kill/reopen and modeled sector-prefix images do not prove filesystem/controller/Ceph power-loss behavior. Local durability depends on truthful sync semantics; an unflushed in-place write may tear. Remote-failure and local I/O-failure injection are primarily native tests, not a VM fault campaign.

Raw build, review, command, failed-attempt and VM receipts are node-local under `/var/tmp/nfs-execution/s3backer-persistent-local-flush-20261103/`; 83 VM JSON receipts are mirrored in `vm-receipts/`. Compact evidence records receipt hashes, exact source/artifact identities and full-precision timings. The retained scratch root is `/srv/s3backer-localflush-4f60d8141eae`; this is evidence, not a new manual endpoint or permission to remove storage.
