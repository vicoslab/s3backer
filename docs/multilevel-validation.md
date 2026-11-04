# Multilevel locking: validation record

Native implementation: `a1d7663`, corrected shutdown admission/drain: `89fbc9b`.
Official base: `76bab1493018e3388bf4da7ec714adb02ddd1d74`; validated baseline: `444f902bcb36fc0da22d31b8bd030da72f2258e2`.

## Architecture and native checks

Stable per-entry standard pthread reader/writer locks protect payloads, with references acquired under a short global housekeeping mutex before blocking outside it. Upload snapshots release the shared lock before network I/O. One `draining`/`active_ops` gate covers complete public reads/writes; it closes admission before shutdown waits, while workers remain alive. Entry references and whole-request admission have distinct lifetimes. See [locking.md](locking.md) and [the native tests/build instructions](../tests/multilevel/README.md).

The final committed C/test/runner bytes produced 191 successful executed cases per variant (assertions, optimized, ASan/UBSan, TSan), with nine separate expected-failure controls for superseded legacy lock-behavior assertions per variant. Independent review rejected the initial shutdown behavior, reproduced the late-write/read-owner cases, and accepted the focused correction. Historical failures remain preserved.

TSan successful executions are not a flawless-sanitizer claim: 20 markerless crash attempts had unproven startup classification. Mapping failures, verified pre-main marker failures, real failures and successful executions are recorded separately. No reported race or real test failure was retried into acceptance.

## Mounted qualification and protocol

Both arms passed separate deterministic 8 × 20 MiB write/read/rewrite/fsync and normal-restart expected-digest checks. Performance used one fresh 20 GiB corpus per arm: 16 workers × 1280 MiB, original `dd | pv | split -b 50M` writes and `tar cf - ... | pv > /dev/null` reads; timed `ls -lA`; five consecutive reads without inserted drain, followed by an explicit separately labelled FLUSH and two settled reads. Payload content hashing was outside the performance protocol. Each corpus had 416 size/count-checked files. This is the direct-XFS/kernel-NBD path, not NFS-client throughput.

Equal geometry: 64 GiB image; 24 GiB native cache (1536 × 16 MiB); 28 GiB lower allocation; 48 request/48 upload threads; maxDirty=0; write delay=4294967232 ms; engine read-ahead 16/2; kernel read-ahead 16384 KiB; shared fast-device WBT=0. Lower O_DIRECT applies to CLEAN hits; DIRTY/WRITING reads remain buffered in both arms.

| Phase | Baseline MiB/s (seconds) | Candidate MiB/s (seconds) | Baseline dirty before → after | Candidate dirty before → after |
|---|---:|---:|---:|---:|
| write | 553.84 (36.978) | 816.00 (25.098) | 0.07% → 67.32% | 0.07% → 77.99% |
| read1 | 461.32 (44.394) | 441.81 (46.355) | 70.38% → 48.57% | 81.45% → 12.57% |
| read2 | 484.72 (42.251) | 1250.90 (16.372) | 48.57% → 4.10% | 12.57% → 0.07% |
| read3 | 1781.89 (11.493) | 1934.98 (10.584) | 4.10% → 0.33% | 0.07% → 0.13% |
| read4 | 1971.15 (10.390) | 1917.26 (10.682) | 0.33% → 0.33% | 0.13% → 0.20% |
| read5 | 1971.77 (10.387) | 1924.05 (10.644) | 0.33% → 0.33% | 0.20% → 0.20% |
| clean1 | 1900.08 (10.779) | 1923.78 (10.646) | 0.00% → 0.00% | 0.00% → 0.00% |
| clean2 | 1962.42 (10.436) | 1905.42 (10.748) | 0.00% → 0.00% | 0.00% → 0.07% |

Timed listing: 0.114729 → 0.006618 seconds.

Write ACK throughput increased 47.33%; the five-read transition-cohort aggregate increased 25.65% (total bytes / summed seconds). **The first DIRTY-heavy read did not improve**: 461.32 → 441.81 MiB/s. Candidate Read2 began far cleaner than baseline Read2, so its isolated gain is not a pure matched-DIRTY locking speedup. The first strictly CLEAN settled pass was close (1900.08 → 1923.78 MiB/s); candidate clean2 ended with one dirty slot and is not strictly CLEAN. One fixed-order pair cannot establish small differences.

During Read1, zero payload GETs were recorded in either arm, and approximately 99.6%/94.7% of payload-equivalent bytes traversed NBD. The candidate completed 1096 normal-block PUTs in that interval versus 374 for the baseline. This records concurrent destaging context, not a causal decomposition of read latency.

Both arms closed normally, their NBD attachment and mounts were released, and the owned scratch lower allocation was subsequently removed after evidence preservation. The two existing operator endpoints remained unchanged. Scratch S3 prefixes remain retained; no production rollout or GitHub publication was performed.

## Remaining limits

- One fixed-order trial per arm; transitioning dirty occupancy differs. No blanket read-speedup claim.
- File-extending payload writes still take the leaf file-size mutex; initial cache-growth serialization is deferred, not hidden.
- Writer preference is glibc-specific, with no FIFO guarantee among writers.
- Tiny expected-pattern/restart integrity qualification is distinct from the performance corpus, which was not content-hashed.
- No abrupt-power-loss durability claim.
- A first-statistics-publication race and delayed free-space accounting caused preserved adapter holds; only their unfinished tails resumed, never formatting or completed qualification.

The [machine-readable record](multilevel-validation.json) retains every pass, dirty fractions, native/backend counters, NBD/device bytes, source/build/review digests and limitations. Full raw receipts are retained by the operator and identified by hashes; public availability is not asserted.
