# Age-eligible dirty eviction: native validation

Base: `b33f81cc16f8b0edad06f5a232ee647d18dcc812`. Native source harnesses only;
no SSH, VM, mount, deployment or S3 operation was performed.

The test-first `de-hog` failed on unchanged baseline source with **0/7**
second-pass hits (80% DIRTY, 20% CLEAN, 35% read set), in buffered and direct FILE
modes. The candidate gets **7/7**; option-off remains **0/7**, with zero dirty PUTs.
Original pre-implementation RED logs and a supplemental assertion-enabled
baseline RED are retained.

| Final matrix | Required cells | Result |
| --- | ---: | --- |
| Production assertions | 283 | 282 pass; one legacy assertion failure, isolated recheck passes |
| Optimized / NDEBUG | 283 | all pass |
| ASAN + UBSAN / assertions | 283 | all pass |
| Prior TSan / assertions | 273 | 96 pass, 177 startup nonpasses; not a clean race result |
| Correction TSan / assertions | 10 | 5 pass, 5 mapping startup nonpasses; not a clean race result |
| Public option parser | 10 | all pass |

Nine unmodified, superseded old-lock harness cells per matrix variant are
recorded separately and excluded from required passes, as in the existing runner.
All failed attempts and sanitizer startup logs remain retained; no retries hide
assertions. The final real-source harness selects the generated configuration,
rejects accidental NDEBUG in assertion builds, and prints its assertion mode.

Cases cover shared recency, successful rewrite grace, dirty reads preserving age,
64-bit clock boundaries, idle/local-FLUSH zero PUTs, young-dirty fallback, two
allocators during PUT/rewrite, retained upload errors, poisoned snapshot errors,
busy references, drain queue reordering/errors, shutdown during PUT/rewrite,
SIGKILL/LOCAL_ACK recovery with fresh grace, and aging during concurrent stress.
Two additional drain races were reproduced RED before their fixes. The existing
`dcache.c`/`dcache.h` and recovery format are unchanged.

Reproduce (output directory must be new):

```sh
python -I tests/multilevel/run_multilevel.py --out "$PWD/native-validation-new" \
  --deps "$DEPS_PREFIX" --variants cand-assert,cand-opt,cand-asan,cand-tsan \
  --suites dirty,local,ml,flush,order,direct,pins --case-timeout 25 --budget 1000 --jobs 6
```

The 283 required cells retain all 273 prior cases and add ten buffered/direct
correction cells. All ten new cells pass in each primary variant. The assertion
matrix retains a failure in the unmodified `read-pins/disk-rd0/worker-flush` case;
an isolated execution of the same binary passes. A controlled backend-return
delay exposes the same legacy test timing gap on both reviewed and corrected
source: its post-snapshot write can still be WRITING when the test expects CLEAN.
Twenty diagnostic runs without that delay pass on each source. Those diagnostics
and the isolated recheck do not replace the original failing matrix result.

The one-worker and all-three-workers read-ahead cases failed
on the reviewed source when no selected-victim PUT started within five seconds.
The corrected speculative allocator abandons a fill instead of waiting for an
upload from its own pool, including across allocation retries and the pressure
fallback. Foreground allocation still waits. The all-worker test gates existing
read-ahead hash lookups so every worker reaches a speculative miss before any
can return to upload scheduling.

The stats mirror exports four counters through the existing stats printer:

| Counter (prefix `block_cache_`) | Meaning |
| --- | --- |
| `dirty_eviction_selected` | Pressure selected an age-eligible DIRTY generation. |
| `dirty_eviction_uploaded` | That selected generation completed its PUT and became CLEAN. This does not promise its subsequent eviction. |
| `dirty_eviction_rewrite_canceled` | A successful rewrite invalidated a selection, counted once per selection. An older successful PUT cannot count the newer generation as reclaimed. |
| `dirty_eviction_clean` | Pressure selected an idle CLEAN or CLEAN2 entry for eviction while the policy was enabled, including an erase attempt that subsequently fails. Timeout evictions are excluded. |

All four counters are zero with the policy disabled and reset through the
existing stats-clear operation. Selections and completions can span a reset.
Real-C assertions cover successful and failed selected uploads, rewrite during
PUT, stats reset, CLEAN eviction, and option-off behavior.

Exact source/test hashes, commands, raw logs and plugin identities belong in the
external validation handoff supplied with a build. Public parser checks use the
real configuration parser in parse-only mode. Native checks do not establish
deployment or VM durability.
