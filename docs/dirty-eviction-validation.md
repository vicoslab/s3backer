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
| Production assertions | 273 | all pass |
| Optimized / NDEBUG | 273 | all pass |
| ASAN + UBSAN / assertions | 273 | all pass |
| TSan / assertions | 273 | startup unsupported; not a clean race result |
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
python -I tests/multilevel/run_multilevel.py --out /NODE_LOCAL/new-native-run \
  --deps /PREFIX/with/include-and-lib --variants cand-assert,cand-opt,cand-asan,cand-tsan \
  --suites dirty,local,ml,flush,order,direct,pins --case-timeout 25 --budget 1000 --jobs 6
```

Exact source/test hashes, compiler flags, commands, outcomes, patch stack,
host-plugin hash and raw evidence paths are in the node-local
`/var/tmp/nfs-execution/s3backer-dirty-eviction-grace-20261103/implementation-handoff.json`
and its linked manifests. Public parser checks use the real configuration parser
in parse-only mode. Native checks do not establish deployment or VM durability.
