# Block cache locking (two levels)

Applies to `block_cache.c` / `dcache.c` on branch `multilevel-cache-locking` (baseline `444f902`). Source/unit level only:
nothing here is mounted acceptance, a speedup claim or a runtime validation of the storage stack.

Profile this targets (protected production, not the historical 64 MiB profile): 16 MiB blocks, 6400 slots, 100 GiB,
48 request / 48 cache threads, `maxWriteDelay=4294967232` ms, `maxDirty=0`, `lowerDirect=true`, read-ahead 16/2.
Small: 256 slots, 4 GiB, 16/16. Scratch: 16 MiB, 1536 slots, 24 GiB, 48/48.

## Locks

* **Global mutex** `priv->mutex`: metadata only (hash table, LRU and dirty lists, `state`/`timeout`/links, reference counts,
  statistics, flush waiters, read-ahead, survey, the dcache free list, entry allocation/free). Held for O(1) work.
* **Entry lock** `entry->lock` (`pthread_rwlock_t`, created with the entry, destroyed when it is freed; entries never move,
  the ETag is a fixed array). Protects the payload (buffer or dslot data) and the dslot's directory record.
  Linux/glibc: `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP`.

Rules: a thread takes a reference (`readers` = shared, `excl_waiters` = exclusive, `num_refs` = total) under the global mutex,
**drops the mutex, then blocks on the entry lock**; it releases the entry lock, retakes the mutex and drops the reference.
The global mutex is never held while blocking on an entry lock, so `entry lock -> global mutex` nesting is safe.
A referenced entry is not evicted or freed. READING[2], DIRTY and WRITING[2] entries are never evicted.

## What no longer runs under the global mutex

Hit copies (CLEAN, DIRTY, WRITING, WRITING2), write payload and cache-file writes, dirty/clean directory records and their
`fdatasync`, miss fill (backend read, copy to caller, dslot write, record), ETag-verify erase, upload snapshot copy, disk
eviction/timeout erase. Remaining under it: `malloc/calloc/free` of entries and buffers (memory mode frees a block buffer on
eviction under the mutex), dcache free-list changes, survey callbacks, and debug-only (`!NDEBUG`) directory asserts in
`s3b_dcache_alloc_block/free_block`.

## Guarantees (covered by tests/multilevel)

* Reads of the same or different entries in any of CLEAN/DIRTY/WRITING/WRITING2 overlap; a write excludes only same-block
  readers/writers. A reader sees one whole generation (no torn read) because writers hold the entry lock exclusively
  across the payload write.
* Different-block writes, hits and a stalled snapshot overlap; the upload snapshot holds only the entry lock (shared),
  and releases it (and its reference) before the network PUT. The directory "clean" record is written only while holding
  the entry lock shared after re-checking that no writer intervened.
* `WRITING2` protection: a writer marks CLEAN->DIRTY or WRITING->WRITING2 under the mutex while holding the entry lock
  exclusively, before its payload. The snapshot clears the mark (its copy contains every committed payload); later
  writers set it again, so an older PUT never makes a newer generation CLEAN.
* A CLEAN->DIRTY write is visible as DIRTY before its payload is committed; FLUSH waits for the entry (the worker waits
  for the entry lock), so FLUSH cannot complete before the writer's payload is committed and unlocked. A full-block write
  miss is a READING entry until its payload and dirty record are written, then DIRTY; FLUSH treats it as not yet written
  (the write has not returned).
* Shutdown: under the mutex it sets `draining` (new reads/writes then return `ENOTCONN` and nothing is acknowledged), then waits
  until every admitted request has returned (`active_ops`: covers write payloads, miss/verify owners in the lower layer, parked
  disk evictions, and requests blocked on space/dirty limits/READING waiters). The workers keep running and, while draining,
  upload dirty blocks without waiting for their delay, so admitted writes publish DIRTY and are uploaded and synchronous or
  `maxDirty` waiters make progress. Only then does it set `stopping`, drain the dirty queue, join the workers and wait for all
  references (`num_refs`). An admitted write therefore either completes and is uploaded before `shutdown()` returns, or the
  request was rejected before it mutated anything. FLUSH requests are not part of admission (unchanged).
* Full cache: allocators sleep on `space_avail` (no spin). While an allocator waits because every CLEAN[2] entry is busy,
  new CLEAN hits wait too (`space_pin_waiters`), so the entries drain. Disk-mode eviction/timeout reclaim parks the victim
  as READING (hashed, not on a list) while its directory record is erased without the mutex; waiters retry afterwards.
* A failed fill removes the entry and wakes waiters, one of which becomes the next owner and retries.
* `dcache.file_size` is read/written atomically; data writes that may extend the file (and the padding of the last slot) run
  under `size_mutex` (leaf lock); writes wholly inside the file take no lock. No whole-file I/O lock.

## Limitations (explicit)

* Writer preference is a glibc property. On other platforms the default rwlock policy applies and writer starvation by
  continuous readers is not excluded. Strict FIFO among writers is **not** claimed or tested; tests assert bounded
  writer progress under continuous readers and that a reader arriving behind a waiting writer queues behind it (Linux).
* `read_direct` applies to CLEAN hits only (as before); DIRTY/WRITING/WRITING2 hits use buffered cache-file reads.
  A reader that queued behind a writer may be served by a buffered or direct read depending on the state at queue time;
  direct reads after a buffered write rely on Linux page-cache coherency (`direct_test coherence`).
* `fdatasync` now runs concurrently from several threads (over-syncing only). Crash ordering is unchanged (erase before
  data, dirty record before payload, clean record after the PUT), but power-fail behaviour was not exercised.
* Callers that issue requests concurrently with shutdown must tolerate `ENOTCONN` once draining starts. Shutdown waits for
  admitted requests that are blocked on the lower layer (it cannot abort a stuck backend read/PUT).
* Tests prove the unit behaviour with fake backends; they do not prove mounted behaviour, performance or crash recovery.
  The `ml-dcache-extend` case passes with `size_mutex` removed (the file-size field is atomic, so TSAN reports no race
  and the padding step is only logically racy); that mutant is **not** detected by this suite.
* Three baseline cases are superseded and adapted (marked `ML-ADAPT` in `multilevel_test.c`); the unmodified copies still run on
  the candidate as expected-failure legacy controls (9 cells per variant, reported as `pins-superseded/*`): `reader-vs-write`
  and `evict-pinned` assert that a later reader copies under the global mutex, and `shutdown-active` asserts that shutdown has
  already stopped the workers while a reader is active (shutdown now first stops admission and waits for the active read).
  The adapted versions assert the stronger entry-lock/queueing/draining behaviour.
* TSAN on this sandbox often crashes before `main` (address-space randomization). The runner retries only startup failures:
  `mapping-message` (TSAN fatal init text), `no-start-marker` (an `ml-test` binary, which flushes `TEST-START <case>` to
  stderr first, died without it) or `unverified-no-marker` (a binary without a marker died with SIGSEGV/core and no output:
  ASSUMED, not proven, to be that startup crash). Races, sanitizer reports, test failures and crashes after the marker are
  never retried; every attempt log is kept.

## Build and tests

See `tests/multilevel/README.md`. Runner: `tests/multilevel/run_multilevel.py` (self-contained, outputs only under `--out`).
