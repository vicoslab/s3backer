# Two-level block-cache locking: design (original proposal; see docs/locking.md for the implementation)

Base: branch `multilevel-cache-locking` @ `444f902` = upstream `76bab14` + nbd-negotiation, flush,
read-pins, direct-read, flush-order (source of pinned production plugin `adde0db2…b0b9`).
Inputs read: `block_cache.c` (sha256 `e492f036…6f6a`), `dcache.c` (`245d6c5f…6263`), `dcache.h`.
Actual protected production profile (corrected; the earlier 64 MiB / 64-block / 8-16 thread / 250 ms figures are
historical and were wrong for this work): large `blockSize=16MiB`, 6400 cache slots, 100 GiB cache, 48 request and
48 cache threads; small profile 256 slots, 4 GiB, 16/16; `maxWriteDelay=4294967232` ms, `maxDirty=0`, `lowerDirect=true`,
read-ahead 16/2, kernel readahead 16384 KiB, WBT 0. The scratch profile will use 16 MiB blocks, 1536 slots, 24 GiB, 48/48.
Every payload copy is therefore up to 16 MiB, and holding the one mutex `priv->mutex` across any copy stalls the whole cache.

> **Implementation status.** Implemented in a simplified form, see `docs/locking.md` for what actually shipped.
> Deviations from this design: the per-entry lock is a standard `pthread_rwlock_t` (glibc writer preference), not the
> custom phase-fair lock; the entry state encoding is unchanged (no explicit `state`, no `DEAD`, no `loaded`); eviction
> in disk mode parks the victim as READING instead of recycling its struct; a new full-block write publishes a READING
> entry while the payload is written; shutdown stops admission and drains admitted requests (`draining`/`active_ops`). Sections 3-6 below are
> the original proposal and are kept for review history only.

## 1. What currently runs under the global mutex (G)

Today only pinned CLEAN hits copy with G released (`block_cache_read_pinned`). These still hold G:

| Site | Work under G |
|---|---|
| `do_read` CLEAN2→READING2 (`block_cache.c:944`) | dir erase + `fdatasync` |
| `do_read` hits on DIRTY/WRITING/WRITING2 (`:972`) | payload copy / `pread` |
| `do_read` READING completion (`:1064–1080`) | memcpy to dest, 64 MiB `pwrite`, record + `fdatasync` |
| `block_cache_write` (`:1189,1204,1265,1281`) | payload copy / `pwrite`, dirty record + `fdatasync` |
| `get_entry`→`free_entry` eviction (`:1434`) | dir erase + `fdatasync` |
| worker snapshot (`:1526`) | 64 MiB copy / `pread` |
| worker success (`:1567`) | clean record + `fdatasync` |
| `block_cache_verified` (`:1778`) | moves the entry to a new address, so a lock embedded in the entry would be copied |

`dcache.c` has no lock of its own. Its free list and `num_alloc` rely on G. `priv->file_size` is
read and written by every data write (`s3b_dcache_write_block`, `write2`, the falloc extend path),
so it races once writes overlap.

## 2. Locks and order

* **G** `priv->mutex`. Protects: hash, LRU lists, dirty queue, all `state`/`timeout`/list links,
  intent counters, `wgen`/`upload_gen` writes, stats, flush waiters, read-ahead, survey, the dcache
  free list (`alloc_block`/`free_block`), `calloc`/`malloc`/`free` of entries and buffers, and
  lifetime.
* **E** `entry->lock`. One phase-fair reader/writer lock per entry, embedded in a stable entry.
  It protects the payload bytes (buffer or dslot data), the dslot's directory record, and `loaded`.
* **E.m** is E's internal mutex. It is a leaf, held only for O(1) counter updates and never across
  a wait.
* **D** `dcache->size_mutex`. A new leaf mutex in dcache, used only by data writes that extend the
  file.

Allowed nesting: `E → G → E.m`, and `E → D`. At most one E per thread.

Rules:
1. Never block acquiring E while G is held. Under G the only E operations allowed are
   `el_trywrlock` on an entry with no intents (it cannot fail or block, and is asserted) and
   `el_unlock`.
2. Every intent is taken under G before G is dropped, and released under G in the same section
   as `el_unlock`. So, as seen under G, "no intents" means E is idle and nobody can still touch
   the entry.
3. Under G there is no backend I/O, no cache-file data or directory I/O, no `fdatasync`, and no
   payload memcpy. Debug-only `assert(s3b_dcache_entry_is_empty())` reads in `alloc_block` and
   `free_block` are the exception; they compile out with `NDEBUG`.
4. G may be waited on with `pthread_cond_wait` (space, write_complete, flush, sync, worker_work,
   unpinned) only while the thread holds no E.

## 3. Stable entry and fields

`block_cache_verified` no longer relocates: it becomes `entry->verify = 0; entry->state = CLEAN`.
`etag[MD5_DIGEST_LENGTH]` becomes a fixed member, so `dcache_load` uses `calloc(1, sizeof(*entry))`.
Entries never move between creation and free. The cost is about 110 B per entry (16 B ETag + about
92 B lock), roughly 7 KiB at 64 entries.

```c
struct entry_lock { pthread_mutex_t m; pthread_cond_t cv;
                    uint32_t readers, rwait, wwait, admit, wseq; uint8_t writer; };
struct cache_entry {
    s3b_block_t block_num;      // G; immutable while hashed
    int      write_error;       // G
    uint8_t  state;             // G (explicit; replaces link/timeout/dirty encoding, READING_TIMEOUT gone)
    uint8_t  verify;            // G (CLEAN2/READING2)
    uint8_t  loaded;            // E: payload valid; set by the fill owner or new-entry writer holding E-excl
    uint32_t timeout;           // G
    uint32_t readers;           // G: shared intents (hit copiers, uploader snapshot/record, fill waiters)
    uint32_t excl_waiters;      // G: exclusive intents (writers, fill/verify owner)
    uint32_t wgen;              // written with E-excl AND G held; readable under either
    uint32_t upload_gen;        // G; written only by the uploader
    TAILQ_ENTRY(cache_entry) link;   // G (LRU, dirties, or local reclaim list)
    union { void *data; u_int dslot; } u;   // immutable while hashed
    u_char   etag[MD5_DIGEST_LENGTH];      // G (CLEAN2) / E-excl owner (READING2)
    struct entry_lock lock;
};
#define ENTRY_GET_STATE(e) ((e)->state)
#define ENTRY_BUSY(e)      ((e)->readers != 0 || (e)->excl_waiters != 0)   // = lifetime refs
```

The `readers`, `excl_waiters`, `num_pins` (sum of `readers`), `space_pin_waiters`, `unpinned`,
`space_avail`, `write_complete` and `dirties`/`link` names are kept on purpose. The existing
harnesses read them (§9). New private fields: `num_refs` (sum of all intents, including DEAD
entries) and `num_reclaiming`. `end_reading` and `block_cache_wait_unpinned` are removed. A new
state `DEAD = 7` marks an entry that is unhashed after a failed fill and freed by its last intent.

### Entry lock (`el_*`, static in block_cache.c, about 30 lines)

The lock is phase-fair: writers are preferred, and readers that queued behind a writer get in at
the next writer release, so neither side starves. It uses its own mutex and cond so that it is
portable (macOS has no `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP`). The waits also go through
the harnesses' wrapped `pthread_cond_wait`, which makes them observable.

```
rdlock:  lock m; if (writer || wwait) { rwait++; t = wseq; while (writer || wseq == t) wait;
                                        rwait--; admit--; } readers++; unlock m
wrlock:  lock m; wwait++; while (writer || readers || admit) wait; wwait--; writer = 1; unlock m
trywr:   lock m; ok = !(writer|readers|wwait|rwait|admit); if ok writer = 1; unlock m; return ok
unlock:  lock m; if (writer) { writer = 0; wseq++; admit = rwait; } else readers--; broadcast cv; unlock m
```

## 4. Lifetime

* Any thread that will touch an entry outside G first increments `readers` or `excl_waiters`
  (and `num_refs`, plus `num_pins` for `readers`) under G. An entry with intents is never
  evicted, recycled or freed.
* Entries in READING[2], DIRTY or WRITING[2] are never evictable, whatever their intents. The
  uploader therefore holds no intent during network I/O.
* `entry_release(priv, e, shared)`, called with G held, does `el_unlock`, decrements the intent,
  then the wake-ups: `unpinned` when `num_refs` reaches 0 and `unpin_waiters` is set;
  `space_avail` when the entry is CLEAN[2] and idle and `space_pin_waiters` is set; `worker_work`
  when it is idle, CLEAN and expired. A DEAD entry whose intents reach 0 has its lock destroyed
  and is freed.
* **Recycle (full cache).** Under G, the allocator takes the first idle CLEAN/CLEAN2 entry from
  `lo_cleans`, then `hi_cleans` (existing `block_cache_first_idle`). It unlinks it from the LRU
  and the hash, and rekeys the same struct: new `block_num`, cleared `verify`, `loaded`, `wgen`
  and `write_error`. It then runs `el_trywrlock` (which must succeed), sets `excl_waiters = 1`
  and reinserts. Memory mode reuses the buffer. Disk mode reuses the dslot; the new owner must
  erase the old directory record outside G before writing data (`need_erase`, a local).
  Crash-safe: until that erase, the old record still describes the old data.
* **Timeout reclaim (worker).** Disk mode: under G, unlink expired idle CLEAN[2] entries into a
  local list and add them to `num_reclaiming`. Drop G and erase each directory record. Retake G:
  `s3b_dcache_free_block`, destroy and free each entry, subtract from `num_reclaiming`, broadcast
  `space_avail`, `continue` (rescan, so no stale `clean_entry`). Memory mode stays entirely
  under G (only `free`).
* Capacity test: `s3b_hash_size + num_reclaiming < cache_size`. An allocator that finds no idle
  CLEAN while `num_reclaiming > 0` waits on `space_avail`, which reclaim signals.

## 5. State transitions

"Excl"/"shared" refer to E. Every transition is made under G.

| From → To | By | E held at transition | Side effects outside G |
|---|---|---|---|
| (none)/recycled → READING | read miss | excl via trywr (fresh, unreachable) | [erase old record] backend read; memcpy dest; dslot write; record clean |
| READING → CLEAN | fill owner | excl, released in same G section | — |
| READING → DEAD (unhashed, dslot freed, buffer freed) | fill owner on error | excl, released | waiters see `loaded == 0` and retry |
| CLEAN2 → READING2 | reader; only when the entry is idle (invariant: CLEAN2 always idle) | excl via trywr | erase record; backend verify/read; [dslot write]; record clean |
| READING2 → CLEAN | verify owner | excl, released; then `goto again` with G held → ordinary hit | — |
| READING2 → DEAD | verify owner on error | excl, released | — |
| CLEAN2 → CLEAN | writer, before taking its intent | none | — |
| CLEAN → DIRTY (`wgen++`, LRU→dirties tail, `num_dirties++`) | writer | excl (acquired after the intent; revalidated) | record dirty, then payload write |
| (none)/recycled → DIRTY (`wgen = 1`) | full-block write miss | excl via trywr | [erase old] payload write, then record dirty |
| DIRTY → DIRTY (`wgen++`) | writer | excl | payload write |
| DIRTY → WRITING (off dirties, `upload_gen = wgen`) | worker select | none (intent `readers++`) | rdlock, snapshot copy |
| WRITING → WRITING2 (`wgen++`) | writer | excl | payload write |
| WRITING2 → WRITING (`upload_gen = wgen`) | uploader at snapshot end | shared, released | then backend PUT on private buffer |
| WRITING[2] → DIRTY head (snapshot error: `write_failed`, delayed timeout) | uploader | shared, released | — |
| WRITING[2] → DIRTY head (PUT error; `write_failed` unless `ECONNABORTED && wgen != upload_gen`) | uploader | none | — |
| WRITING (`wgen == upload_gen`) → CLEAN | uploader after PUT success | memory: none; disk: shared, taken outside G, record clean done first, released in this section | — |
| WRITING2 (`wgen != upload_gen`) → DIRTY tail, timeout `now + dirty_timeout` | uploader after PUT success | same | — |
| CLEAN[2] idle → recycled/reclaimed | allocator/worker | none (idle) | erase record (reclaim/new owner) |

Generation rule: only the uploader moves an entry to CLEAN, and only if `wgen == upload_gen` while
it holds E-shared, which excludes every writer. Each writer bumps `wgen` while holding E-excl.
An old PUT therefore can never clean a newer generation. A writer that slips in between select
and snapshot is absorbed by the WRITING2→WRITING relabel, since the snapshot contains its data.
`check_cancel` returns `state == WRITING2`, which is equivalent to `wgen != upload_gen`.
Invariant: for WRITING[2] entries, `state == WRITING2 ⇔ wgen != upload_gen`.

## 6. Paths

Contract: `do_read` is called with G held and returns with G held. It drops G internally.

**Read hit** (`block_cache_read`, `do_read`), after the existing read-ahead bookkeeping:
```
G: e = hash_get; if len == 0 && state not READING[2]: (stats) read_hits++, return 0
   CLEAN/CLEAN2 and (space_pin_waiters > 0): cond_wait(space_avail); goto again      // drain gate
   CLEAN2: claim → READING2 (above); goto fill
   CLEAN: LRU tail, timeout;  direct = read_direct            // direct only for CLEAN hits, as today
   readers++ (all of CLEAN, DIRTY, WRITING[2], READING[2])
drop G; el_rdlock(e); if (!e->loaded) r = RETRY else copy (memcpy | dcache read[_direct]) to dest
G: entry_release(shared); RETRY → goto again; r == 0 → read_hits++ (if stats); return r
```
A waiter on READING[2] blocks in `el_rdlock` until the owner releases. That is the singleflight;
`end_reading` is gone. On a failed fill the waiter retries the lookup and may become the next
owner, which matches today's `goto again`. If a writer won the race between the G section and
`el_rdlock`, the reader sees the whole newer generation. On Linux a direct read is still coherent
with a buffered write just made; the existing `direct_test coherence` case covers this.

**Miss fill**: under G, the existing stats, survey disqualify and capacity logic; then
`calloc` + `el_init` + trywr, or recycle; dslot pop or buffer malloc; state READING, `loaded = 0`,
`excl_waiters = 1`, insert. If there is no space: nudge the dirty head, then
`space_pin_waiters++` if `num_cleans > 0`, wait `space_avail`, `space_pin_waiters--`. If the
count drops to 0, broadcast `space_avail` to release gated hits. `goto again`.
Outside G: malloc the disk temp buffer; `[erase]`; `inner->read_block`; memcpy to dest;
`s3b_dcache_write_block`; `record_block(etag)`; `loaded = 1`.
Back under G: READING → CLEAN, LRU tail, `num_cleans++`, `signal(space_avail)`, `entry_release`.
On error: DEAD; `s3b_dcache_free_block` (the record is already empty), or free the buffer;
unhash; `broadcast(space_avail)`; `entry_release`. The verified-but-not-read case keeps today's
`goto again`.

**Write** (`block_cache_write`):
```
G: checks (num_threads, survey);  e = hash_get
   hit: READING[2] → excl_waiters++ ; CLEAN2 → CLEAN ; CLEAN gated by space_pin_waiters ;
        CLEAN with max_dirty reached → signal worker, wait write_complete, goto again ;
        excl_waiters++
   miss partial: do_read(…, 0, 0, NULL, 0) fill; goto again (G held throughout)
   miss full: max_dirty wait; allocate/recycle + trywr → DIRTY (wgen = 1, hash, dirties tail, num_dirties++), signal worker
drop G; hit: el_wrlock(e)
G (hit only): !loaded → entry_release, goto again ; revalidate state (READING impossible):
   CLEAN: max_dirty reached → entry_release, signal, wait write_complete, goto again ;
          else CLEAN → DIRTY  (wgen++)
   DIRTY: wgen++ ; WRITING/WRITING2: → WRITING2, wgen++ ;  write_hits++ unless partial_miss
drop G; disk: [erase recycled] / [record dirty if was CLEAN, before data] ; payload write ;
        [record dirty after data, new entry; loaded = 1]   (same orders as today; errors logged as today)
G: entry_release(excl); synchronous wait loop unchanged (no E held); return 0
```
Writes to different blocks hold different E's and overlap, and their dcache `pwrite`s go to
disjoint dslots. The only shared dcache field is `file_size`, handled by D (§7).

**Upload** (`block_cache_worker_main`). Scheduling, adjusted timeouts and read-ahead signalling
are unchanged.
```
G: e = dirties head eligible → remove, state WRITING, upload_gen = wgen, readers++
drop G; el_rdlock; snapshot copy into the worker's private buf
G: ok → upload_gen = wgen; state WRITING; entry_release(shared)
       err → DIRTY head, write_failed, timeout = now + max(dirty_timeout, 1), entry_release,
             worker_wait(e), continue
drop G; r = inner->write_block(buf, etag, check_cancel)          // network, no E, no G
G: r != 0 → as today (DIRTY head, cancel test via wgen, write_failed, broadcast write_complete)
   r == 0 → write_error = 0; memory → CLEAN/DIRTY-tail per generation rule;
            disk → readers++; drop G; el_rdlock; if (wgen == upload_gen) record_block(etag);
                   G: generation rule; entry_release(shared)
   clean → num_dirties--, LRU tail, verify = 0, timeout, num_cleans++, signal space_avail ;
   always broadcast write_complete
```

**Flush, FUA, survey, stats, read-ahead, sync writes, timeouts**: these work purely on G state and
keep their existing code. Flush still snapshots via `hash_foreach`, moves DIRTY entries to the
head, waits on `write_complete` while WRITING[2], registers `write_failed` waiters, and calls
inner flush outside G. FUA (nbdkit write + `flush_blocks`) is unchanged. Read-ahead still calls
`do_read(ra, 0, 0, NULL, 0)` under G. The cancel test for read-ahead adds `num_reclaiming`.

**Shutdown**: unchanged until the workers exit and are joined. Then it waits
`while (num_refs > 0) cond_wait(unpinned)`. Afterwards no new intents can appear, because reads
and writes return `ENOTCONN` once `num_threads == 0`. Destroy asserts `num_refs == 0` and
`num_reclaiming == 0`, and `free_one` runs `el_destroy`.

## 7. Minimal dcache change (no backend change)

The only shared mutable field is `file_size`. Add `pthread_mutex_t size_mutex` (D).
`s3b_dcache_write_block` takes D only when `DATA_OFFSET(dslot + 1) > __atomic_load(file_size)`.
It rechecks under D, and the existing write and padding code then runs unchanged under D. Every
store to `file_size` (`write2`, the falloc extend, open and resize) becomes `__atomic_store`;
outside D, loads are `__atomic_load`. Writes to covered slots and directory writes never extend
the file, so they take no lock. The free list and `num_alloc` stay G-only. `pread`, `pwrite`,
`fdatasync`, `posix_fadvise`, `fallocate(PUNCH_HOLE)` on disjoint ranges, and the per-thread
direct bounce buffer are already thread-safe. Concurrent `fdatasync` calls only over-sync.

## 8. Production files touched and why

* `block_cache.c`: the struct/lock/fields in §3; `el_*`; `entry_release`; `do_read`,
  `block_cache_write`, the worker (select, snapshot, completion, reclaim), `get_entry` (recycle,
  capacity), `free_entry` (reclaim split), `verified` (no relocation), `check_cancel`, `dcache_load`,
  shutdown, destroy, `free_one`, the invariants (new: CLEAN2 ⇒ idle; READING[2] ⇒
  `excl_waiters ≥ 1`; generation iff WRITING2; `num_pins = Σreaders`; `num_refs ≥ Σintents`;
  `hash + num_reclaiming ≤ cache_size`), and the header comment. `block_cache_read_pinned` and
  `block_cache_wait_unpinned` are folded into the hit path and removed.
* `dcache.c`: D plus the atomic `file_size` (§7).
* Not touched: `block_cache.h`, `dcache.h` API, `nbdkit.c`, `fuse_ops.c`, `http_io.c`,
  `ec_protect.c`, `zero_cache.c`, other layers, configure, options. Byte, error, FLUSH and FUA
  contracts are unchanged: error values, stats points, `write_failed` timing and the dir-record
  crash ordering are kept. The only observable difference is that a reader arriving after a
  waiting writer now sees the writer's data instead of a copy made under G.

## 9. Bounded tests (to build in the implementation phase)

Location: new `tests/multilevel/{multilevel_test.c,build_multilevel.py}` in this repo. The
existing harness sources are included by path or `-I` and are never edited. Output goes to
`/var/tmp/nfs-execution/s3backer-multilevel-locking-20261103/build-<UTC>` only. The build uses
`/usr/bin/gcc` and `/var/tmp/s3-alt-run-s3backer-4c1a/tools/usr`, and the script runs with
`/usr/bin/python3 -I`. Source comes from `git archive <commit>` plus `native/config.h` (NDEBUG
stripped for assert builds) and `gitrev.c`, using the link wraps that
`read-pins/flush-order-fix/build_flush_order.py` already uses (`pread`, `open`,
`pthread_cond_[timed]wait`) plus `pwrite` for the new file. Every binary runs under `timeout`
with its own `alarm`. No VM, SSH, mount, S3 or network is involved.

Matrix: {baseline `444f902`, candidate} × {assert, NDEBUG -O2} × {memory, disk
`S3BACKER_TEST_CACHE`, disk+`read_direct` via `build_direct.RD1_PRELUDE`}. The candidate also
gets `-fsanitize=thread` and `address,undefined` builds, using the existing `TSAN`/`ASAN` tuples
from `build_direct.py`.

Reused unchanged (expected PASS on the candidate):
* `flush_test.c` + `cache_test.c`: all 10 `FLUSH_CASES`, memory and disk.
* `flush-order-fix/flush_order_test.c` + `flush_order_cache.c`: the 4 `ORDER_CASES`.
* `direct-deployment/read_pins_test.c` with `-DREAD_PINS_CANDIDATE`: `hit-overlap`, `same-entry`,
  `timeout-pinned`, `pread-error`, `shutdown-active`, `worker-flush`, `stress`,
  `verify-relocation`, `partial-clean`. `require_pins` stays valid because `readers`,
  `excl_waiters` and `num_pins` keep their meaning, and `entry_release` drops the uploader's
  intent in the same G section as `num_dirties--`.
* `direct-deployment/direct_test.c`: all 23 `DIRECT_CASES`, to check that the dcache D/atomic
  change does not regress.
* Deliberately superseded, so not run: `reader-vs-write` and `evict-pinned`. Both expect a later
  reader to copy under G (`c->g.held == 1`) and call `finish(c)` before releasing the blocker,
  which would hang in the new design. They are replaced by `ml-reader-vs-write` and
  `ml-evict-gate` below.

New `multilevel_test.c` does `#define main read_pins_main` / `#include "read_pins_test.c"` /
`#undef main`, the same pattern `flush_order_test.c` uses. It reuses `open_cache`, `warm`,
`poison`, `reader`, gates, `AWAIT`, `cache_mutex_held`, the `be_*` backend and `wgate_*`. It adds
`__wrap_pwrite` (gate kind `VIA_PWRITE`, keyed on the test thread's `tl_gate` and the length) and
a gated read hook installed via `inner.read_block`. Candidate-only internals are behind
`#ifdef MULTILEVEL_CANDIDATE`. All waits are bounded (`AWAIT` ≤ 5 s); sleeps only bound negative
observations (≤ 400 ms).

| Case | Asserts | Baseline |
|---|---|---|
| `ml-reader-vs-write` | A pinned on block 0; writer W waits (cond wait observed, G free); a later reader C blocks (not entered within 300 ms, G free); release A → W done → C sees W's generation; A got the whole old generation | RED (C copies old under G) |
| `ml-evict-gate` | all 4 entries pinned; miss M waits on `space_avail` (≤ 2 waits, no spin); a new CLEAN hit C waits on `space_avail`, not entered; release block 2 pin → M completes, then C completes with the backend generation (miss counted) | RED |
| `ml-write-overlap` | writer A on block 0 held inside its payload memcpy/pwrite with G free; writer B on block 1 and a hit on block 2 complete meanwhile; a reader of block 0 blocks; whole generations afterwards | RED (A's copy holds G) |
| `ml-fill-unlocked` | disk: miss owner held in the dslot data `pwrite` and (after a recycle) in the dir-erase `pwrite`; G free; an unrelated hit completes | RED |
| `ml-singleflight` | two concurrent misses on one block with the backend read gated: `breads + 1`; the second waits on E, not G; both get data; read-ahead singleflight via `seq` reads | GREEN (regression) |
| `ml-fill-error-retry` | first gated backend read returns EIO: owner gets EIO, the waiter retries, becomes owner and succeeds; candidate: `num_refs == 0` at the end, DEAD freed (ASAN) | GREEN |
| `ml-upload-rewrite` | worker held in PUT of block 1 (`wgate`); a hit on 1 and a write of 1 both complete before permit (no network wait); state WRITING2; permit → state not CLEAN; final backend generation = new, `bwrites == 2` | GREEN (generation regression) |
| `ml-snapshot-race` | writer holds E-excl on DIRTY block 0 (gated memcpy/pwrite) while flush makes a worker select it; worker waits (worker cond wait observed); release → exactly 1 PUT, of the new generation (`be_write` checks whole-generation), state CLEAN | GREEN |
| `ml-full-dirty-nospin` | cache full of DIRTY entries with PUT gated; a miss waits; worker cond waits over 400 ms < 50; permit → completes | GREEN |
| `ml-clean2-shared` | reload (CLEAN2); two readers and one writer on the same CLEAN2 block: exactly 1 verify, no torn data, final generation = the writer's | GREEN |

Negative controls (candidate source mutated by an exact string replace, each must turn the listed
case RED):
* completion ignores `wgen` → `ml-upload-rewrite`
* hit copy under G → `hit-overlap`
* `el_rdlock` ignores `wwait` → `ml-reader-vs-write`
* recycle ignores `ENTRY_BUSY` → `ml-evict-gate` (ASAN/assert)
* fill waiter skips the `loaded` check → `ml-fill-error-retry`
* no D → TSAN on `ml-write-overlap` (disk, fresh slots)

Budget: about 12 builds, each case ≤ 15 s, whole run ≤ 15 min on 12 cores. The receipt records
SHA-256 of every input and binary, the commands, and exit/output per case.

Acceptance for the implementation phase: all reused and new cases GREEN on the candidate in all
modes and variants (assert, opt, TSAN, ASAN); the RED cells above reproduce on baseline; all
mutants are RED. This is unit-level only. Runtime acceptance still requires the separate
authorized KVM stage. Nothing is deployed.

## 10. Residual risks and review points

* Strict writer preference within a phase: a block under continuous rewrite delays its uploader's
  snapshot by at most one writer phase. Phase fairness bounds this, and the test only asserts that
  progress happens.
* `fdatasync` in record/erase now runs concurrently from several threads. That is correct (it only
  over-syncs). Fsync cost no longer serializes unrelated blocks.
* An entry recycled while DIRTY data elsewhere is still being written is not possible, because
  only idle CLEAN[2] entries are ever recycled.
* The CLEAN2 claim requires CLEAN2 ⇒ idle. That holds because every path that would take an
  intent on a CLEAN2 entry converts it first; the invariant checker enforces it.
* Debug-build directory asserts do I/O under G. This is accepted because they compile out in
  production (`NDEBUG`).
