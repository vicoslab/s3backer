# Persistent local FLUSH

`--blockCacheLocalFlush` defaults to **off**. With it off, NBD FLUSH/FUA retain
remote durability and existing write-delay behavior. With it on, FLUSH, FUA and
FUSE fsync synchronize the persistent cache payload and recovery mapping, leaving
blocks DIRTY until their actual successful PUT. The cache may be the **sole copy**
of acknowledged data. Loss of that file or its storage can lose acknowledged data.

Requires `--blockCacheFile=PATH`, nonzero cache capacity and
`--blockCacheRecoverDirtyBlocks`. RAM, no verification, synchronous writes,
shared mode, force and reset are rejected. Preserve the same block size/capacity.
Local mode refuses old-format files and resize. Migrate an existing cache only
after a successful remote drain and clean shutdown in its original mode.

Local mode disables timed uploads regardless of `blockCacheWriteDelay`.
Uploads run for space progress, at the existing 90% dirty threshold (or the
configured lower maximum), explicit drain, and shutdown. FLUSH briefly gates new
writes while prior admitted mutations finish and the cache syncs; reads continue.
It holds no cache housekeeping mutex over sync. FUA currently uses the same full
local barrier as FLUSH, which also covers its requested range.

`--blockCacheDirtyEvictionGrace=120000` optionally allows blocks last written at
least 120000 **milliseconds** ago to compete with CLEAN blocks for space. The
default is `0` (off), preserving the existing CLEAN-only eviction policy. This
requires FILE local FLUSH mode and `--blockCacheNumProtected=0`; protected slots
are rejected with this option. No data PUT is triggered just because grace expires.

At full capacity, the least recently accessed idle CLEAN or eligible DIRTY block
wins. Reads refresh recency but do not restart write grace; each successful write
does both. Thus old DIRTY data cannot indefinitely crowd out a larger fresh read
set. A selected DIRTY block goes through the existing worker upload path before
its slot can be reused. Allocators wait without retaining an entry pointer, then
rescan shared recency. A concurrent rewrite remains DIRTY after an older PUT.
Upload errors retain the local copy and allow an idle CLEAN fallback; explicit
drain reports the error and can retry it. Existing all-dirty pressure fallback,
dirty threshold uploads, explicit drain and shutdown still apply regardless of
grace, so this is not a minimum remote retention guarantee.

Last-write time uses a 64-bit monotonic millisecond clock, separate from a 64-bit
access-order sequence shared by reads and writes. Both are RAM-only; recovery
starts a new full grace interval for recovered DIRTY blocks. The cache header,
LOCAL_ACK marker, local acknowledgment and recovery format are unchanged.

Use the existing stats mirror, for example `--statsFileMirror=/local/cache.stats`.
Request a bounded remote barrier with:

```
python3 tools/local-cache-drain.py /local/cache.stats --timeout 330
```

The helper verifies enabled mode, PID and Linux process start time, signals via a
pidfd, and waits for completion/error counters. It requires Linux pidfd support.
The opt-in SIGUSR1 handler only posts a process-lifetime semaphore; an owned thread
runs the existing remote barrier with a 300 second remote timeout. Signal the
**nbdkit PID from the stats**, not the s3backer launcher. Raw `kill -USR1 PID` is
asynchronous: delivery alone is not successful completion. Do not signal a default
mode process. Stats include local mode, PID/start time, requested/completed drains,
last error, total drain errors and sticky local error; clearing ordinary stats
does not reset these lifecycle counters. Concurrent external drain requests should
be avoided. The helper serializes other helpers using a sibling lock file.

Normal shutdown closes admission and drains remotely with workers alive. The
header's existing flags word gains `LOCAL_ACK=0x2` (no size/layout change), cleared
only after successful clean shutdown and token clearing. Until then, older binaries,
option-off starts and reset refuse the file. To migrate back, drain, shut down
cleanly, verify success, then restart without the option. Never remove the marker
or cache to work around an error.

A local data/metadata/sync error poisons the mount: subsequent writes, local FLUSH
and remote drain fail. Preserve cache and backend evidence; stop application I/O
and investigate storage errors before recovery. A remote PUT error retains DIRTY
data and reports drain failure. Shutdown failure retains the header marker.

The S3 token and cache token are two resources, without an atomic commit. If they
mismatch, startup fails closed. Stop all possible owners, retain an offline copy
of the cache and remote token/object evidence, establish the authoritative owner,
and reconcile the matching token pair under operator control before restarting
with local flush and recovery enabled. There is no automatic adopt/discard switch;
`--force` and reset are not recovery procedures.

Persistence depends on truthful filesystem/device sync semantics. This is not
atomic block replacement, a WAL, or proof against arbitrary controller corruption.
Unacknowledged in-place writes may tear. Invalid zero-etag CLEAN identities are
ignored; DIRTY markers follow durable identity/payload, and CLEAN overwrite and
identity erase are preceded by durable marker transitions. Byte/sector alignment
is not assumed atomic. Native process-kill and prefix-tear tests supplement, but
do not replace, isolated power-loss/runtime validation and independent review.
