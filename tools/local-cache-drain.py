#!/usr/bin/env python3
"""Request a remote barrier from an opted-in mount; requires its stats mirror and Linux pidfds."""
import argparse
import fcntl
import os
from pathlib import Path
import signal
import time

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('stats', type=Path)
p.add_argument('--timeout', type=float, default=330)
a = p.parse_args()
if a.timeout <= 0 or a.timeout > 3600:
    p.error('--timeout must be in (0, 3600] seconds')

def stats():
    return {parts[0]: int(parts[1]) for line in a.stats.read_text().splitlines()
            if (parts := line.split()) and parts[0].startswith(('block_cache_drain_', 'block_cache_local_'))}

def identity(s):
    if s.get('block_cache_local_flush') != 1:
        raise RuntimeError('stats do not identify an enabled local-flush mount')
    return s['block_cache_drain_pid'], s['block_cache_drain_start_ticks']

# Serialize helpers across atomic replacement of the mirror file.
with Path(str(a.stats) + '.drain.lock').open('a') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    before = stats()
    pid, born = identity(before)
    fd = os.pidfd_open(pid)
    try:
        # A container may mount procfs from an outer PID namespace. fdinfo gives
        # the PID as represented by that procfs, while pidfd_open/signal use ours.
        info = dict(line.split(':', 1) for line in Path(f'/proc/self/fdinfo/{fd}').read_text().splitlines() if ':' in line)
        proc_pid = int(info['Pid'].strip())
        actual = int(Path(f'/proc/{proc_pid}/stat').read_text().rsplit(')', 1)[1].split()[19])
        if actual != born or identity(stats()) != (pid, born):
            raise RuntimeError('PID identity changed or stale stats mirror')
        if before['block_cache_local_error']:
            raise RuntimeError('local cache has a sticky I/O error; preserve it for recovery')
        if before['block_cache_drain_requested'] != before['block_cache_drain_completed']:
            raise RuntimeError('a drain is already active; wait for its completion')
        target = before['block_cache_drain_completed'] + 1
        signal.pidfd_send_signal(fd, signal.SIGUSR1)
        deadline = time.monotonic() + a.timeout
        while time.monotonic() < deadline:
            now = stats()
            if identity(now) != (pid, born):
                raise RuntimeError('mount identity changed during drain')
            if now['block_cache_drain_completed'] >= target:
                error = now['block_cache_drain_error'] or now['block_cache_local_error']
                if now['block_cache_drain_errors'] > before['block_cache_drain_errors']:
                    error = error or 5
                if error:
                    raise RuntimeError(f'remote drain failed: errno {error}')
                print(f'remote drain completed: PID {pid}, completion {now["block_cache_drain_completed"]}')
                break
            time.sleep(min(0.2, max(0, deadline - time.monotonic())))
        else:
            raise TimeoutError('no completed drain observed before deadline; request may still be running')
    finally:
        os.close(fd)
