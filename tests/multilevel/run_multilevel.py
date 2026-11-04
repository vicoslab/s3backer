#!/usr/bin/python3 -I
"""Portable runner for the two-level block-cache locking tests (native C, actual block_cache.c/dcache.c).

No VM, SSH, mount, S3 or network. Everything (sources copies, objects, binaries, logs, cache files) is written under --out,
which must not exist. Dependencies are overridable: --cc, --deps (prefix with include/ and lib/x86_64-linux-gnu holding
libfuse3/expat/crypto/curl/z), --guest-fuse (optional, only for the guest plugin), --base-commit (git rev of the baseline).

  run_multilevel.py --out DIR --variants base-assert,cand-assert,cand-opt,cand-asan,cand-tsan --suites ml,flush,order,direct,pins
"""
import argparse, concurrent.futures as cf, hashlib, json, os, pathlib, shutil, subprocess, sys, tarfile, io, time

sys.dont_write_bytecode = True
HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent.parent
BASE = HERE / 'baseline'
SOURCES = 'nbdkit block_cache block_part dcache ec_protect zero_cache erase fuse_ops hash util compress http_io reset s3b_config test_io sslcompat gitrev'.split()
WRAPS = ['fsync', 'fdatasync', 'pread', 'pwrite', 'pthread_cond_wait', 'pthread_cond_timedwait', 'pthread_rwlock_rdlock', 'pthread_rwlock_wrlock']
ML_CASES = ['ml-writer-observation-gap', 'ml-dirty-read-same', 'ml-dirty-read-diff', 'ml-write-overlap-dirty', 'ml-write-overlap-clean', 'ml-same-block-excl', 'ml-reader-vs-write',
            'ml-writer-progress', 'ml-snapshot', 'ml-flush-pending-write', 'ml-shutdown-pending-write', 'ml-upload-rewrite', 'ml-singleflight',
            'ml-fill-error', 'ml-readahead', 'ml-evict-gate', 'ml-full-dirty', 'ml-clean2-shared', 'ml-pwrite-error', 'ml-snapshot-error', 'ml-restart',
            'ml-slot-reuse', 'ml-dcache-extend', 'ml-late-write-hit', 'ml-late-write-miss', 'ml-read-owner', 'ml-after-shutdown']
PIN_CASES = ['partial-clean', 'hit-overlap', 'same-entry', 'reader-vs-write', 'evict-pinned', 'timeout-pinned', 'pread-error', 'shutdown-active',
             'worker-flush', 'stress', 'verify-relocation']
DISK_ONLY = {'pread-error', 'verify-relocation', 'ml-clean2-shared', 'ml-pwrite-error', 'ml-snapshot-error', 'ml-restart', 'ml-dcache-extend'}
FLUSH_CASES = ['writing', 'error-retry', 'multi-writing2', 'empty-lower-error', 'lower-error', 'fua', 'timeout', 'concurrent-writing', 'error-quick-retry', 'cancel-rewrite']
ORDER_CASES = ['head-explicit', 'head-flushall', 'head-fua', 'head-maxdelay']
DIRECT_CASES = ['aligned', 'unaligned-dest', 'unaligned-off', 'unaligned-len', 'tiny', 'full-block', 'slot-boundary', 'identity', 'symlink', 'hardlink', 'range', 'capability',
                'bound', 'fdmode', 'memory', 'coherence', 'tail', 'eintr', 'einval-fallback', 'eio-error', 'concurrency', 'disabled', 'config']
# Superseded by the adapted copies in multilevel_test.c: reader-vs-write and evict-pinned assert a later reader copies under the old global mutex; shutdown-active asserts shutdown has already stopped the workers while a reader is active (now it first stops admission and waits for the active read).
SUPERSEDED = {'reader-vs-write', 'evict-pinned', 'shutdown-active'}
RD1 = '#include "config.h"\n#include <stdlib.h>\n#define getenv(n) ({ char *_v = (getenv)(n); if (_v != NULL) cc.read_direct = 1; _v; })\n'
TSAN = (['-O1', '-fsanitize=thread', '-fno-pie'], ['-fsanitize=thread', '-no-pie'], {'TSAN_OPTIONS': 'halt_on_error=1 exitcode=66 second_deadlock_stack=1'})
ASAN = (['-O1', '-fsanitize=address,undefined', '-fno-sanitize-recover=undefined', '-fno-omit-frame-pointer', '--param=asan-globals=0'], ['-fsanitize=address,undefined'],
        {'ASAN_OPTIONS': 'detect_leaks=0 abort_on_error=0', 'UBSAN_OPTIONS': 'halt_on_error=1 print_stacktrace=1'})
# name: (source, NDEBUG, sanitizer, candidate)
VARIANTS = {'base-assert': ('base', False, None, False), 'base-opt': ('base', True, None, False), 'cand-assert': ('cand', False, None, True),
            'cand-opt': ('cand', True, None, True), 'cand-asan': ('cand', False, ASAN, True), 'cand-tsan': ('cand', False, TSAN, True)}


def sha(p): return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', type=pathlib.Path, required=True)
    ap.add_argument('--variants', default='base-assert,cand-assert,cand-opt')
    ap.add_argument('--suites', default='ml,flush,order,direct,pins')
    ap.add_argument('--cases', default='', help='comma separated ml/pins case filter')
    ap.add_argument('--cc', default=os.environ.get('S3B_CC', '/usr/bin/gcc'))
    ap.add_argument('--deps', type=pathlib.Path, default=os.environ.get('S3B_DEPS'))
    ap.add_argument('--guest-fuse', type=pathlib.Path, default=os.environ.get('S3B_GUEST_FUSE'))
    ap.add_argument('--base-commit', default='444f902bcb36fc0da22d31b8bd030da72f2258e2')
    ap.add_argument('--cand-src', type=pathlib.Path, default=REPO, help='directory with the candidate *.c/*.h (default: this repository)')
    ap.add_argument('--jobs', type=int, default=6)
    ap.add_argument('--case-timeout', type=int, default=90)
    ap.add_argument('--budget', type=int, default=1500, help='seconds; remaining cases are recorded as skipped (partial progress kept)')
    a = ap.parse_args()
    out = a.out.resolve(); out.mkdir(parents=True, exist_ok=False)
    if a.deps is None: raise SystemExit('--deps (or S3B_DEPS) is required')
    deps = a.deps; lib = deps / 'lib/x86_64-linux-gnu'
    env = {k: v for k, v in os.environ.items() if k not in ('CPATH', 'C_INCLUDE_PATH', 'LIBRARY_PATH', 'LD_PRELOAD', 'TSAN_OPTIONS', 'ASAN_OPTIONS', 'UBSAN_OPTIONS')}
    env['LD_LIBRARY_PATH'] = str(lib)
    (out / 'tmp').mkdir()
    env['TMPDIR'] = str(out / 'tmp')
    NOASLR = ['setarch', os.uname().machine, '-R'] if shutil.which('setarch') and subprocess.run(['setarch', os.uname().machine, '-R', 'true'], capture_output=True).returncode == 0 else []
    t_start = time.time(); suites = a.suites.split(','); only = set(filter(None, a.cases.split(',')))
    rec = {'repo': str(REPO), 'compiler': subprocess.check_output([a.cc, '--version'], text=True).splitlines()[0], 'compiler_sha256': sha(pathlib.Path(a.cc).resolve()),
           'inputs': {str(p.relative_to(HERE)): sha(p) for p in sorted(HERE.rglob('*')) if p.is_file() and '__pycache__' not in p.parts},
           'dependency_files': {n: sha((lib / f'lib{n}.so').resolve()) for n in ['fuse3', 'expat', 'crypto', 'curl', 'z'] if (lib / f'lib{n}.so').exists()},
           'variants': {}, 'tests': {}, 'artifacts': {}, 'commands': []}
    def save(): (out / 'receipt.json').write_text(json.dumps(rec, indent=1) + '\n')

    def run(cmd, log, **kw):
        rec['commands'].append([str(c) for c in cmd])
        return subprocess.run([str(c) for c in cmd], env=env, timeout=600, check=True, stdout=log, stderr=log, **kw)

    for name in a.variants.split(','):
        srckind, ndebug, san, cand = VARIANTS[name]
        b = out / name; src = b / 'src'; src.mkdir(parents=True)
        if srckind == 'base':
            tar = subprocess.check_output(['git', '-C', REPO, 'archive', a.base_commit])
            with tarfile.open(fileobj=io.BytesIO(tar)) as t: t.extractall(src, filter='data')
        else:
            for f in list(a.cand_src.glob('*.c')) + list(a.cand_src.glob('*.h')): shutil.copy(f, src / f.name)
        cfg = (HERE / 'config.h').read_text()
        if not ndebug: cfg = cfg.replace('#define NDEBUG 1\n', '/* NDEBUG removed: assertions enabled for tests */\n')
        (src / 'config.h').write_text(cfg); (src / 'gitrev.c').write_text('const char *const s3backer_version = "";\n')
        cflags, lflags, renv = (san if san else ([], [], {}))
        flags = [a.cc, '-std=gnu11', '-O2', '-g', '-march=x86-64', '-mtune=generic', '-fPIC', '-ffunction-sections', '-fdata-sections', '-pthread', *cflags,
                 f'-I{src}', f'-I{HERE}', f'-I{deps}/include', f'-I{deps}/include/x86_64-linux-gnu', f'-I{deps}/include/fuse3'] + (['-DREAD_PINS_CANDIDATE'] + (['-DMULTILEVEL_CANDIDATE'] if cand else []) + (['-DML_HAS_DRAINING'] if 'draining' in (src / 'block_cache.c').read_text() else []))
        if not ndebug: flags += ['-DML_EXPECT_ASSERTIONS']
        if 'local_flush' in (src / 'block_cache.h').read_text(): flags += ['-DML_HAS_LOCAL_FLUSH']
        if 'dirty_eviction_grace' in (src / 'block_cache.h').read_text(): flags += ['-DML_HAS_DIRTY_EVICTION']
        libs = [f'-L{lib}', f'-Wl,-rpath,{lib}', '-lfuse3', '-lexpat', '-lcrypto', '-lcurl', '-lz']
        W = lambda *ws: [f'-Wl,--wrap={w}' for w in ws]
        COND = ['pthread_cond_wait', 'pthread_cond_timedwait']
        link = [a.cc, '-pthread', *lflags, '-Wl,--gc-sections']
        core = [b / f'{s}.o' for s in ['hash', 'util', 'dcache', 'block_part']]
        art = rec['artifacts'][name] = {}
        rec['variants'][name] = {'source': srckind, 'ndebug': ndebug, 'sanitizer': bool(san), 'cflags': flags[1:]}
        with (b / 'compile.log').open('w') as log:
            try:
                for s in (SOURCES if (ndebug and not san) else ['hash', 'util', 'dcache', 'block_part', 'block_cache']):
                    run([*flags, '-c', src / f'{s}.c', '-o', b / f'{s}.o'], log)
                (b / 'ml_rd1.c').write_text(RD1 + f'#include "{HERE / "multilevel_test.c"}"\n')
                (b / 'pins_rd1.c').write_text(RD1 + f'#include "{BASE / "read_pins_test.c"}"\n')
                (b / 'flush_rd1.c').write_text(RD1 + f'#include "{BASE / "flush_test.c"}"\n')
                (b / 'order_rd1.c').write_text(RD1 + f'#include "{BASE / "flush_order_test.c"}"\n')
                units = {'ml': HERE / 'multilevel_test.c', 'ml_rd1': b / 'ml_rd1.c', 'pins': BASE / 'read_pins_test.c', 'pins_rd1': b / 'pins_rd1.c',
                         'direct': BASE / 'direct_test.c', 'cache_orig': BASE / 'cache_test.c', 'flush': BASE / 'flush_test.c', 'flush_rd1': b / 'flush_rd1.c',
                         'order': BASE / 'flush_order_test.c', 'order_rd1': b / 'order_rd1.c', 'order_cache': BASE / 'flush_order_cache.c'}
                for u, f in units.items(): run([*flags, '-c', f, '-o', b / f'{u}.o'], log)
                for exe, m, wr in [('ml-test', 'ml', W(*WRAPS)), ('ml-test-rd1', 'ml_rd1', W(*WRAPS)), ('read-pins-test', 'pins', W('pread', *COND)), ('read-pins-test-rd1', 'pins_rd1', W('pread', *COND))]:
                    run([*link, *wr, b / f'{m}.o', *[b / f'{s}.o' for s in ['hash', 'util', 'dcache']], *libs, '-o', b / exe], log)
                for exe, m, c in [('flush-test', 'flush', 'cache_orig'), ('flush-test-rd1', 'flush_rd1', 'cache_orig'), ('order-test', 'order', 'order_cache'), ('order-test-rd1', 'order_rd1', 'order_cache')]:
                    run([*link, *W(*COND), b / f'{c}.o', b / f'{m}.o', *core, *libs, '-o', b / exe], log)
                run([*link, *W('pread', 'open'), b / 'direct.o', *[b / f'{s}.o' for s in ['block_cache', 'hash', 'util', 'dcache']], *libs, '-o', b / 'direct-test'], log)
                for exe in ['ml-test', 'ml-test-rd1', 'read-pins-test', 'flush-test', 'order-test', 'direct-test']: art[exe] = sha(b / exe)
                if ndebug and not san:
                    objects = [b / f'{s}.o' for s in SOURCES]
                    run([a.cc, '-shared', '-pthread', *objects, *libs, '-o', b / f'nbdkit-s3backer-{name}-host.so'], log)
                    art['host_so'] = sha(b / f'nbdkit-s3backer-{name}-host.so')
                    if a.guest_fuse and pathlib.Path(a.guest_fuse).exists():
                        guest = b / f'nbdkit-s3backer-{name}-guest.so'
                        run([a.cc, '-shared', '-pthread', *objects, a.guest_fuse, f'-L{lib}', '-lexpat', '-lcrypto', '-lcurl', '-lz', '-o', guest], log)
                        art['guest_so'] = sha(guest)
                        art['guest_fuse_sha256'] = sha(a.guest_fuse)
            except subprocess.CalledProcessError as e:
                rec['tests'][name] = {'BUILD': {'exit': e.returncode, 'startup_failure': True}}; save(); print(name, 'BUILD FAILED', e.returncode, flush=True); continue
        res = rec['tests'][name] = {}
        jobs = []
        def add(binary, case, label, use_disk):
            if only and case not in only: return
            jobs.append((b / binary, case, label, use_disk))
        if 'dirty' in suites:
            for c in ['de-hog', 'de-default', 'de-shared', 'de-rewrite', 'de-read-recency', 'de-idle', 'de-young', 'de-put-rewrite', 'de-upload-error', 'de-busy-dirty', 'de-busy-clean', 'de-config', 'de-crash', 'de-stress', 'de-boundary', 'de-restart-grace', 'de-snapshot-error', 'de-drain-selected', 'de-reordered-error', 'de-put-shutdown']:
                add('ml-test', c, f'dirty/disk-rd0/{c}', True)
                add('ml-test-rd1', c, f'dirty/disk-rd1/{c}', True)
        if 'local' in suites:
            for c in ['lf-read-verify-tears', 'lf-read-refill-tears', 'lf-alloc-terminal', 'lf-read-fill-tears', 'lf-remote-read-errors', 'lf-shutdown-snapshot-error', 'lf-shutdown-snapshot-short', 'lf-poison-during-sync', 'lf-poison-space-waiter', 'lf-local', 'lf-default', 'lf-sync-error', 'lf-pwrite-error', 'lf-drain', 'lf-pending', 'lf-signal', 'lf-drain-error', 'lf-shutdown-error', 'lf-pressure', 'lf-metadata-error', 'lf-create-error', 'lf-invalid', 'lf-crash', 'lf-identity-tears', 'lf-marker-error', 'lf-header-error', 'lf-clean2-write', 'lf-sync-concurrent', 'lf-reopen-gates', 'lf-erase-order', 'lf-generation', 'lf-shutdown-pending', 'lf-stress', 'lf-read-miss-error', 'lf-erase-error', 'lf-clean-migration', 'lf-maxdirty', 'lf-slot-reuse', 'lf-helper']:
                add('ml-test', c, f'local/disk/{c}', True)
        if 'ml' in suites:
            for c in ML_CASES:
                if c not in DISK_ONLY: add('ml-test', c, f'ml/memory/{c}', False)
                add('ml-test', c, f'ml/disk-rd0/{c}', True); add('ml-test-rd1', c, f'ml/disk-rd1/{c}', True)
            for c in PIN_CASES:   # adapted copy of the pinned-reader cases
                if c not in DISK_ONLY: add('ml-test', c, f'ml/memory/{c}', False)
                add('ml-test', c, f'ml/disk-rd0/{c}', True); add('ml-test-rd1', c, f'ml/disk-rd1/{c}', True)
        if 'pins' in suites and cand:   # unmodified baseline copy on the candidate (two cases are superseded, recorded separately)
            for c in PIN_CASES:
                lab = 'pins-superseded' if c in SUPERSEDED else 'read-pins'
                if c not in DISK_ONLY: add('read-pins-test', c, f'{lab}/memory/{c}', False)
                add('read-pins-test', c, f'{lab}/disk-rd0/{c}', True); add('read-pins-test-rd1', c, f'{lab}/disk-rd1/{c}', True)
        if 'flush' in suites:
            for c in FLUSH_CASES:
                add('flush-test', c, f'flush/memory/{c}', False); add('flush-test', c, f'flush/disk-rd0/{c}', True); add('flush-test-rd1', c, f'flush/disk-rd1/{c}', True)
        if 'order' in suites:
            for c in ORDER_CASES:
                add('order-test', c, f'order/memory/{c}', False); add('order-test', c, f'order/disk-rd0/{c}', True); add('order-test-rd1', c, f'order/disk-rd1/{c}', True)
        if 'direct' in suites:
            for c in DIRECT_CASES: add('direct-test', c, f'direct/{c}', True)

        def one(job):
            binary, case, label, use_disk = job
            if time.time() - t_start > a.budget: return label, {'exit': None, 'skipped': 'suite budget exhausted'}
            attempts = 0; bases = []
            while True:
                attempts += 1; tenv = dict(env, **renv); cache = None
                tenv["S3BACKER_TEST_HELPER"] = str(REPO / "tools/local-cache-drain.py")
                if use_disk:
                    cache = b / (label.replace('/', '_') + f'.{attempts}.cache'); cache.unlink(missing_ok=True); tenv['S3BACKER_TEST_CACHE'] = str(cache)
                try:
                    nor = NOASLR if 'TSAN_OPTIONS' in renv else []   # TSAN may need ASLR off on newer kernels (only if the sandbox permits it)
                    command = ['timeout', '-k', '5', str(a.case_timeout), *nor, str(binary), case]
                    r = subprocess.run(command, env=tenv, cwd=b, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                    code, text = r.returncode, r.stdout
                except Exception as e: code, text = -1, repr(e)
                if cache: cache.unlink(missing_ok=True)
                (b / (label.replace('/', '_') + f'.attempt-{attempts}.log')).write_text(text)
                # A sanitizer environment that cannot start (e.g. ASLR/memory mapping) is retried; a reported race/error never is.
                # Startup basis (TSAN only). 'mapping-message': TSAN printed its fatal init message (pre-main). 'no-start-marker': an ml-test binary
                # (which flushes "TEST-START <case>" to stderr first thing in main) died without the marker, so it never reached main. 'unverified-no-marker':
                # a binary without a marker crashed with SIGSEGV/core and no test or sanitizer output; ASSUMED to be the startup crash, not proven.
                # A test crash after the marker, a test failure, or any sanitizer/race report is never retried.
                basis = None
                if 'TSAN_OPTIONS' in renv:
                    has_marker = 'TEST-START' in text
                    reported = any(m in text for m in ['WARNING: ThreadSanitizer', 'runtime error', 'FAIL', 'PASS', 'observed'])
                    crashed = code in (-11, 139) or 'dumped core' in text
                    if 'unexpected memory mapping' in text or 'ThreadSanitizer: failed to' in text or 'FATAL: ThreadSanitizer' in text: basis = 'mapping-message'
                    elif crashed and not has_marker and not reported: basis = 'no-start-marker' if pathlib.Path(binary).name.startswith('ml-test') else 'unverified-no-marker'
                startup = basis is not None
                bases.append(basis)
                # One attempt: retain actual sanitizer environment failures.
                break
            return label, {'exit': code, 'command': command, 'cwd': str(b), 'environment': {k: tenv[k] for k in ['TMPDIR', 'LD_LIBRARY_PATH', 'S3BACKER_TEST_CACHE', *renv] if k in tenv},
                           'attempts': attempts, 'startup_failure': startup, 'startup_attempt_basis': [x for x in bases if x],
                           'sanitizer_report': any(m in text for m in ['WARNING: ThreadSanitizer', 'ERROR: AddressSanitizer', 'runtime error:']),
                           'timed_out': code in (124, 137), 'output': text.strip()[-500:]}
        with cf.ThreadPoolExecutor(a.jobs) as ex:
            for label, r in ex.map(one, jobs):
                res[label] = r
                print(name, label, r['exit'], r.get('output', '').replace('\n', ' | ')[-160:], flush=True)
        save()
    # verdict: candidate variants must be all-green; base variants record RED cells
    verdict = {}
    for n, r in rec['tests'].items():
        ok = {k: v for k, v in r.items() if not k.startswith('pins-superseded')}
        bad = [k for k, v in ok.items() if v['exit'] != 0]
        verdict[n] = {'total': len(ok), 'failed': len(bad), 'skipped': sum(1 for v in ok.values() if v['exit'] is None), 'failing': bad[:80]}
    rec['verdict'] = verdict; rec['elapsed_s'] = round(time.time() - t_start, 1); save()
    print(json.dumps({k: {x: y for x, y in v.items() if x != 'failing'} for k, v in verdict.items()}))
    cand_bad = any(v['failed'] for k, v in verdict.items() if k.startswith('cand'))
    raise SystemExit(1 if cand_bad else 0)


if __name__ == '__main__':
    main()
