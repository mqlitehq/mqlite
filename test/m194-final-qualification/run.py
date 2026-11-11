"""Frozen finite paired Receive-path qualification. No external database configuration."""
import argparse, collections, datetime, hashlib, io, json, os, pathlib, platform, shutil, signal, statistics, subprocess, tarfile, tempfile, time
HERE = pathlib.Path(__file__).resolve().parent
SOURCES = {k: (v['main'], v['sdk']) for k, v in json.loads((HERE / 'plan.json').read_text())['sources'].items()}
LOCAL_DEADLINE = None

def overall_deadline(signum, frame):
    raise TimeoutError('Frozen overall local qualification deadline: 1200 seconds including 5-second cleanup reserve')

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

def dump(p, x):
    p.write_text(json.dumps(x, indent=2) + '\n')

def source(root):
    return {p.relative_to(root).as_posix(): sha(p) for p in root.rglob('*') if p.is_file()}

def checkout(repo, rev, dest):
    data = subprocess.check_output(['git', '-C', str(repo), 'archive', '--format=tar', rev])
    dest.mkdir()
    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        for m in tar:
            n = pathlib.PurePosixPath(m.name)
            if n.is_absolute() or '..' in n.parts or (not (m.isdir() or m.isfile())):
                raise ValueError('nonregular source')
            if m.isdir():
                (dest / m.name).mkdir(parents=True, exist_ok=True)
            else:
                p = dest / m.name
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(tar.extractfile(m).read())
    mapping = source(dest)
    entries = subprocess.check_output(['git', '-C', str(repo), 'ls-tree', '-r', '-z', rev]).split(b'\x00')
    expected = set()
    object_format = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', '--show-object-format'], text=True).strip()
    assert object_format in ('sha1', 'sha256')
    for entry in entries:
        if not entry:
            continue
        header, name = entry.split(b'\t', 1)
        mode, kind, checksum = header.decode('ascii').split()
        relative = os.fsdecode(name)
        assert kind == 'blob' and mode in ('100644', '100755')
        data = (dest / relative).read_bytes()
        actual = hashlib.new(object_format, b'blob ' + str(len(data)).encode() + b'\x00' + data).hexdigest()
        assert actual == checksum
        expected.add(relative)
    assert expected == set(mapping)
    return mapping

def group_exists(pgid):
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        if platform.system() != "Darwin":
            raise
        # Darwin can retain an empty process-group identity briefly after its
        # last member exits. Confirm absence through the process inventory.
        groups = subprocess.check_output(
            ["/bin/ps", "-A", "-o", "pgid="], timeout=2).split()
        if str(pgid).encode() in groups:
            raise
        return False

def stop_owned(proc):
    """Reap the leader and remove its dedicated POSIX group, even after leader exit."""
    pgid = proc.pid
    if pgid <= 1 or pgid == os.getpgrp():
        raise RuntimeError("Refusing to signal an unowned or controller group")
    existed, last_error = True, None
    try:
        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            existed = False
        except PermissionError as error:
            last_error = error
        if existed:
            time.sleep(0.2)  # The leader's wait cannot provide descendants' grace time.
    finally:
        # A controller interruption during TERM grace must still kill/reap the
        # dedicated group; a reaped leader is not proof its descendants exited.
        proc.poll()
        if existed:
            for attempt in range(3):
                try:
                    os.killpg(pgid, signal.SIGKILL)
                    last_error = None
                    break
                except ProcessLookupError:
                    last_error = None
                    break
                except PermissionError as error:
                    last_error = error
                    proc.poll()
                    if attempt < 2:
                        time.sleep(0.05)
        try:
            proc.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            raise RuntimeError("Owned subprocess leader did not join") from None
        deadline = time.monotonic() + 2
        try:
            while group_exists(pgid) and time.monotonic() < deadline:
                time.sleep(0.02)
            remains = group_exists(pgid)
        except PermissionError:
            remains = True
        if not remains:
            last_error = None
        if last_error is not None or remains:
            raise RuntimeError("Owned subprocess group cleanup was not confirmed") from None
    return {"group_existed_after_wait": existed, "leader_joined": True, "group_gone": True}

def command(argv, cwd, env, log, seconds):
    start = time.monotonic()
    if LOCAL_DEADLINE is not None and LOCAL_DEADLINE - start < seconds + 5:
        raise TimeoutError('Insufficient remaining overall budget for the full child deadline and cleanup reserve')
    load = []
    p = None
    cleanup = True
    try:
        with log.open('wb') as f:
            p = subprocess.Popen(argv, cwd=cwd, env=env, stdout=f, stderr=subprocess.STDOUT, start_new_session=True)
            while p.poll() is None:
                load.append(os.getloadavg()[0])
                if time.monotonic() - start > seconds:
                    raise TimeoutError('owned command deadline')
                time.sleep(0.25)
            code = p.wait(timeout=1)
    finally:
        if p is not None:
            cleanup = stop_owned(p)['group_gone']
    result = {'command': argv, 'exit': code, 'seconds': time.monotonic() - start, 'load1_max': max(load, default=os.getloadavg()[0]), 'owned_group_gone_after_wait': cleanup, 'log_sha256': sha(log)}
    return result

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--main-repo', type=pathlib.Path, required=True)
    ap.add_argument('--sdk-repo', type=pathlib.Path, required=True)
    ap.add_argument('--out', type=pathlib.Path, required=True)
    a = ap.parse_args()
    out = a.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    env = {k: v for k, v in os.environ.items() if not k.startswith(('MQLITE_', 'MQLITEC_', 'TURSO_', 'LIBSQL_', 'M153_QUAL_'))}
    env.update(GOWORK='off', GOENV='off', GOFLAGS='', GOTOOLCHAIN='local', GOMAXPROCS='2', PYTHONDONTWRITEBYTECODE='1')
    results = []
    ret = {'passed': False, 'source_maps': {}, 'profile_legs': [], 'sustained': [], 'commands': results}
    before_harness = source(HERE / 'harness')
    dump(out / 'harness-source.json', before_harness)
    dump(out / 'plan.json', json.loads((HERE / 'plan.json').read_text()))
    work = None
    global LOCAL_DEADLINE
    LOCAL_DEADLINE = time.monotonic() + 1200
    previous_alarm = signal.signal(signal.SIGALRM, overall_deadline)
    signal.setitimer(signal.ITIMER_REAL, 1195)
    try:
        with tempfile.TemporaryDirectory(prefix='m153-receive-qual-') as tmp:
            work = pathlib.Path(tmp).resolve()
            prepared = {}
            for variant, (main_sha, sdk_sha) in SOURCES.items():
                main = work / (variant + '-main')
                sdk = work / (variant + '-sdk')
                maps = {'main': checkout(a.main_repo, main_sha, main), 'sdk': checkout(a.sdk_repo, sdk_sha, sdk)}
                assert maps['main'] and maps['sdk']
                ret['source_maps'][variant] = {'main_sha': main_sha, 'sdk_sha': sdk_sha, 'maps_before': maps}
                h = work / (variant + '-harness')
                shutil.copytree(HERE / 'harness', h)
                text = (h / 'go.mod').read_text()
                import re
                text = re.sub('(?m)^replace github.com/mqlitehq/mqlite => .+$', 'replace github.com/mqlitehq/mqlite => ' + str(main), text)
                text = re.sub('(?m)^replace github.com/mqlitehq/mqlite-go => .+$', 'replace github.com/mqlitehq/mqlite-go => ' + str(sdk), text)
                (h / 'go.mod').write_text(text)
                prepared[variant] = (main, sdk, h)
                c = command(['go', 'test', '-list', '^TestReceiveAcknowledgementQualification$', '.'], h, env, out / (variant + '-discovery.log'), 120)
                results.append(c)
                assert c['exit'] == 0 and 'TestReceiveAcknowledgementQualification' in (out / (variant + '-discovery.log')).read_text()
                c = command(['go', 'test', '-race', '-p=1', '-count=1', '-timeout=90s', '-run', '^TestReceiveQualificationRejectsLedgerFaults$', '-json', '.'], h, env, out / (variant + '-controls.log'), 120)
                results.append(c)
                assert c['exit'] == 0
            for i, variant in enumerate(['A', 'B', 'B', 'A', 'A', 'B', 'B', 'A']):
                h = prepared[variant][2]
                target = out / ('profile-%d-%s.json' % (i, variant))
                e = dict(env, M153_QUAL_STAGE='profiles', M153_QUAL_OUT=str(target))
                c = command(['go', 'test', '-p=1', '-count=1', '-timeout=120s', '-run', '^TestReceiveAcknowledgementQualification$', '-json', '.'], h, e, out / ('profile-%d-%s.log' % (i, variant)), 150)
                results.append(c)
                assert c['exit'] == 0 and c['owned_group_gone_after_wait']
                data = json.loads(target.read_text())
                assert data['passed'] and len(data['rows']) == 8 and all((x['message_count'] == x['batch'] * 260 and x['cycles'] == 260 and x['empty'] and x['deleted'] for x in data['rows']))
                ret['profile_legs'].append({'variant': variant, 'data': data, 'command': c})
                dump(out / 'progress.json', ret)
                print(json.dumps({'phase': 'profiles', 'leg': i, 'variant': variant, 'passed': True}), flush=True)
            qualified = max((x['command']['load1_max'] for x in ret['profile_legs'])) < 6
            timing = {}
            for kind in ['native', 'sdk']:
                for batch in [1, 64]:
                    for profile in ['plain', 'escaped']:
                        rows = {v: [r for leg in ret['profile_legs'] if leg['variant'] == v for r in leg['data']['rows'] if r['client'] == kind and r['batch'] == batch and (r['profile'] == profile)] for v in ['A', 'B']}
                        checks = {}
                        for key, limit in [('wall_ms', 1.1), ('receive_p95_ms', 1.2)]:
                            aa = [r[key] for r in rows['A']]
                            bb = [r[key] for r in rows['B']]
                            ratio = statistics.median(bb) / statistics.median(aa)
                            checks[key] = {'A': aa, 'B': bb, 'median_ratio_B_over_A': ratio, 'limit': limit, 'within': ratio <= limit}
                        for key in ['measured_allocated_bytes','measured_allocations']:
                            aa = [r[key] for r in rows['A']]
                            bb = [r[key] for r in rows['B']]
                            checks[key] = {'A':aa,'B':bb,'median_ratio_B_over_A':statistics.median(bb)/statistics.median(aa)}
                        timing[kind + '-' + str(batch) + '-' + profile] = checks
            ret['timing'] = {'qualified_host': qualified, 'profiles': timing}
            screen = qualified and all((c.get('within',True) for row in timing.values() for c in row.values()))
            dump(out / 'timing-screen.json', ret['timing'])
            assert screen, 'Frozen performance screen did not pass'
            for variant in ['A', 'B']:
                h = prepared[variant][2]
                target = out / ('sustained-' + variant + '.json')
                e = dict(env, M153_QUAL_STAGE='sustained', M153_QUAL_OUT=str(target))
                c = command(['go', 'test', '-p=1', '-count=1', '-timeout=240s', '-run', '^TestReceiveAcknowledgementQualification$', '-json', '.'], h, e, out / ('sustained-' + variant + '.log'), 270)
                results.append(c)
                assert c['exit'] == 0 and c['owned_group_gone_after_wait']
                data = json.loads(target.read_text())
                assert data['passed'] and data['elapsed_seconds'] >= 180 and (len(data['rows']) == 4)
                ret['sustained'].append({'variant': variant, 'data': data})
                dump(out / 'progress.json', ret)
                print(json.dumps({'phase': 'sustained', 'variant': variant, 'passed': True}), flush=True)
            for variant, (main, sdk, h) in prepared.items():
                before = ret['source_maps'][variant]['maps_before']
                after = {'main': source(main), 'sdk': source(sdk)}
                ret['source_maps'][variant]['maps_after_equal'] = before == after
                assert before == after
            assert source(HERE / 'harness') == before_harness
            ret['passed'] = True
    except BaseException as e:
        ret['passed'] = False
        ret['failure_type'] = type(e).__name__
        ret['failure_reason'] = str(e) if isinstance(e, (AssertionError, TimeoutError)) else 'Controlled execution failed; inspect retained source/logs'
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_alarm)
        ret['overall_budget_seconds'] = 1200
        ret['cleanup_reserve_seconds'] = 5
        ret['at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        ret['source_directories_removed'] = work is not None and (not work.exists())
        ret['harness_source_unchanged'] = source(HERE / 'harness') == before_harness
        ret['passed'] = ret['passed'] and ret['source_directories_removed'] and ret['harness_source_unchanged']
        ret['scope'] = 'Finite loopback Receive-path comparison and 2x180s paired clients with exact accounting; not remote DB capacity or final48h acceptance'
        dump(out / 'result.json', ret)
    return 0 if ret['passed'] else 1
if __name__ == '__main__':
    raise SystemExit(main())
