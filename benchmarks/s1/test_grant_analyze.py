"""Reproduce the retained GRANT/ANALYZE/SHARE cycle and check the proposed order."""
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

name = 'issue1168-grant-' + uuid.uuid4().hex[:12]
image = 'postgres:18.4-bookworm@sha256:efef99e1558f86089bc84bece29208c0777a185ff717ec7fa288a652ce2d0adf'
results = []

def sql(query, *, background=False):
    args = ['docker', 'exec', name, 'psql', '-XAtq', '-h', '127.0.0.1', '-U', 'postgres', '-v', 'ON_ERROR_STOP=1', '-c', query]
    return (subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if background else subprocess.run(args, capture_output=True, text=True, timeout=20))

try:
    subprocess.run(['docker', 'run', '--detach', '--name', name, '--network', 'none',
                    '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', image], check=True, capture_output=True)
    for _ in range(60):
        if sql('SELECT 1').returncode == 0:
            break
        time.sleep(1)
    else:
        raise AssertionError('PostgreSQL did not start')
    setup = sql('CREATE ROLE grant_reader; CREATE TABLE grant_probe (id int); '
                'INSERT INTO grant_probe SELECT generate_series(1, 1000); '
                'GRANT SELECT ON grant_probe TO grant_reader')
    assert setup.returncode == 0, setup.stderr
    for label, grant, expect_deadlock in [
        ('repeated-grant', 'GRANT SELECT ON grant_probe TO grant_reader', True),
        ('already-authorized', "DO $$ BEGIN IF NOT has_table_privilege('grant_reader', 'grant_probe', 'SELECT') THEN LOCK TABLE grant_probe IN SHARE MODE; GRANT SELECT ON grant_probe TO grant_reader; END IF; END $$", False),
        ('missing-grant', "DO $$ BEGIN IF NOT has_table_privilege('grant_reader', 'grant_probe', 'SELECT') THEN LOCK TABLE grant_probe IN SHARE MODE; GRANT SELECT ON grant_probe TO grant_reader; END IF; END $$", False),
    ]:
        if label == 'missing-grant':
            assert sql('REVOKE SELECT ON grant_probe FROM grant_reader').returncode == 0
        xmin_before = sql("SELECT xmin::text FROM pg_class WHERE oid='grant_probe'::regclass").stdout.strip()
        first = sql("SET application_name='grant-cycle'; SET deadlock_timeout='100ms'; BEGIN; "
                    + grant + '; SELECT pg_sleep(2); LOCK TABLE grant_probe IN SHARE MODE; COMMIT', background=True)
        for _ in range(100):
            if sql("SELECT wait_event FROM pg_stat_activity WHERE application_name='grant-cycle'").stdout.strip() == 'PgSleep':
                break
            time.sleep(.02)
        else:
            raise AssertionError('GRANT barrier not reached')
        second = sql("SET application_name='analyze-cycle'; SET deadlock_timeout='100ms'; ANALYZE grant_probe", background=True)
        out1, err1 = first.communicate(timeout=15)
        out2, err2 = second.communicate(timeout=15)
        deadlock = 'deadlock detected' in err1 + err2
        xmin_after = sql("SELECT xmin::text FROM pg_class WHERE oid='grant_probe'::regclass").stdout.strip()
        results.append(dict(label=label, deadlock=deadlock, first_exit=first.returncode,
                            second_exit=second.returncode, stderr=err1 + err2,
                            xmin_before=xmin_before, xmin_after=xmin_after))
        assert deadlock == expect_deadlock, results[-1]
        if not expect_deadlock:
            assert first.returncode == second.returncode == 0, results[-1]
    revoked = sql("REVOKE SELECT ON grant_probe FROM grant_reader; DO $$ BEGIN IF NOT has_table_privilege('grant_reader', 'grant_probe', 'SELECT') THEN LOCK TABLE grant_probe IN SHARE MODE; GRANT SELECT ON grant_probe TO grant_reader; END IF; END $$; SELECT has_table_privilege('grant_reader', 'grant_probe', 'SELECT')")
    assert revoked.returncode == 0 and revoked.stdout.strip() == 't', revoked.stderr
    print('GRANT/ANALYZE cycle reproduced; idempotent grant and missing-grant repair: PASS')
finally:
    output = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).with_name('grant-analyze-results.json')
    output.write_text(json.dumps(results, indent=2) + '\n')
    subprocess.run(['docker', 'rm', '-f', name], capture_output=True, check=True)
