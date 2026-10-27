"""Check the actual MDM binary's repeat-grant and missing-grant behavior."""
import hashlib
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path('benchmarks/s1/qualification/tests/integrations/pg-mdm/time').resolve()))
from bootstrap_compat import adapt_bootstrap

image, output_name = sys.argv[1:]
output = Path(output_name)
output.mkdir(exist_ok=False)
container = 'issue1168-mdm-grants-' + uuid.uuid4().hex[:12]

def sql(query, database='foundation'):
    result = subprocess.run(['docker', 'exec', '-i', container, 'psql', '-XAtq', '-h', '127.0.0.1',
                             '-U', 'postgres', '-d', database, '-v', 'ON_ERROR_STOP=1'],
                            input=query, capture_output=True, text=True, timeout=180)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout.strip()

try:
    subprocess.run(['docker', 'run', '--detach', '--name', container, '--network', 'none',
                    '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', image, 'postgres',
                    '-c', 'shared_preload_libraries=pg_trickle,pg_react', '-c', 'pg_trickle.enabled=off',
                    '-c', 'pg_trickle.differential_max_change_ratio=1', '-c', 'max_locks_per_transaction=256',
                    '-c', 'autovacuum=off'], capture_output=True, check=True)
    for _ in range(60):
        result = subprocess.run(['docker', 'exec', container, 'pg_isready', '-h', '127.0.0.1', '-U', 'postgres'], capture_output=True)
        if result.returncode == 0:
            break
        time.sleep(1)
    else:
        raise AssertionError('PostgreSQL did not start')
    original = subprocess.check_output(['docker', 'exec', container, 'cat', '/tests/e2e.sql'])
    assert hashlib.sha256(original).hexdigest() == 'b72d8aa270369db31dfc2333c120ea6873eaa6d15fad0ac3398800c9b9d607f0'
    version = sql("SELECT default_version FROM pg_available_extensions WHERE name = 'pg_trickle'", 'postgres')
    (output / 'bootstrap.log').write_text(sql(adapt_bootstrap(original, version).decode(), 'postgres'))
    refresh = "SET SESSION AUTHORIZATION mdm_legacy_login; SET ROLE mdm_legacy_administrator; SELECT mdm.refresh('policy_qualification','ALLOW'); RESET ROLE; RESET SESSION AUTHORIZATION;"
    sql(refresh)
    snapshot = "SELECT jsonb_object_agg(c.oid::text,c.xmin::text) FROM pg_class c WHERE c.oid IN (SELECT relation_oid FROM mdm_internal.graph_members)"
    before = json.loads(sql(snapshot))
    result = json.loads(sql(refresh))
    after = json.loads(sql(snapshot))
    (output / 'repeat-grant.json').write_text(json.dumps(dict(before=before, after=after, refresh=result), indent=2) + '\n')
    assert before == after, 'repeat refresh rewrote graph storage catalog rows'
    relation = sql("SELECT m.relation_name FROM mdm_internal.graph_members m JOIN mdm_internal.graph_bindings b ON b.graph_binding_id=m.graph_binding_id JOIN mdm_internal.entities e ON e.entity_id=b.entity_id WHERE e.entity_name='policy_qualification' AND b.execution_role_oid='mdm_legacy_administrator'::regrole ORDER BY b.graph_generation DESC,m.topological_ordinal LIMIT 1")
    # The relation name is quoted by MDM from its catalog OIDs.
    sql(f'REVOKE SELECT ON {relation} FROM mdm_helper_owner')
    literal = relation.replace("'", "''")
    assert sql(f"SELECT has_table_privilege('mdm_helper_owner','{literal}','SELECT')") == 'f'
    sql(refresh)
    assert sql(f"SELECT has_table_privilege('mdm_helper_owner','{literal}','SELECT')") == 't'
    sql(Path(__file__).with_name('jit_scope.sql').read_text())
    (output / 'result.json').write_text(json.dumps(dict(image=image, repeated_grants=0, missing_select_restored=True, jit_scope_verified=True), indent=2) + '\n')
    print('actual MDM grants and candidate JIT-scope regression: PASS')
finally:
    logs = subprocess.run(['docker', 'logs', container], capture_output=True, text=True)
    (output / 'postgres.log').write_text(logs.stdout + logs.stderr)
    subprocess.run(['docker', 'rm', '-f', container], capture_output=True, check=True)
