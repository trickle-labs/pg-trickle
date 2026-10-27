"""The compatibility adapter must preserve all unrelated fixture bytes."""

import importlib.util
from pathlib import Path

module = Path(__file__).parent / "qualification/tests/integrations/pg-mdm/time/bootstrap_compat.py"
spec = importlib.util.spec_from_file_location("bootstrap_compat", module)
compat = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compat)
source = b"before\n"
for total in (4, 3):
    source += (
        "    IF result->>'resolver_strategy' IS DISTINCT FROM 'full'\n"
        "       OR result->>'resolver_fallback_reason' IS DISTINCT FROM 'delta_full_invalidation'\n"
        f"       OR (result->>'affected_records')::bigint <> {total}\n"
        "       OR helper_audit_failed THEN RAISE EXCEPTION 'helper changed'; END IF;\n"
    ).encode()
source += b"after\n"
adapted = compat.adapt_bootstrap(source)
assert adapted.startswith(b"before\n") and adapted.endswith(b"after\n")
assert adapted.count(b"OR helper_audit_failed THEN RAISE EXCEPTION 'helper changed'; END IF;") == 2
assert adapted.count(b"AND result->>'resolver_fallback_reason' IS NULL") == 2
assert adapted.count(b"IS NOT DISTINCT FROM 1)") == 2
assert b"IS NOT DISTINCT FROM 4)" in adapted and b"IS NOT DISTINCT FROM 3)" in adapted
for invalid in (b"", source + source, adapted):
    try:
        compat.adapt_bootstrap(invalid)
    except ValueError:
        pass
    else:
        raise AssertionError("changed or repeated bootstrap must fail closed")

versioned = b"CREATE EXTENSION pg_trickle VERSION '0.108.3';\n" + source
adapted_versioned = compat.adapt_bootstrap(versioned, "0.108.4")
assert b"CREATE EXTENSION pg_trickle VERSION '0.108.4';" in adapted_versioned
try:
    compat.adapt_bootstrap(versioned + versioned, "0.108.4")
except ValueError:
    pass
else:
    raise AssertionError("repeated pg_trickle version pin must fail closed")
print("bootstrap exact-delta compatibility checks: PASS")
