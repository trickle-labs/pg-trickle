"""Keep the old bootstrap's helper audit valid with exact keyless deltas (#1171)."""


def adapt_bootstrap(source, extension_version=None):
    if extension_version is not None:
        old = b"CREATE EXTENSION pg_trickle VERSION '0.108.3';"
        new = f"CREATE EXTENSION pg_trickle VERSION '{extension_version}';".encode()
        if source.count(old) != 1:
            raise ValueError("bootstrap pg_trickle version pin changed")
        source = source.replace(old, new)
    for total in (4, 3):
        old = (
            "    IF result->>'resolver_strategy' IS DISTINCT FROM 'full'\n"
            "       OR result->>'resolver_fallback_reason' IS DISTINCT FROM 'delta_full_invalidation'\n"
            f"       OR (result->>'affected_records')::bigint <> {total}\n"
        ).encode()
        new = (
            "    IF NOT (\n"
            "         (result->>'resolver_strategy' IS NOT DISTINCT FROM 'full'\n"
            "          AND result->>'resolver_fallback_reason' IS NOT DISTINCT FROM 'delta_full_invalidation'\n"
            f"          AND (result->>'affected_records')::bigint IS NOT DISTINCT FROM {total})\n"
            "         OR (result->>'resolver_strategy' IS NOT DISTINCT FROM 'affected'\n"
            "          AND result->>'resolver_fallback_reason' IS NULL\n"
            "          AND (result->>'affected_records')::bigint IS NOT DISTINCT FROM 1)\n"
            "       )\n"
        ).encode()
        if source.count(old) != 1:
            raise ValueError(f"bootstrap assertion for {total} records changed")
        source = source.replace(old, new)
    return source
