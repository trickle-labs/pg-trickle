-- pg_trickle 0.87.14 -> 0.87.15
-- Add the versioned row-identity V2 foundation API.

CREATE OR REPLACE FUNCTION pgtrickle."encode_row_id_v2"(
    domain text,
    record anyelement
) RETURNS bytea
STRICT STABLE PARALLEL SAFE
LANGUAGE c
AS 'MODULE_PATHNAME', 'encode_row_id_v2_wrapper';

CREATE OR REPLACE FUNCTION pgtrickle."row_probe_v1"(
    input bytea
) RETURNS bytea
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c
AS 'MODULE_PATHNAME', 'row_probe_v1_wrapper';
