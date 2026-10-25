\set ON_ERROR_STOP on
SET TIME ZONE 'UTC';

-- Fixture-only source. It is intentionally not the live mdm_steward namespace.
CREATE SCHEMA IF NOT EXISTS mdm_fixture;
DROP TABLE IF EXISTS mdm_fixture.policy_cases_v1;
CREATE TABLE mdm_fixture.policy_cases_v1 (
    case_key bigint PRIMARY KEY,
    review_id uuid UNIQUE NOT NULL,
    entity_name name NOT NULL,
    issue_key bytea NOT NULL,
    occurrence integer NOT NULL,
    status text NOT NULL,
    severity text NOT NULL,
    reason_code text NOT NULL,
    approved_metadata jsonb NOT NULL,
    permitted_actions text[] NOT NULL,
    assigned_queue name,
    due_at timestamptz,
    escalation_level integer NOT NULL,
    manual_assignment_protected boolean NOT NULL,
    opened_at timestamptz,
    opened_at_source text NOT NULL,
    resolved_at timestamptz,
    review_version bigint NOT NULL,
    definition_version bigint NOT NULL,
    publication_revision bigint NOT NULL,
    stewardship_epoch bigint NOT NULL,
    evidence_basis_digest bytea NOT NULL,
    action_revision bigint NOT NULL,
    pending_stewardship boolean NOT NULL,
    last_observed_at timestamptz NOT NULL,
    UNIQUE (entity_name, issue_key, occurrence)
);

INSERT INTO mdm_fixture.policy_cases_v1 (
    case_key, review_id, entity_name, issue_key, occurrence, status, severity,
    reason_code, approved_metadata, permitted_actions, assigned_queue, due_at,
    escalation_level, manual_assignment_protected, opened_at, opened_at_source,
    resolved_at, review_version, definition_version, publication_revision,
    stewardship_epoch, evidence_basis_digest, action_revision,
    pending_stewardship, last_observed_at)
VALUES
    (1, '40000000-0000-4000-8000-000000000001', 'customer', decode(repeat('11', 32), 'hex'), 1,
     'open', 'high', 'POSSIBLE_DUPLICATE', '{}', ARRAY['ASSIGN_QUEUE', 'SET_DUE_AT']::text[], 'general', NULL,
     0, false, '2026-09-21 08:00:00+00', 'publication', NULL, 1, 3, 5, 8,
     decode(repeat('22', 32), 'hex'), 2, false, '2026-09-21 08:01:00+00'),
    (2, '40000000-0000-4000-8000-000000000002', 'customer', decode(repeat('33', 32), 'hex'), 1,
     'open', 'medium', 'TIED_ROUTE', '{}', ARRAY['ASSIGN_QUEUE']::text[], NULL, NULL,
     0, false, '2026-09-21 08:00:00+00', 'publication', NULL, 1, 3, 5, 8,
     decode(repeat('44', 32), 'hex'), 2, false, '2026-09-21 08:01:00+00'),
    (3, '40000000-0000-4000-8000-000000000003', 'customer', decode(repeat('55', 32), 'hex'), 1,
     'open', 'low', 'UNMATCHED', '{}', ARRAY['ASSIGN_QUEUE']::text[], NULL, NULL,
     0, false, '2026-09-21 08:00:00+00', 'publication', NULL, 1, 3, 5, 8,
     decode(repeat('66', 32), 'hex'), 2, false, '2026-09-21 08:01:00+00'),
    (4, '40000000-0000-4000-8000-000000000004', 'customer', decode(repeat('77', 32), 'hex'), 1,
     'open', 'high', 'MANUAL_ROUTE', '{}', ARRAY['ASSIGN_QUEUE']::text[], 'manual', NULL,
     0, true, '2026-09-21 08:00:00+00', 'publication', NULL, 1, 3, 5, 8,
     decode(repeat('88', 32), 'hex'), 2, false, '2026-09-21 08:01:00+00'),
    (5, '40000000-0000-4000-8000-000000000005', 'customer', decode(repeat('99', 32), 'hex'), 1,
     'open', 'high', 'DEADLINE_ONLY', '{}', ARRAY['SET_DUE_AT']::text[], NULL, NULL,
     0, false, NULL, 'unknown', NULL, 1, 3, 5, 8,
     decode(repeat('aa', 32), 'hex'), 2, false, '2026-09-21 08:01:00+00'),
    (6, '40000000-0000-4000-8000-000000000006', 'customer', decode(repeat('bb', 32), 'hex'), 1,
     'open', 'high', 'EXISTING_DEADLINE', '{}', ARRAY['SET_DUE_AT']::text[], NULL,
     '2026-09-21 10:00:00+00', 0, false, '2026-09-21 08:00:00+00', 'publication', NULL, 1, 3, 5, 8,
     decode(repeat('cc', 32), 'hex'), 2, false, '2026-09-21 08:01:00+00'),
    (7, '40000000-0000-4000-8000-000000000007', 'customer', decode(repeat('dd', 32), 'hex'), 1,
     'open', 'high', 'POSSIBLE_DUPLICATE', '{}', ARRAY['ASSIGN_QUEUE']::text[], 'priority', NULL,
     0, false, '2026-09-21 08:00:00+00', 'administrator', NULL, 1, 3, 5, 8,
     decode(repeat('ee', 32), 'hex'), 2, false, '2026-09-21 08:01:00+00');

SELECT 'v0.47 fixture loaded' AS result;
