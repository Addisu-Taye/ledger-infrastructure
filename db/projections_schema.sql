-- Application Summary Projection
CREATE TABLE IF NOT EXISTS application_summary (
    application_id TEXT PRIMARY KEY,
    state TEXT NOT NULL DEFAULT 'SUBMITTED',
    applicant_id TEXT,
    requested_amount_usd NUMERIC,
    approved_amount_usd NUMERIC,
    risk_tier TEXT,
    fraud_score NUMERIC,
    compliance_status TEXT DEFAULT 'PENDING',
    decision TEXT,
    agent_sessions_completed TEXT[] DEFAULT '{}',
    last_event_type TEXT,
    last_event_at TIMESTAMPTZ,
    human_reviewer_id TEXT,
    final_decision_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_app_summary_state ON application_summary(state);
CREATE INDEX idx_app_summary_decision ON application_summary(decision);

-- Agent Performance Projection
CREATE TABLE IF NOT EXISTS agent_performance (
    agent_id TEXT NOT NULL,
    model_version TEXT NOT NULL,
    analyses_completed INTEGER DEFAULT 0,
    decisions_generated INTEGER DEFAULT 0,
    avg_confidence_score NUMERIC,
    avg_duration_ms NUMERIC,
    approve_rate NUMERIC DEFAULT 0,
    decline_rate NUMERIC DEFAULT 0,
    refer_rate NUMERIC DEFAULT 0,
    human_override_rate NUMERIC DEFAULT 0,
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (agent_id, model_version)
);

-- Compliance Audit Projection
CREATE TABLE IF NOT EXISTS compliance_audit (
    application_id TEXT PRIMARY KEY,
    regulation_set_version TEXT NOT NULL,
    overall_status TEXT DEFAULT 'PENDING',
    last_updated_at TIMESTAMPTZ,
    snapshot_position BIGINT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Compliance Checks (supporting table for temporal queries)
CREATE TABLE IF NOT EXISTS compliance_checks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id TEXT NOT NULL REFERENCES compliance_audit(application_id) ON DELETE CASCADE,
    rule_id TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PASSED', 'FAILED')),
    evaluation_timestamp TIMESTAMPTZ NOT NULL,
    evidence_hash TEXT,
    failure_reason TEXT,
    remediation_required BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_compliance_checks_app ON compliance_checks(application_id);
CREATE INDEX idx_compliance_checks_timestamp ON compliance_checks(evaluation_timestamp);
