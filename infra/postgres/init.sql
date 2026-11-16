-- Simulated Fintech OLTP Schema & Seed Data (with intentional anomalies for Data Quality testing)

CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR(32) PRIMARY KEY,
    customer_name VARCHAR(120) NOT NULL,
    account_tier VARCHAR(32) NOT NULL,
    kyc_status VARCHAR(32) NOT NULL,
    country_code CHAR(2) NOT NULL,
    risk_segment VARCHAR(24) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    id BIGSERIAL PRIMARY KEY,
    tx_id VARCHAR(64) NOT NULL,
    account_id VARCHAR(32),
    merchant_category VARCHAR(64) NOT NULL,
    tx_type VARCHAR(32) NOT NULL,
    currency VARCHAR(8) NOT NULL,
    amount NUMERIC(18, 4) NOT NULL,
    status VARCHAR(32) NOT NULL,
    tx_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_ts ON transactions (tx_timestamp);

INSERT INTO accounts (account_id, customer_name, account_tier, kyc_status, country_code, risk_segment, updated_at)
VALUES
    ('ACC-1001', 'Grupo Industrial Bajio SA', 'ENTERPRISE', 'VERIFIED', 'MX', 'LOW', NOW() - INTERVAL '10 days'),
    ('ACC-1002', 'Fintech Horizon LLC', 'GROWTH', 'VERIFIED', 'US', 'MEDIUM', NOW() - INTERVAL '8 days'),
    ('ACC-1003', 'Importadora Queretaro', 'STANDARD', 'VERIFIED', 'MX', 'LOW', NOW() - INTERVAL '5 days'),
    ('ACC-1004', 'EuroTrade Logistics GmbH', 'ENTERPRISE', 'PENDING_REVIEW', 'DE', 'HIGH', NOW() - INTERVAL '3 days'),
    ('ACC-1005', 'Pacific Treasury Corp', 'GROWTH', 'VERIFIED', 'CA', 'LOW', NOW() - INTERVAL '1 day')
ON CONFLICT (account_id) DO NOTHING;

-- Seed multi-currency transactions across recent days, including:
-- 1) Valid transactions across USD, MXN, EUR, GBP, CAD
-- 2) Intentional duplicate tx_id (tests Silver deduplication)
-- 3) Intentional corrupted rows (negative amount, invalid currency 'XXX', null account_id) for Quarantine routing
INSERT INTO transactions (tx_id, account_id, merchant_category, tx_type, currency, amount, status, tx_timestamp)
VALUES
    ('TX-90001', 'ACC-1001', 'CLOUD_INFRA', 'DEBIT', 'USD', 1450.50, 'COMPLETED', DATE_TRUNC('day', NOW()) - INTERVAL '1 day' + INTERVAL '09 hours'),
    ('TX-90002', 'ACC-1001', 'SUPPLIER_SETTLEMENT', 'TRANSFER', 'MXN', 48500.00, 'COMPLETED', DATE_TRUNC('day', NOW()) - INTERVAL '1 day' + INTERVAL '11 hours'),
    ('TX-90003', 'ACC-1002', 'FX_DESK', 'FX_SETTLEMENT', 'EUR', 3200.75, 'COMPLETED', DATE_TRUNC('day', NOW()) - INTERVAL '1 day' + INTERVAL '13 hours'),
    ('TX-90004', 'ACC-1003', 'LOGISTICS', 'DEBIT', 'MXN', 12950.25, 'COMPLETED', DATE_TRUNC('day', NOW()) - INTERVAL '1 day' + INTERVAL '15 hours'),
    ('TX-90005', 'ACC-1004', 'TREASURY', 'CREDIT', 'GBP', 2100.00, 'PENDING', DATE_TRUNC('day', NOW()) - INTERVAL '1 day' + INTERVAL '16 hours'),
    ('TX-90006', 'ACC-1005', 'SAAS_BILLING', 'DEBIT', 'CAD', 890.40, 'COMPLETED', DATE_TRUNC('day', NOW()) - INTERVAL '1 day' + INTERVAL '18 hours'),
    ('TX-90007', 'ACC-1002', 'PAYROLL', 'TRANSFER', 'USD', 6400.00, 'COMPLETED', DATE_TRUNC('day', NOW()) - INTERVAL '1 day' + INTERVAL '19 hours'),
    -- Intentional duplicate event emitted by upstream retry
    ('TX-90002', 'ACC-1001', 'SUPPLIER_SETTLEMENT', 'TRANSFER', 'MXN', 48500.00, 'COMPLETED', DATE_TRUNC('day', NOW()) - INTERVAL '1 day' + INTERVAL '11 hours 5 minutes'),
    -- Intentional bad record #1: negative amount -> routed to Quarantine
    ('TX-90008', 'ACC-1003', 'REFUND_GLITCH', 'DEBIT', 'USD', -250.00, 'FAILED', DATE_TRUNC('day', NOW()) - INTERVAL '1 day' + INTERVAL '20 hours'),
    -- Intentional bad record #2: invalid currency -> routed to Quarantine
    ('TX-90009', 'ACC-1004', 'UNKNOWN_VENDOR', 'DEBIT', 'XXX', 100.00, 'COMPLETED', DATE_TRUNC('day', NOW()) - INTERVAL '1 day' + INTERVAL '21 hours'),
    -- Today's batch (ensures manual "Trigger DAG" on current date processes live rows + anomalies)
    ('TX-91001', 'ACC-1001', 'CLOUD_INFRA', 'DEBIT', 'USD', 2150.00, 'COMPLETED', DATE_TRUNC('day', NOW()) + INTERVAL '01 hours'),
    ('TX-91002', 'ACC-1002', 'FX_DESK', 'FX_SETTLEMENT', 'EUR', 1800.50, 'COMPLETED', DATE_TRUNC('day', NOW()) + INTERVAL '02 hours'),
    ('TX-91003', 'ACC-1003', 'SUPPLIER_SETTLEMENT', 'TRANSFER', 'MXN', 34200.00, 'COMPLETED', DATE_TRUNC('day', NOW()) + INTERVAL '03 hours'),
    ('TX-91004', 'ACC-1004', 'TREASURY', 'CREDIT', 'GBP', 950.00, 'COMPLETED', DATE_TRUNC('day', NOW()) + INTERVAL '04 hours'),
    ('TX-91005', 'ACC-1005', 'SAAS_BILLING', 'DEBIT', 'CAD', 1240.00, 'COMPLETED', DATE_TRUNC('day', NOW()) + INTERVAL '05 hours'),
    ('TX-91003', 'ACC-1003', 'SUPPLIER_SETTLEMENT', 'TRANSFER', 'MXN', 34200.00, 'COMPLETED', DATE_TRUNC('day', NOW()) + INTERVAL '03 hours 5 minutes'),
    ('TX-91006', 'ACC-1003', 'REFUND_GLITCH', 'DEBIT', 'USD', -75.00, 'FAILED', DATE_TRUNC('day', NOW()) + INTERVAL '06 hours'),
    ('TX-91007', 'ACC-1004', 'UNKNOWN_VENDOR', 'DEBIT', 'XXX', 420.00, 'COMPLETED', DATE_TRUNC('day', NOW()) + INTERVAL '07 hours');
