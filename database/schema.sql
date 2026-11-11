
CREATE TABLE hcpcs_codes (
    id BIGSERIAL PRIMARY KEY,
    hcpcs_code VARCHAR(10) NOT NULL,
    group_code VARCHAR(5) NOT NULL,
    category_name VARCHAR(255),
    long_description TEXT NOT NULL,
    description_hash CHAR(32) NOT NULL,
    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    version INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE UNIQUE INDEX uq_hcpcs_current_code
ON hcpcs_codes (hcpcs_code)
WHERE is_current = TRUE;

CREATE INDEX idx_hcpcs_code
ON hcpcs_codes (hcpcs_code);

CREATE INDEX idx_hcpcs_current
ON hcpcs_codes (is_current);

