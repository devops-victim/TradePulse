ALTER TABLE trades ADD COLUMN source VARCHAR(20) NOT NULL DEFAULT 'external_bse';
ALTER TABLE trades ADD CONSTRAINT trades_source_check CHECK (source IN ('external_bse','legacy_import'));
