-- Task06-B System B Theme Trend Rank results and component audit facts.

CREATE TABLE IF NOT EXISTS system_b_theme_rank_snapshot (
    trade_date DATE NOT NULL,
    theme_id VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL,
    rank_eligible BOOLEAN NOT NULL,
    rank_eligibility_reason VARCHAR NOT NULL,
    trend_strength_score DOUBLE,
    trend_persistence_score DOUBLE,
    current_structure_score DOUBLE,
    popularity_support_score DOUBLE,
    theme_raw_score DOUBLE,
    theme_rank DOUBLE,
    theme_score DOUBLE,
    theme_status VARCHAR NOT NULL,
    theme_universe_size INTEGER NOT NULL,
    input_provenance VARCHAR NOT NULL,
    diagnostics VARCHAR NOT NULL,
    evidence VARCHAR NOT NULL,
    production_run_id VARCHAR NOT NULL,
    calculation_version VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (trade_date, theme_id)
);

CREATE TABLE IF NOT EXISTS system_b_theme_rank_component_audit (
    trade_date DATE NOT NULL,
    theme_id VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL,
    dimension VARCHAR NOT NULL,
    component VARCHAR NOT NULL,
    raw_value DOUBLE,
    direction VARCHAR NOT NULL,
    raw_rank DOUBLE,
    normalized_rank_score DOUBLE,
    base_weight DOUBLE NOT NULL,
    effective_weight DOUBLE,
    weighted_contribution DOUBLE,
    universe_size INTEGER NOT NULL,
    tie_count INTEGER NOT NULL,
    status VARCHAR NOT NULL,
    source_provenance VARCHAR NOT NULL,
    metadata_json VARCHAR NOT NULL,
    production_run_id VARCHAR NOT NULL,
    calculation_version VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (trade_date, theme_id, dimension, component)
);
