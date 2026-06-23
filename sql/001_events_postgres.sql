CREATE TABLE IF NOT EXISTS events (
    event_id bigint PRIMARY KEY,
    category_seq bigint,
    gu_seq bigint,
    event_name text,
    period text,
    place text,
    org_name text,
    use_target text,
    ticket_price text,
    inqury_number text,
    player text,
    describe text,
    etc_desc text,
    homepage_link text,
    main_img text,
    reg_date timestamptz,
    is_public boolean,
    start_date timestamptz,
    end_date timestamptz,
    theme text,
    latitude double precision,
    longitude double precision,
    is_free boolean,
    detail_url text,
    display_time text,
    geohash text
);

CREATE INDEX IF NOT EXISTS events_start_date_event_id_idx
    ON events (start_date DESC, event_id DESC);

CREATE INDEX IF NOT EXISTS events_category_start_date_idx
    ON events (category_seq, start_date DESC, event_id DESC);

CREATE INDEX IF NOT EXISTS events_gu_start_date_idx
    ON events (gu_seq, start_date DESC, event_id DESC);

CREATE INDEX IF NOT EXISTS events_geohash_prefix_idx
    ON events (geohash text_pattern_ops);
