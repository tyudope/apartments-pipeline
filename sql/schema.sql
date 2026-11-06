-- Gold layer: a small star schema in Postgres.
-- Grain of fact_listing: one row per (listing id, month it was observed).
-- The same flat listed in three months is three observations of the market,
-- not a duplicate; v_listing_latest collapses to one row per flat for pricing.

CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dim_city (
    city_key    text PRIMARY KEY,      -- as in source: lowercase, no diacritics
    city_name   text NOT NULL,         -- display name
    voivodeship text NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_month (
    month_key   text PRIMARY KEY,      -- 'YYYY-MM'
    year        smallint NOT NULL,
    month       smallint NOT NULL,
    quarter     smallint NOT NULL,
    first_day   date NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.fact_listing (
    id                  text NOT NULL,
    month_key           text NOT NULL REFERENCES dw.dim_month(month_key),
    city_key            text NOT NULL REFERENCES dw.dim_city(city_key),
    building_type       text,
    ownership           text,
    building_material   text,
    condition           text,
    area_m2             numeric(7,2) NOT NULL,
    rooms               smallint,
    floor               smallint,
    floor_count         smallint,
    build_year          smallint,
    latitude            double precision,
    longitude           double precision,
    centre_distance_km  numeric(5,2),
    poi_count           smallint,
    has_parking         boolean,
    has_balcony         boolean,
    has_elevator        boolean,
    has_security        boolean,
    has_storage         boolean,
    price               integer NOT NULL,
    price_per_m2        numeric(9,2) NOT NULL,
    loaded_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, month_key)        -- natural key; load upserts on it
);
CREATE INDEX IF NOT EXISTS ix_fact_listing_city_month ON dw.fact_listing (city_key, month_key);

-- Rows the transform refused, with the reason. Replaced per month on load.
CREATE TABLE IF NOT EXISTS dw.rejects (
    id            text,
    month_key     text NOT NULL,
    city_key      text,
    reject_reason text NOT NULL,
    area_m2       numeric(7,2),
    rooms         smallint,
    price         integer,
    price_per_m2  numeric(9,2),
    loaded_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rejects_month ON dw.rejects (month_key);

-- Every pipeline step and every quality check writes one row here.
CREATE TABLE IF NOT EXISTS dw.run_log (
    run_id    bigserial PRIMARY KEY,
    month_key text NOT NULL,
    step      text NOT NULL,           -- extract / transform / load / check:<name>
    passed    boolean NOT NULL,
    value     text,
    ran_at    timestamptz NOT NULL DEFAULT now()
);

-- One row per flat: the last month it was observed. Used for pricing questions.
CREATE OR REPLACE VIEW dw.v_listing_latest AS
SELECT DISTINCT ON (id) *
FROM dw.fact_listing
ORDER BY id, month_key DESC;

INSERT INTO dw.dim_city (city_key, city_name, voivodeship) VALUES
    ('warszawa',    'Warszawa',    'mazowieckie'),
    ('krakow',      'Kraków',      'małopolskie'),
    ('wroclaw',     'Wrocław',     'dolnośląskie'),
    ('gdansk',      'Gdańsk',      'pomorskie'),
    ('gdynia',      'Gdynia',      'pomorskie'),
    ('poznan',      'Poznań',      'wielkopolskie'),
    ('lodz',        'Łódź',        'łódzkie'),
    ('szczecin',    'Szczecin',    'zachodniopomorskie'),
    ('bydgoszcz',   'Bydgoszcz',   'kujawsko-pomorskie'),
    ('lublin',      'Lublin',      'lubelskie'),
    ('katowice',    'Katowice',    'śląskie'),
    ('bialystok',   'Białystok',   'podlaskie'),
    ('czestochowa', 'Częstochowa', 'śląskie'),
    ('radom',       'Radom',       'mazowieckie'),
    ('rzeszow',     'Rzeszów',     'podkarpackie')
ON CONFLICT (city_key) DO NOTHING;