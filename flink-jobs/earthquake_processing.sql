-- =============================================================================
--  Flink SQL — Pipeline séismes temps réel (Option A : enrichissement)
--
--  Architecture :
--    Kafka (earthquake-raw)
--      ├── JOB 1 → Kafka (earthquake-enriched)  [enrichissement, SANS fenêtre]
--      └── JOB 2 → Kafka (earthquake-alerts)    [alertes,        SANS fenêtre]
--
--  Pourquoi pas de fenêtre TUMBLE ?
--    Les séismes sont rares (~1 toutes les 15-30 min globalement).
--    Une TUMBLE de 60s produirait des fenêtres vides → rien dans Kafka.
--    Sans fenêtre, chaque événement produit IMMÉDIATEMENT une sortie.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Table SOURCE : lit les données brutes depuis le topic earthquake-raw
--
-- PROCTIME() : horloge système Flink (temps de traitement).
-- scan.startup.mode = 'earliest-offset' : lit depuis le début du topic.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS earthquake_raw (
    event_id     STRING,
    magnitude    DOUBLE,
    mag_type     STRING,
    place        STRING,
    event_time   STRING,
    latitude     DOUBLE,
    longitude    DOUBLE,
    depth_km     DOUBLE,
    significance INT,
    tsunami      INT,
    status       STRING,
    type         STRING,
    title        STRING,
    ingested_at  STRING,
    proc_time    AS PROCTIME()
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'earthquake-raw',
    'properties.bootstrap.servers' = 'kafka:29092',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'json'
);
    
-- ---------------------------------------------------------------------------
-- Table SINK 1 : séismes enrichis → topic earthquake-enriched
--
-- Champs ajoutés par Flink :
--   depth_category  : classification sismologique (Superficiel / Intermédiaire / Profond)
--   severity        : niveau de danger (Mineur → Dévastateur / TSUNAMI)
--   is_significant  : 1 si significance > 500 (seuil USGS)
--   processed_at    : timestamp de traitement Flink
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS earthquake_enriched (
    event_id        STRING,
    magnitude       DOUBLE,
    mag_type        STRING,
    place           STRING,
    event_time      STRING,
    latitude        DOUBLE,
    longitude       DOUBLE,
    depth_km        DOUBLE,
    significance    INT,
    tsunami         INT,
    status          STRING,
    title           STRING,
    depth_category  STRING,
    severity        STRING,
    is_significant  INT,
    processed_at    STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'earthquake-enriched',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format'                       = 'json'
);

-- ---------------------------------------------------------------------------
-- Table SINK 2 : alertes temps réel → topic earthquake-alerts
-- ---------------------------------------------------------------------------
SET 'pipeline.name' = 'earthquake-enrichment';

INSERT INTO earthquake_enriched
SELECT
    event_id,
    magnitude,
    mag_type,
    place,
    event_time,
    latitude,
    longitude,
    depth_km,
    significance,
    tsunami,
    status,
    title,

    -- Catégorie de profondeur (classification sismologique standard)
    CASE
        WHEN depth_km < 70  THEN 'Superficiel (0-70 km)'
        WHEN depth_km < 300 THEN 'Intermédiaire (70-300 km)'
        ELSE                     'Profond (300+ km)'
    END AS depth_category,

    -- Niveau de sévérité (tsunami prioritaire sur la magnitude)
    CASE
        WHEN tsunami   = 1   THEN 'TSUNAMI'
        WHEN magnitude >= 8.0 THEN 'Dévastateur'
        WHEN magnitude >= 7.0 THEN 'Majeur'
        WHEN magnitude >= 6.0 THEN 'Fort'
        WHEN magnitude >= 5.0 THEN 'Modéré'
        ELSE                       'Mineur'
    END AS severity,

    -- Flag significativité USGS (seuil : sig > 500)
    CASE WHEN significance > 500 THEN 1 ELSE 0 END AS is_significant,

    CAST(CURRENT_TIMESTAMP AS STRING) AS processed_at

FROM earthquake_raw;


-- ===========================================================================
--  JOB 1 : Enrichissement temps réel — SANS fenêtre
--
--  Chaque événement arrivant dans earthquake-raw produit IMMÉDIATEMENT
--  une ligne enrichie dans earthquake-enriched.
--  Pas de GROUP BY → pas de fenêtre → latence quasi nulle.
-- ===========================================================================
SET 'pipeline.name' = 'earthquake-alerts';

INSERT INTO earthquake_alerts
SELECT
    event_id,
    event_time,
    magnitude,
    place,
    depth_km,
    tsunami,

    -- Sévérité (pas de cas 'Mineur' : filtre WHERE garantit mag >= 5.0)
    CASE
        WHEN tsunami   = 1    THEN 'TSUNAMI'
        WHEN magnitude >= 8.0 THEN 'Dévastateur'
        WHEN magnitude >= 7.0 THEN 'Majeur'
        WHEN magnitude >= 6.0 THEN 'Fort'
        ELSE                       'Modéré'
    END AS severity,

    CAST(CURRENT_TIMESTAMP AS STRING) AS alert_time

FROM earthquake_raw
WHERE magnitude >= 5.0 OR tsunami = 1;


-- ===========================================================================
--  JOB 2 : Alertes temps réel — filtre SANS fenêtre
--
--  Seuls les séismes dangereux (magnitude >= 5.0 ou tsunami) sont émis.
--  Chaque ligne source génère immédiatement une ligne en sortie.
-- ===========================================================================

SET 'pipeline.name' = 'earthquake-alerts';

INSERT INTO earthquake_alerts
SELECT
    event_id,
    event_time,
    magnitude,
    place,
    depth_km,
    tsunami,

    CASE
        WHEN tsunami   = 1    THEN 'TSUNAMI'
        WHEN magnitude >= 8.0 THEN 'Dévastateur'
        WHEN magnitude >= 7.0 THEN 'Majeur'
        WHEN magnitude >= 6.0 THEN 'Fort'
        ELSE                       'Modéré'
    END AS severity,

    CAST(CURRENT_TIMESTAMP AS STRING) AS alert_time

FROM earthquake_raw
WHERE magnitude >= 5.0 OR tsunami = 1;