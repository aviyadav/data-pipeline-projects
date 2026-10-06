-- Projection-pushdown variant: identical semantics to windowed_join_full.sql,
-- but the scan only reads the three columns the plan actually needs
-- (serial_number, date, smart_5_raw) instead of all 193.
--
-- This mirrors the article's "let me give Polars the best possible chance"
-- rewrite, where column pruning was added to avoid reading the wide table.
-- It is the fairer I/O comparison between the engines; the full variant is the
-- fairer end-to-end comparison.

WITH models_deduped AS (
    SELECT
        serial_number,
        ANY_VALUE(model) AS model
    FROM read_parquet('{MODELS}')
    GROUP BY serial_number
),

windowed AS (
    SELECT
        r.serial_number,
        r.date,
        r.smart_5_raw,

        ROW_NUMBER() OVER (
            PARTITION BY serial_number
            ORDER BY date
        ) AS row_num,

        LAG(smart_5_raw, 1) OVER (
            PARTITION BY serial_number
            ORDER BY date
        ) AS previous_smart_5_raw,

        AVG(smart_5_raw) OVER (
            PARTITION BY serial_number
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_smart_5_raw

    FROM read_parquet('{RAW}') r
),

joined AS (
    SELECT
        w.*,
        m.model
    FROM windowed w
    LEFT JOIN models_deduped m
        ON w.serial_number = m.serial_number
)

SELECT
    COUNT(*) AS row_count,
    SUM(row_num) AS sum_row_num,
    SUM(previous_smart_5_raw) AS sum_previous,
    SUM(rolling_avg_smart_5_raw) AS sum_rolling_avg

FROM joined;
