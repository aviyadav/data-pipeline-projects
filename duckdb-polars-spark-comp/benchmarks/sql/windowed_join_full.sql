-- Full-column variant: this is the article's DuckDB query verbatim, with
-- delta_scan() replaced by read_parquet() because the data is generated
-- locally instead of living in a Delta table.
--
-- {RAW}    -> parquet glob for the fact table
-- {MODELS} -> parquet glob for the small join dimension
--
-- Every engine in this project implements exactly this plan:
--   1. de-duplicate the small dimension to one row per drive
--   2. three window expressions over the fact table, partitioned by drive and
--      ordered by date (row_number, lag(1), trailing 30-row average)
--   3. a left join of that result back onto the dimension
--   4. four scalar aggregates used as a correctness fingerprint

WITH models_deduped AS (
    SELECT
        serial_number,
        ANY_VALUE(model) AS model
    FROM read_parquet('{MODELS}')
    GROUP BY serial_number
),

windowed AS (
    SELECT
        r.* EXCLUDE (model),

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
