# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Machine scorecard (Q1: which machines are truly weak?)
# MAGIC Separates weak machines from unlucky, new, loosely set or badly placed ones.
# MAGIC Cutoffs below are judgment calls and are disclosed in the write-up.

# COMMAND ----------
# Verdict cutoffs (judgment calls)
MIN_DAYS = 90        # fewer days on floor -> "Short time on floor"
LUCK_Z_BAD = -2.0    # luck z below this -> "Inspect payout"
PLAY_RATIO = 0.75    # play per hour vs zone average below this -> "Low play for its zone"
THEO_RATIO = 0.80    # theo per hour vs zone average below this -> "Looser game (settings)"
LUCK_Z_MILD = -1.0   # luck z below this -> "Mostly unlucky"

spark.sql(f"""
CREATE OR REPLACE TABLE workspace.default.machine_scorecard_final AS
WITH d AS (
  SELECT * FROM workspace.default.prepared_daily_view WHERE available_hours > 0
),
zone_days AS (            -- main zone = zone where the machine spent the most days
  SELECT machine_id, zone_name, COUNT(*) AS days FROM d GROUP BY machine_id, zone_name
),
main_zone AS (
  SELECT machine_id, MAX_BY(zone_name, days) AS zone FROM zone_days GROUP BY machine_id
),
m AS (
  SELECT machine_id,
         COUNT(*)                         AS days_on_floor,
         SUM(house_win_usd)               AS house_win,
         SUM(theo_usd)                    AS theo,
         SUM(luck_residual_usd)           AS luck,
         SUM(luck_residual_usd) / (SQRT(COUNT(*)) * NULLIF(STDDEV(luck_residual_usd), 0)) AS luck_z,
         SUM(coin_in_usd) / SUM(available_hours)  AS coin_in_per_hr,
         SUM(theo_usd)    / SUM(available_hours)  AS theo_per_hr,
         AVG(configured_rtp)              AS rtp
  FROM d GROUP BY machine_id
),
j AS (
  SELECT m.*, z.zone,
         coin_in_per_hr / AVG(coin_in_per_hr) OVER (PARTITION BY z.zone) AS play_vs_zone,
         theo_per_hr    / AVG(theo_per_hr)    OVER (PARTITION BY z.zone) AS theo_vs_zone,
         (rtp - AVG(rtp) OVER (PARTITION BY z.zone)) * IF(rtp <= 1, 100, 1) AS rtp_pts_vs_zone
  FROM m JOIN main_zone z USING (machine_id)
)
SELECT machine_id, zone, days_on_floor, house_win, theo, luck, luck_z,
       play_vs_zone, rtp_pts_vs_zone,
       CASE WHEN days_on_floor < {MIN_DAYS}      THEN 'Short time on floor'
            WHEN luck_z < {LUCK_Z_BAD}           THEN 'Inspect payout'
            WHEN play_vs_zone < {PLAY_RATIO}     THEN 'Low play for its zone'
            WHEN theo_vs_zone < {THEO_RATIO}     THEN 'Looser game (settings)'
            WHEN luck_z < {LUCK_Z_MILD}          THEN 'Mostly unlucky'
            ELSE 'Normal' END AS verdict
FROM j
""")
display(spark.sql("SELECT verdict, COUNT(*) n FROM workspace.default.machine_scorecard_final GROUP BY verdict ORDER BY n DESC"))
