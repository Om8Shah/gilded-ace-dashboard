# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Demand, downtime, players and comps (Q2-Q5)
# MAGIC Builds the gold tables behind dashboard pages 2 and 3.

# COMMAND ----------
# Scheduled machine-hours: active machines x 24h (23h on the 2025-03-08 clock change)
spark.sql("""
CREATE OR REPLACE TEMP VIEW pd AS
SELECT *, active_machines * IF(gaming_date = DATE'2025-03-08', 23, 24) AS scheduled_hours
FROM workspace.default.property_day
""")

# Q2: weekday-adjusted demand index = coin-in / that weekday's average
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.gold_daily_index AS
SELECT gaming_date, coin_in_usd,
       coin_in_usd / AVG(coin_in_usd) OVER (PARTITION BY dayofweek(gaming_date)) AS demand_index
FROM pd
""")
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.gold_floor_weekday AS
SELECT dayofweek(gaming_date) AS dow, date_format(gaming_date, 'EEEE') AS weekday,
       AVG(coin_in_usd) AS avg_daily_coin_in,
       100 * (1 - SUM(available_machine_hours) / SUM(scheduled_hours)) AS lost_pct
FROM pd GROUP BY 1, 2 ORDER BY 1
""")

# Q3: downtime
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.gold_downtime_monthly AS
SELECT date_trunc('month', gaming_date) AS month,
       SUM(scheduled_hours - available_machine_hours) AS lost_machine_hours,
       SUM(faults_opened) AS faults_opened
FROM pd GROUP BY 1 ORDER BY 1
""")

# COMMAND ----------
# Q4: tracked players (loyalty-card play only). tracked_net_usd < 0 means the player lost.
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.gold_player_monthly AS
SELECT period_month, age_band, home_region, opening_tier,
       SUM(tracked_coin_in_usd) AS coin_in, SUM(tracked_net_usd) AS net_usd
FROM workspace.default.player_period GROUP BY ALL
""")
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.gold_player_concentration AS
WITH p AS (SELECT player_id, SUM(tracked_net_usd) AS net FROM workspace.default.player_period GROUP BY player_id),
r AS (SELECT *, PERCENT_RANK() OVER (ORDER BY net ASC) AS pr FROM p),   -- biggest losers first
tot AS (SELECT SUM(-net) AS total_loss FROM p WHERE net < 0)
SELECT SUM(IF(pr <= 0.01 AND net < 0, -net, 0)) / MAX(total_loss) AS top1pct_share_of_net_loss,
       SUM(IF(pr <= 0.10 AND net < 0, -net, 0)) / MAX(total_loss) AS top10pct_share_of_net_loss,
       AVG(IF(net > 0, 1, 0)) AS pct_players_ahead
FROM r CROSS JOIN tot
""")

# Q5: comps
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.gold_comp_monthly AS
SELECT date_trunc('month', gaming_date) AS month,
       SUM(comp_cost_usd) AS comp_cost, SUM(comp_face_value_usd) AS comp_face_value,
       100 * SUM(comp_cost_usd) / SUM(house_win_usd) AS comp_cost_pct_of_house_win
FROM pd GROUP BY 1 ORDER BY 1
""")
display(spark.table("workspace.default.gold_player_concentration"))
display(spark.table("workspace.default.gold_comp_monthly"))
