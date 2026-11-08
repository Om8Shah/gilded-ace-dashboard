# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Load data
# MAGIC Reads the competition files from the Unity Catalog volume and saves them as Delta tables.
# MAGIC Upload these 7 parquet files to `/Volumes/workspace/default/casino/` first.

# COMMAND ----------
VOL = "/Volumes/workspace/default/casino"
NAMES = ["prepared_daily_view", "property_day", "player_period", "action_catalog",
         "events", "weather_daily", "city_visitation_monthly"]
for n in NAMES:
    df = spark.read.parquet(f"{VOL}/{n}.parquet")
    df.write.mode("overwrite").saveAsTable(f"workspace.default.{n}")
    print(n, df.count(), "rows")
