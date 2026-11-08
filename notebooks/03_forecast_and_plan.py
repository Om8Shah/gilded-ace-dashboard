# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Forecast validation and the $100K plan (Q6)
# MAGIC Time-based holdout: train Jan 1 - Apr 30 2025, test May 1 - Jun 30 2025. No test data used in training.

# COMMAND ----------
import pandas as pd
df = spark.table("workspace.default.property_day").select("gaming_date", "coin_in_usd").toPandas()
df["gaming_date"] = pd.to_datetime(df["gaming_date"])
df["dow"] = df["gaming_date"].dt.dayofweek
train, test = df[df.gaming_date <= "2025-04-30"], df[df.gaming_date >= "2025-05-01"].copy()

const_mean = train.coin_in_usd.mean()                      # baseline
dow_mean = train.groupby("dow").coin_in_usd.mean()         # weekday model
test["pred_const"] = const_mean
test["pred_weekday"] = test["dow"].map(dow_mean)

rows = []
for name, col in [("constant mean (baseline)", "pred_const"), ("weekday mean", "pred_weekday")]:
    err = test[col] - test.coin_in_usd
    rows.append((name, float(err.abs().mean()), float(err.mean())))
metrics = pd.DataFrame(rows, columns=["model", "mae_usd", "bias_usd"])
print(metrics)

out = test.rename(columns={"coin_in_usd": "coin_in"})[["gaming_date", "coin_in", "pred_const", "pred_weekday"]]
out["gaming_date"] = out["gaming_date"].dt.date
spark.createDataFrame(out).write.mode("overwrite").saveAsTable("workspace.default.gold_forecast_holdout")
spark.createDataFrame(metrics).write.mode("overwrite").saveAsTable("workspace.default.gold_forecast_metrics")

try:  # MLflow logging is a bonus; never block the pipeline
    import mlflow
    for _, r in metrics.iterrows():
        with mlflow.start_run(run_name=r["model"]):
            mlflow.log_param("train_end", "2025-04-30"); mlflow.log_param("test", "2025-05-01..2025-06-30")
            mlflow.log_metric("mae_usd", r["mae_usd"]); mlflow.log_metric("bias_usd", r["bias_usd"])
except Exception as e:
    print("MLflow skipped:", e)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Action plan (under $100K, 3 action types)
# MAGIC Costs come from `action_catalog`: qty x (unit_cost_cents + recurring_cents_per_day x max(0, 30 - lead_days)) / 100.
# MAGIC Expected totals: promotion weekend $30,000, Back Forty wayfinding $7,500, machine inspection $2,400 ($39,900 in all).
# MAGIC **Step 1: look at the catalog and set the three action ids and quantities in the next cell.**

# COMMAND ----------
display(spark.table("workspace.default.action_catalog"))

# COMMAND ----------
# Fill in from the catalog above (id of each action and how many units are bought)
PLAN = {
    "Promotion weekend":         {"action_type_id": None, "qty": None},
    "Back Forty wayfinding":     {"action_type_id": None, "qty": None},
    "Machine inspection":        {"action_type_id": None, "qty": None},
}
EXPECTED = {"Promotion weekend": 30000, "Back Forty wayfinding": 7500, "Machine inspection": 2400}

cat = spark.table("workspace.default.action_catalog").toPandas().set_index("action_type_id")
costs = {}
for name, p in PLAN.items():
    assert p["action_type_id"] is not None, "Set PLAN ids/quantities from the catalog first"
    c = cat.loc[p["action_type_id"]]
    costs[name] = p["qty"] * (c.unit_cost_cents + c.recurring_cost_cents_per_day * max(0, 30 - c.lead_days)) / 100
    print(name, costs[name], "expected", EXPECTED[name])
    assert abs(costs[name] - EXPECTED[name]) < 1, "cost does not match the plan in the write-up"
print("Total spend:", sum(costs.values()), "| unspent:", 100000 - sum(costs.values()))
