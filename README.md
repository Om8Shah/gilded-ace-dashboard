# The Gilded Ace: Floor Analytics (TAMU Datathon, Dashboard Royale + Databricks track)

Which 10 of 300 slot machines should the casino replace (Jan 1 – Jun 30, 2025)? This project separates machines that are truly weak from machines that are unlucky, new, mis-set or badly located, and prices a plan under $100K.

## Key findings
- 9 of the 10 lowest house-win machines sit in Back Forty. They are not under-played: 6 run loose game settings.
- M-1158 is the one anomalous machine (payout far below expectation): inspect, don't accuse.
- A weekday-mean demand model beats a constant baseline on a time-based holdout (MAE $68.7K vs $218.3K).
- Plan: 3 action types, $39,900 spent, $60,100 kept (promotion weekend $30,000; Back Forty wayfinding $7,500; machine inspection $2,400). All lose money at zero lift and break even near 3% lift.

## How Databricks is used
- Unity Catalog volume holds the source files; Delta tables (bronze to `gold_*`) feed the dashboard.
- Serverless notebooks (Python + Spark SQL) build the machine scorecard, downtime, player and comp tables, forecast and action plan.
- Forecast runs are logged with MLflow.
- AI/BI dashboard (4 pages, one per challenge question), built with Genie Code.

## Repo contents
- `notebooks/`: Databricks source notebooks `00` to `03`, run in order (import with Workspace > Import). They rebuild the tables behind the dashboard from the 7 parquet files in the volume. The final action-plan cell (sensitivity and break-even) is documented in the write-up; the `03` plan cell asserts the costs match $39,900.
- `dashboard/`: dashboard export and screenshots.

## Method notes and limitations
- Luck z-score = SUM(luck) / (sqrt(n) * stddev(luck)); rough, since days are correlated.
- Verdict cutoffs (my judgment calls): <90 days on floor, luck z < -2, play vs zone < 0.75, theo/hr < 0.8x zone average.
- Moved machines are assigned their main zone (most days).
- Forecast: train Jan 1 – Apr 30, test May 1 – Jun 30 (one 61-day holdout). Weather and visitation data explain demand only; they are not forecast inputs.
- Promotion and wayfinding lifts are assumptions; see the sensitivity table.

## AI-use disclosure
Claude (Anthropic) helped write the code, analysis and dashboard prompts. Findings were reviewed by the team.
