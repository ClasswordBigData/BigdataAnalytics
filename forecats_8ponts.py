import os
from pathlib import Path
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

TZ = "Africa/Kigali"
OUT = Path("assignment8_outputs")
OUT.mkdir(exist_ok=True)
host = os.getenv("PGHOST", "10.32.87.163")
port = os.getenv("PGPORT", "5432")
database = os.getenv("PGDATABASE", "smart_energy_grid")
username = os.getenv("PGUSER", "eric")
password = os.getenv("PGPASSWORD", "kigali")

from sqlalchemy.engine import URL
connection_url = URL.create(
    "postgresql+psycopg2",
    username=username,
    password=password,
    host=host,
    port=int(port),
    database=database,
)

engine = create_engine(connection_url, pool_pre_ping=True, connect_args={"connect_timeout": 15})
sql = text("""
SELECT
    time_bucket(INTERVAL '1 hour', r.event_time, 'Africa/Kigali') AS target_time,
    m.region,
    SUM(r.energy_kwh)::double precision AS actual_kwh,
    COUNT(*)::int AS reading_count,
    COUNT(r.energy_kwh)::int AS nonnull_energy_count
FROM energy_readings AS r
JOIN meters AS m ON m.meter_id = r.meter_id
WHERE r.event_time >= TIMESTAMP '2026-09-01 00:00:00' AT TIME ZONE 'Africa/Kigali'
  AND r.event_time <  TIMESTAMP '2026-09-29 00:00:00' AT TIME ZONE 'Africa/Kigali'
GROUP BY 1, 2
ORDER BY 1, 2
""")
with engine.connect() as conn:
    raw = pd.read_sql(sql, conn)

if raw.empty:
    raise SystemExit("The query returned no data. Check the database and date range.")

raw["target_time"] = pd.to_datetime(raw["target_time"], utc=True).dt.tz_convert(TZ)
raw["region"] = raw["region"].astype(str)
raw = raw.sort_values(["region", "target_time"]).reset_index(drop=True)

regions = sorted(raw["region"].unique(), key=lambda x: (int(x.split()[-1]) if x.split()[-1].isdigit() else 999, x))
hours = pd.date_range("2026-09-01 00:00", "2026-09-28 23:00", freq="h", tz=TZ)
full = pd.MultiIndex.from_product([regions, hours], names=["region", "target_time"]).to_frame(index=False)
df = full.merge(raw, on=["region", "target_time"], how="left")
df["reading_count"] = df["reading_count"].fillna(0).astype(int)
df["nonnull_energy_count"] = df["nonnull_energy_count"].fillna(0).astype(int)
df["actual_kwh"] = pd.to_numeric(df["actual_kwh"], errors="coerce")
df = df.sort_values(["region", "target_time"]).reset_index(drop=True)

# Calendar predictors known at midnight. Hour-of-day is the target hour being
# forecast; it is known in advance. No same-day observed consumption is used.
df["hour"] = df["target_time"].dt.hour
df["day_of_week"] = df["target_time"].dt.dayofweek
df["day_of_month"] = df["target_time"].dt.day
df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
df["dow_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
df["dow_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)

# Lag values are joined by exact source timestamp within each region.
# All source timestamps are target_time minus the lag and thus precede that
# target day's 00:00 forecast origin, including all 24 forecast hours.
for lag in (24, 48, 168):
    source = df[["region", "target_time", "actual_kwh"]].copy()
    source["target_time"] = source["target_time"] + pd.Timedelta(hours=lag)
    source = source.rename(columns={"actual_kwh": f"lag_{lag}h_kwh"})
    df = df.merge(source, on=["region", "target_time"], how="left", validate="one_to_one")

# Check source-time chronology for every lag and target. For target timestamp t,
# the source is t-lag. The source is always earlier than the day's midnight origin.
df["forecast_origin"] = df["target_time"].dt.normalize()
chronology = {}
for lag in (24, 48, 168):
    source_time = df["target_time"] - pd.Timedelta(hours=lag)
    chronology[f"lag_{lag}h_all_before_origin"] = bool((source_time < df["forecast_origin"]).all())
    chronology[f"lag_{lag}h_source_precedes_target"] = bool((source_time < df["target_time"]).all())
if not all(chronology.values()):
    raise AssertionError(f"Feature chronology check failed: {chronology}")

df["split"] = np.select(
    [
        df["target_time"] < pd.Timestamp("2026-09-15 00:00", tz=TZ),
        df["target_time"] < pd.Timestamp("2026-09-22 00:00", tz=TZ),
    ],
    ["train", "validation"],
    default="test",
)

calendar_features = [
    "hour", "day_of_week", "day_of_month", "is_weekend",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos"
]
lag_features = ["lag_24h_kwh", "lag_48h_kwh", "lag_168h_kwh"]
features = ["region"] + calendar_features + lag_features
target = "actual_kwh"

# Report the complete grid, reading counts, and missingness.
raw_count = int(raw["reading_count"].sum())
nonnull_count = int(raw["nonnull_energy_count"].sum())
missing_hours = int(df["actual_kwh"].isna().sum())
print("\n=== DATA PREPARATION ===")
print(f"Rows in complete region-hour grid: {len(df)}")
print(f"Regions: {len(regions)}; hourly timestamps: {len(hours)}")
print(f"Raw readings represented by aggregation: {raw_count:,}")
print(f"Non-null energy values represented: {nonnull_count:,}")
print(f"Region-hours with no observed target total: {missing_hours:,}")
print(f"Rows with reading_count=0: {(df['reading_count'] == 0).sum():,}")
print("Observed totals are preserved as targets; missing region-hours are NaN, not zero.")
print("\n=== SPLIT DATE LIMITS AND ROW COUNTS (before lookback/target exclusions) ===")
for name in ("train", "validation", "test"):
    part = df[df["split"] == name]
    print(f"{name:10s}: rows={len(part):,}, dates={part['target_time'].min()} to {part['target_time'].max()}")

# Initial lookback: 168 hours. Training rows from Sep 1-7 lack the weekly lag.
train_all = df[df["split"] == "train"].copy()
train_lookback_ok = train_all[lag_features].notna().all(axis=1)
train_target_ok = train_all[target].notna()
train_excluded_lookback = int((~train_lookback_ok).sum())
train_excluded_missing_target_after_lookback = int((train_lookback_ok & ~train_target_ok).sum())
train_keep = train_all[train_lookback_ok & train_target_ok].copy()

print("\n=== LOOKBACK EXCLUSIONS ===")
print(f"Training rows before exclusions: {len(train_all):,}")
print(f"Excluded for missing chosen lookback (lags 24/48/168): {train_excluded_lookback:,}")
print(f"Additional retained-lookback rows without observed target: {train_excluded_missing_target_after_lookback:,}")
print(f"Retained training rows: {len(train_keep):,}")
print("Shared model rows/configurations: identical retained training rows.")

# Earliest usable target: Sep 8 00:00 when 168h lag exists.
print("\n=== FEATURE INVENTORY ===")
feature_rows = [
    ("region", "static category", "2026-09-01 00:00 CAT"),
    ("hour", "calendar; 0-hour lookahead", "2026-09-01 00:00 CAT"),
    ("day_of_week", "calendar; 0-hour lookahead", "2026-09-01 00:00 CAT"),
    ("day_of_month", "calendar; 0-hour lookahead", "2026-09-01 00:00 CAT"),
    ("is_weekend", "calendar; 0-hour lookahead", "2026-09-01 00:00 CAT"),
    ("hour_sin/hour_cos", "calendar cycle; 0-hour lookahead", "2026-09-01 00:00 CAT"),
    ("dow_sin/dow_cos", "calendar cycle; 0-hour lookahead", "2026-09-01 00:00 CAT"),
    ("lag_24h_kwh", "24 hours", "2026-09-02 00:00 CAT"),
    ("lag_48h_kwh", "48 hours", "2026-09-03 00:00 CAT"),
    ("lag_168h_kwh", "168 hours (7 days)", "2026-09-08 00:00 CAT"),
]
for row in feature_rows:
    print(f"{row[0]:22s} | {row[1]:32s} | earliest target: {row[2]}")
print("Chronology checks:", chronology)

# Preprocessing is fit on training data only. Median imputation handles missing
# lag predictors using training medians; one-hot encodes region; scales numeric
# predictors. No target imputation is performed.
numeric_features = calendar_features + lag_features
preprocess = ColumnTransformer(
    transformers=[
        ("region", OneHotEncoder(handle_unknown="ignore"), ["region"]),
        ("numeric", Pipeline([
            ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
            ("scaler", StandardScaler())
        ]), numeric_features),
    ],
    remainder="drop"
)

configs = [
    ("RF_100_leaf2", dict(n_estimators=100, max_depth=None, min_samples_leaf=2, max_features=1.0)),
    ("RF_200_depth12", dict(n_estimators=200, max_depth=12, min_samples_leaf=2, max_features=1.0)),
]
val = df[df["split"] == "validation"].copy()
val = val[val[target].notna()].copy()
# Validation rows require the same feature set. Missing lag values are imputed
# by training-only preprocessing; they are not dropped.
X_train, y_train = train_keep[features], train_keep[target]
X_val, y_val = val[features], val[target]
if len(train_keep) == 0 or len(val) == 0:
    raise SystemExit("No usable training/validation rows; inspect missing data and lags.")

validation_results = []
fitted = {}
for config_id, settings in configs:
    model = Pipeline([
        ("preprocess", preprocess),
        ("regressor", RandomForestRegressor(random_state=42, n_jobs=-1, **settings)),
    ])
    model.fit(X_train, y_train)
    pred = model.predict(X_val)
    mae = mean_absolute_error(y_val, pred)
    rmse = np.sqrt(mean_squared_error(y_val, pred))
    validation_results.append({
        "configuration_id": config_id,
        "model_settings": str(settings),
        "training_rows": len(X_train),
        "validation_rows": len(X_val),
        "validation_MAE_kWh": mae,
        "validation_RMSE_kWh": rmse,
    })
    fitted[config_id] = model
    print(f"\nTraining complete: {config_id}")
    print(f"Validation MAE: {mae:.6f} kWh; RMSE: {rmse:.6f} kWh")

val_table = pd.DataFrame(validation_results).sort_values("validation_MAE_kWh")
val_table.to_csv(OUT / "validation_table.csv", index=False)
print("\n=== VALIDATION TABLE ===")
print(val_table.to_string(index=False))
best_id = str(val_table.iloc[0]["configuration_id"])
best_model = fitted[best_id]
print(f"\nSelected by lowest validation MAE: {best_id}")
print("Model and preprocessing are now frozen; no refit on validation/test data.")

# Baseline and model predictions on all 1,680 test grid rows. For the baseline,
# lag_24h_kwh is the previous day's same-hour observed regional total.
test = df[df["split"] == "test"].copy().sort_values(["target_time", "region"])
test["baseline_prediction_kwh"] = test["lag_24h_kwh"]
test["model_prediction_kwh"] = best_model.predict(test[features])
test["baseline_error_kwh"] = test["baseline_prediction_kwh"] - test["actual_kwh"]
test["model_error_kwh"] = test["model_prediction_kwh"] - test["actual_kwh"]
test["baseline_abs_error_kwh"] = test["baseline_error_kwh"].abs()
test["model_abs_error_kwh"] = test["model_error_kwh"].abs()
test.to_csv(OUT / "test_predictions.csv", index=False)
print("\n=== TEST PREDICTIONS ===")
print(f"Baseline prediction rows: {test['baseline_prediction_kwh'].notna().sum():,}")
print(f"Model prediction rows: {test['model_prediction_kwh'].notna().sum():,}")
print(f"Test rows with observed evaluation targets: {test['actual_kwh'].notna().sum():,}")
print(test[["target_time", "region", "actual_kwh", "baseline_prediction_kwh", "model_prediction_kwh"]].head(12).to_string(index=False))

def metrics(frame, pred_col):
    valid = frame[["actual_kwh", pred_col]].dropna()
    if valid.empty:
        return {"n": 0, "MAE_kWh": np.nan, "RMSE_kWh": np.nan}
    err = valid[pred_col] - valid["actual_kwh"]
    return {
        "n": len(valid),
        "MAE_kWh": mean_absolute_error(valid["actual_kwh"], valid[pred_col]),
        "RMSE_kWh": np.sqrt(mean_squared_error(valid["actual_kwh"], valid[pred_col])),
    }

overall = []
for method, col in [
    ("Previous-day baseline", "baseline_prediction_kwh"),
    (f"Random forest ({best_id})", "model_prediction_kwh")
]:
    overall.append({"method": method, **metrics(test, col)})
overall_df = pd.DataFrame(overall)
overall_df.to_csv(OUT / "overall_metrics.csv", index=False)
print("\n=== OVERALL TEST METRICS ===")
print(overall_df.to_string(index=False))

regional = []
for region in regions:
    sub = test[test["region"] == region]
    for method, col in [
        ("Previous-day baseline", "baseline_prediction_kwh"),
        (f"Random forest ({best_id})", "model_prediction_kwh")
    ]:
        regional.append({"region": region, "method": method, **metrics(sub, col)})
regional_df = pd.DataFrame(regional)
regional_df.to_csv(OUT / "regional_metrics.csv", index=False)
print("\n=== REGIONAL TEST METRICS ===")
print(regional_df.to_string(index=False))

# Peak actual hour per region; pandas idxmax returns the earliest row for ties
# because test is sorted chronologically.
peaks = []
for region in regions:
    observed = test[(test["region"] == region) & test["actual_kwh"].notna()].sort_values("target_time")
    if observed.empty:
        continue
    peak_idx = observed["actual_kwh"].idxmax()
    row = observed.loc[peak_idx]
    peaks.append({
        "region": region,
        "peak_time_CAT": row["target_time"],
        "actual_peak_kWh": row["actual_kwh"],
        "baseline_prediction_kWh": row["baseline_prediction_kwh"],
        "baseline_abs_error_kWh": row["baseline_abs_error_kwh"],
        "model_prediction_kWh": row["model_prediction_kwh"],
        "model_abs_error_kWh": row["model_abs_error_kwh"],
        "reading_count": row["reading_count"],
    })
peaks_df = pd.DataFrame(peaks)
peaks_df.to_csv(OUT / "regional_peak_errors.csv", index=False)
print("\n=== PEAK-HOUR ERRORS BY REGION ===")
print(peaks_df.to_string(index=False))

# Seven-day plot for Region 1, with observed totals and both forecasts.
plot_region = "Region 1" if "Region 1" in regions else regions[0]
plot_df = test[test["region"] == plot_region].sort_values("target_time")
plt.figure(figsize=(15, 6))
plt.plot(plot_df["target_time"].dt.tz_localize(None), plot_df["actual_kwh"], label="Actual observed total", linewidth=1.8)
plt.plot(plot_df["target_time"].dt.tz_localize(None), plot_df["baseline_prediction_kwh"], label="Previous-day baseline", alpha=0.8)
plt.plot(plot_df["target_time"].dt.tz_localize(None), plot_df["model_prediction_kwh"], label=f"Random forest {best_id}", alpha=0.8)
plt.title(f"Hourly regional consumption forecast — {plot_region} — 22–28 September 2026 CAT")
plt.xlabel("Target timestamp (CAT)")
plt.ylabel("Energy (kWh)")
plt.legend()
plt.grid(True, alpha=0.25)
plt.tight_layout()
plt.savefig(OUT / "seven_day_forecast_plot.png", dpi=160)
plt.close()

# Screenshots: terminal output should be captured with Win+Shift+S after each
# section. Machine-readable outputs and plot are saved in assignment8_outputs.
print("\n=== OUTPUT FILES ===")
for p in sorted(OUT.iterdir()):
    print(p.resolve())

# Final caution: the model predicts observed regional totals. Missing meter
# readings can make those totals lower than true consumption; delayed arrivals
# can make a live forecast differ from this offline, finalized-data evaluation.
