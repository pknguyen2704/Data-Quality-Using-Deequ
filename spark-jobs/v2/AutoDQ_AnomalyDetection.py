import os
os.environ["SPARK_VERSION"] = os.environ.get("SPARK_VERSION", "3.5")
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pyspark.sql.dataframe")
import gc
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# Prometheus PushGateway
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# Spark
from pyspark.sql import SparkSession, functions as F

# ML
import shap
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score


# ---------------------- Job Config ----------------------
def get_spark_session(job_name):
  builder = (
    SparkSession.builder
      .appName(job_name)
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
  )

  spark = builder.getOrCreate()
  return spark

def get_job_config(spark):
  raw_exclude_cols = spark.conf.get("spark.exclude_cols", "")
  exclude_cols = ["label", "date_hour"]

  if raw_exclude_cols:
    parts = [x.strip() for x in raw_exclude_cols.split(",") if x.strip()]
    exclude_cols.extend(parts)

  print("exclude_cols:", exclude_cols)

  return {
    "date_hour": spark.conf.get("spark.date_hour", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    "table": spark.conf.get("spark.table_name", None),
    "read_days": int(spark.conf.get("spark.read_days", "3")),
    "sample_frac": float(spark.conf.get("spark.sample_frac", "1")),
    "random_state": int(spark.conf.get("spark.random_state", "42")),
    "test_frac": float(spark.conf.get("spark.test_frac", "0.2")),
    "n_estimators": int(spark.conf.get("spark.n_estimators", "500")),
    "max_depth": int(spark.conf.get("spark.max_depth", "10")),
    "early_stopping_rounds": int(spark.conf.get("spark.early_stopping_rounds", "10")),
    "pushgateway_url": spark.conf.get("spark.pushgateway.url", None),
    "exclude_cols": exclude_cols,
  }

# ---------------------- Read Data ----------------------
def read_records(spark, table, date_hour, read_days, sample_frac, seed):
    prev_hour_dt = date_hour - timedelta(hours=1)
    prev_date_str = prev_hour_dt.strftime("%Y-%m-%d")
    prev_hour = prev_hour_dt.hour

    print(f"[INFO] Job time = {date_hour}, reading previous hour = {prev_hour_dt}")

    today = date_hour.date()
    start_date = today - timedelta(days=read_days)

    date_list = [
        (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(read_days + 1)
    ]

    print(f"[INFO] Reading partitions: {start_date} → {today}")
    print(f"[INFO] Reading dates: {date_list}")

    # Load data theo ngày (keep all date_list)
    df = (
        spark.table(table)
        .withColumn("date", F.to_date("date_hour"))
        .filter(F.col("date").isin(date_list))
        .drop("date")
    )

    print(f"[INFO] Loaded {(read_days+1)} days: {df.count()} records before hour filtering")

    # Lọc theo giờ (KEEP all dates), sau đó label target_date = 1, others = 0
    df_filtered = (
        df.withColumn("hour", F.hour("date_hour"))
          .filter(F.col("hour") == prev_hour)   # <-- chỉ filter theo hour, không loại date khác
          .drop("hour")
          .withColumn(
              "label",
              F.when(F.to_date("date_hour") == F.lit(prev_date_str), F.lit(1)).otherwise(F.lit(0))
          )
    )

    print(f"[INFO] Filtered to hour={prev_hour}:00 across dates → {df_filtered.count()} records (target_date={prev_date_str})")

    if sample_frac < 1.0:
        df_target = df_filtered.filter(F.col("label") == 1).sample(sample_frac, seed)
        df_other = df_filtered.filter(F.col("label") == 0).sample(sample_frac, seed)
        print(f"[INFO] Sampled: target={df_target.count()}, others={df_other.count()}")
        return df_target.unionByName(df_other)

    print(f"[INFO] Returned full dataset: {df_filtered.count()} records")
    return df_filtered
# ---------------------- Prepare Data for ML ----------------------
def to_pandas_for_ml(df_spark):
  df_pd = df_spark.toPandas()

  list_cols = [c for c in df_pd.columns if df_pd[c].apply(lambda v: isinstance(v, (list, tuple))).any()]
  for c in list_cols:
    df_pd[c] = df_pd[c].apply(lambda v: len(v) if isinstance(v, (list, tuple)) else np.nan)

  datetime_cols = df_pd.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns
  for c in datetime_cols:
    df_pd[c] = df_pd[c].dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S")

  return df_pd

def extract_features(X, exclude_cols):
  if exclude_cols is None:
    exclude_cols = []

  use_features = [c for c in X.columns if c not in exclude_cols]
  final_features = []

  for c in use_features:
    if pd.api.types.is_numeric_dtype(X[c]):
      final_features.append(c)
    elif pd.api.types.is_object_dtype(X[c]) or pd.api.types.is_categorical_dtype(X[c]):
      X[c] = X[c].astype("category")
      final_features.append(c)
  return X[final_features]

# ---------------------- Train Model & Collect Metrics ----------------------
def train_model_and_collect_metrics(
  spark, X_all, y_all, test_frac, random_state, n_estimators, max_depth, early_stopping_rounds, table
):
  X_train, X_test, y_train, y_test = train_test_split(
    X_all, y_all, test_size=test_frac, random_state=random_state, stratify=y_all
  )

  clf = xgb.XGBClassifier(
    tree_method="hist",
    device="gpu",
    random_state=random_state,
    enable_categorical=True,
    eval_metric="logloss",
    n_estimators=n_estimators,
    max_depth=max_depth,
    early_stopping_rounds=early_stopping_rounds,
    grow_policy="lossguide",
  )

  print("___START_TRAINING___")
  clf.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

  print("___START_TESTING___")
  proba = clf.predict_proba(X_test)[:, 1]
  metrics_dict = {
    "roc_auc": roc_auc_score(y_test, proba),
  }

  metrics_df = pd.DataFrame(
    [
      {"table": table, "type": "ML_Metric", "instance": None, "metric": k, "value": float(v)}
      for k, v in metrics_dict.items()
    ]
  )

  explainer = shap.TreeExplainer(clf)
  shap_values = explainer.shap_values(X_test)
  shap_mean_abs = np.abs(shap_values).mean(axis=0)

  shap_df = pd.DataFrame(
    {
      "table": table,
      "type": "SHAP",
      "instance": X_test.columns.astype(str),
      "metric": "shap_mean_abs",
      "value": shap_mean_abs.astype(float),
    }
  )

  all_metrics_df = pd.concat([metrics_df, shap_df], ignore_index=True)
  return spark.createDataFrame(all_metrics_df)

# ---------------------- Push Metrics ----------------------
def push_ml_metric(spark_metrics_df, pushgateway_url, job_name, table):
  print("___PUSHING_ML_METRICS_TO_PUSHGATEWAY___")
  registry = CollectorRegistry()
  g = Gauge(
    job_name,
    job_name,
    ["table", "metric_type", "instance", "metric"],
    registry=registry,
  )

  for row in spark_metrics_df.collect():
    d = row.asDict()
    table = d["table"]
    type = d["type"]
    instance = d["instance"]
    metric = d["metric"]
    value = d["value"]
    g.labels(table, type, instance, metric).set(value)

  push_to_gateway(pushgateway_url, job=job_name, registry=registry, grouping_key={"table": table})

# ---------------------- Main ----------------------
def main():
  JOB_NAME = "AutoDQ_AnomalyDetection"
  spark = get_spark_session(JOB_NAME)
  job_conf = get_job_config(spark)
  table = job_conf["table"]
  read_days = job_conf["read_days"]
  sample_frac = job_conf["sample_frac"]
  random_state = job_conf["random_state"]
  exclude_cols = job_conf["exclude_cols"]
  test_frac = job_conf["test_frac"]
  n_estimators = job_conf["n_estimators"]
  max_depth = job_conf["max_depth"]
  early_stopping_rounds = job_conf["early_stopping_rounds"]
  pushgateway_url = job_conf["pushgateway_url"]
  date_hour = datetime.strptime(job_conf["date_hour"], "%Y-%m-%d %H:%M:%S")

  print(f"[INFO] Starting job {JOB_NAME} for table {table} at {date_hour}")

  df = read_records(
    spark,
    table,
    date_hour, 
    read_days, 
    sample_frac, 
    random_state
  )
  
  print("___START_ANOMALY_DETECTION_USING_ML___")
  df_pd = to_pandas_for_ml(df)
  print(exclude_cols)
  X_all = extract_features(df_pd, exclude_cols)
  y_all = df_pd["label"].values
  del df_pd
  gc.collect()

  ml_metrics_df = train_model_and_collect_metrics(
    spark,
    X_all,
    y_all,
    test_frac,
    random_state,
    n_estimators,
    max_depth,
    early_stopping_rounds,
    table
  )

  push_ml_metric(ml_metrics_df, pushgateway_url, JOB_NAME, table)

  spark.stop()
  print("___JOB_COMPLETED___")

if __name__ == "__main__":
  main()
