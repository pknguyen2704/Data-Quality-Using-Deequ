import os
os.environ["SPARK_VERSION"] = os.environ.get("SPARK_VERSION", "3.5")
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pyspark.sql.dataframe")

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pydeequ.checks import *
from pydeequ.verification import *
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


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
  return {
    "date_hour": spark.conf.get("spark.date_hour", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    "table": spark.conf.get("spark.table", None),
    "pushgateway_url": spark.conf.get("spark.pushgateway.url", None),
  }

def read_activated_rules(spark, table):
  table = table.replace(".", "_")
  hive_table = f"constraints.{table}_constraints_activated"
  df_rules = spark.table(hive_table)
  df_active = df_rules.filter(F.col("active") == 1)
  print(f"[INFO] Loaded {df_active.count()} active rules from {hive_table}")
  return df_active.collect()

def read_records(spark, table, date_hour):
  prev_hour = date_hour - timedelta(hours=1)
  date_str = prev_hour.strftime("%Y-%m-%d")
  hour_int = prev_hour.hour

  df = (
    spark.table(table)
      .withColumn("date", F.to_date("date_hour"))
      .withColumn("hour", F.hour(F.col("date_hour").cast("timestamp")))
      .filter(
          (F.col("date") == date_str) &
          (F.col("hour") == hour_int)
      )
      .drop("date", "hour")
  )

  print(f"[INFO] Loaded data for {date_str} hour={hour_int}: count={df.count()}")
  df.printSchema()
  df.show(5)

  return df

def build_check(spark, df, rules, check_name):
  check = Check(spark, CheckLevel.Warning, check_name)
  for r in rules:
      check = eval(f"check{r.code_for_constraint}")
  return check

def push_verification_metric(spark, job_name, check_result, table, pushgateway_url):
  registry = CollectorRegistry()
  gauge = Gauge(
    job_name,
    job_name,
    ["table", "check_level", "constraint_name", "constraint_message"],
    registry=registry
  )

  df_result = VerificationResult.checkResultsAsDataFrame(spark, check_result)
  df_result.show(20)
  for row in df_result.collect():
    constraint_name = row["constraint"]  
    check_level = row["check_level"] 
    constraint_message = row["constraint_message"]
    gauge.labels(table, check_level, constraint_name, constraint_message).set(0 if row["constraint_status"] == "Failure" else 1)

  push_to_gateway(pushgateway_url, job=job_name, registry=registry, grouping_key={"table": table})

def main():
  JOB_NAME = "AutoDQ_ConstraintsVerification"
  spark = get_spark_session(JOB_NAME)
  job_conf = get_job_config(spark)
  table = job_conf["table"]
  pushgateway_url = job_conf["pushgateway_url"]
  date_hour = datetime.strptime(job_conf["date_hour"], "%Y-%m-%d %H:%M:%S")
  print(f"[INFO] Starting job {JOB_NAME} for table {table} at {date_hour}")
  df = read_records(spark, table, date_hour)
  rules = read_activated_rules(spark, table)
  print("___START_CONSTRAINTS_VERIFICATION___")
  check = build_check(spark, df, rules, JOB_NAME)
  check_result = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(check) \
    .run()
  print("___PUSHING_VERIFICATION_METRICS___")
  push_verification_metric(spark, JOB_NAME, check_result, table, pushgateway_url)

  spark.sparkContext._gateway.shutdown_callback_server()
  spark.stop()
  print("___JOB_COMPLETED___")

if __name__ == "__main__":
    main()
