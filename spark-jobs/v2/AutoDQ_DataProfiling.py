import os
os.environ["SPARK_VERSION"] = os.environ.get("SPARK_VERSION", "3.5")
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pyspark.sql.dataframe")

from datetime import datetime, timedelta
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType
from pydeequ.analyzers import *
from pydeequ.suggestions import *

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

def deequ_analyzer(spark, df):
  size_analyzer = Size()
  analyzers = []
  for field in df.schema.fields:
    col_name = field.name
    data_type = field.dataType
    analyzers.extend([Completeness(col_name), ApproxCountDistinct(col_name)])

    if isinstance(data_type, NumericType):
      analyzers.extend([Minimum(col_name), Maximum(col_name), Mean(col_name), Sum(col_name)])
    elif isinstance(data_type, StringType):
      analyzers.extend([MaxLength(col_name), MinLength(col_name)])

  builder = AnalysisRunner(spark).onData(df).addAnalyzer(size_analyzer)
  for analyzer in analyzers:
    builder = builder.addAnalyzer(analyzer)

  analysis_result = builder.run()
  return analysis_result

def push_analyzer_metric(spark, job_name, analyzer_result, table, pushgateway_url):
  registry = CollectorRegistry()
  gauge = Gauge(
    job_name,
    job_name,
    ["table", "entity", "instance", "name"],
    registry=registry,
  )

  df_metrics = AnalyzerContext.successMetricsAsDataFrame(spark, analyzer_result)
  for row in df_metrics.collect():
    entity = row["entity"]
    instance = row["instance"]
    name = row["name"]
    value = row["value"]
    gauge.labels(table, entity, instance, name).set(value)

  push_to_gateway(pushgateway_url, job=job_name, registry=registry, grouping_key={"table": table})


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

def main():
  JOB_NAME = "AutoDQ_DataProfiling"
  spark = get_spark_session(JOB_NAME)
  job_conf = get_job_config(spark)
  table = job_conf["table"]
  pushgateway_url = job_conf["pushgateway_url"]
  date_hour = datetime.strptime(job_conf["date_hour"], "%Y-%m-%d %H:%M:%S")

  print(f"[INFO] Starting job {JOB_NAME} for table {table} at {date_hour}")
  df = read_records(spark, table, date_hour)

  print("___START_DATA_PROFILING___")
  analyzer_result = deequ_analyzer(spark, df)
  push_analyzer_metric(spark, JOB_NAME, analyzer_result, table, pushgateway_url)
  del analyzer_result
  spark.stop()
  print("___JOB_COMPLETED___")

if __name__ == "__main__":
  main()
