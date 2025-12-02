import os
os.environ["SPARK_VERSION"] = os.environ.get("SPARK_VERSION", "3.5")
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pyspark.sql.dataframe")
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Row, functions as F
from pydeequ.suggestions import *

def get_spark_session(job_name):
  builder = (
    SparkSession.builder
      .appName(job_name)
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
  )

  spark = builder.getOrCreate()
  return spark

def get_job_config(spark):
  return {
    "date_hour": spark.conf.get("spark.date_hour", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    "table": spark.conf.get("spark.table", None),
    "read_days": int(spark.conf.get("spark.read_days", "2")),
    "top_low_ratio": float(spark.conf.get("spark.top_low_ratio", "0.8")),
  }

def save_suggestions(spark, table, suggestions):
  table = table.replace(".", "_")
  suggestion_table = f"constraints.{table}_constraints_suggested"

  rows = [
    Row(
      constraint_name=s.get("constraint_name"),
      column_name=s.get("column_name"),
      current_value=s.get("current_value"),
      description=s.get("description"),
      suggesting_rule=s.get("suggesting_rule"),
      rule_description=s.get("rule_description"),
      code_for_constraint=s.get("code_for_constraint"),
    )
    for s in suggestions
  ]

  df_suggestions = spark.createDataFrame(rows)

  df_suggestions.write.mode("overwrite").format("hive").saveAsTable(suggestion_table)

  print(f"[INFO] Saved {df_suggestions.count()} constraints to table {suggestion_table}")

def create_or_update_activate_table(spark, table):
  table = table.replace(".", "_")
  suggestion_table = f"constraints.{table}_constraints_suggested"
  activate_table = f"constraints.{table}_constraints_activated"
  tmp_table = f"{activate_table}_tmp"

  spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {activate_table} (
      constraint_name STRING,
      column_name STRING,
      current_value STRING,
      description STRING,
      suggesting_rule STRING,
      rule_description STRING,
      code_for_constraint STRING,
      level STRING,
      frequency INT,
      active INT,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    )
    STORED AS PARQUET
  """)

  df_new = (
    spark.table(suggestion_table)
      .withColumn("constraint_name", F.col("constraint_name"))
      .withColumn("column_name", F.col("column_name"))
      .withColumn("code_for_constraint", F.col("code_for_constraint"))
      .withColumn("level", F.lit("low"))
      .withColumn("frequency", F.lit(1))
      .withColumn("active", F.lit(0))
      .withColumn("created_at", F.current_timestamp())
      .withColumn("updated_at", F.current_timestamp())
  )

  try:
    df_old = spark.table(activate_table)
    df_old = (
      df_old
        .withColumn("constraint_name", F.col("constraint_name"))
        .withColumn("column_name", F.col("column_name"))
        .withColumn("code_for_constraint", F.col("code_for_constraint"))
    )
  except Exception:
    df_old = spark.createDataFrame([], df_new.schema)

  join_cond = [
    F.col("n.constraint_name") == F.col("o.constraint_name"),
    F.col("n.column_name") == F.col("o.column_name"),
    F.col("n.code_for_constraint") == F.col("o.code_for_constraint"),
  ]

  df_merged = (
    df_new.alias("n")
      .join(df_old.alias("o"), on=join_cond, how="full_outer")
      .select(
        F.coalesce(F.col("n.constraint_name"), F.col("o.constraint_name")).alias("constraint_name"),
        F.coalesce(F.col("n.column_name"), F.col("o.column_name")).alias("column_name"),
        F.coalesce(F.col("n.current_value"), F.col("o.current_value")).alias("current_value"),
        F.coalesce(F.col("n.description"), F.col("o.description")).alias("description"),
        F.coalesce(F.col("n.suggesting_rule"), F.col("o.suggesting_rule")).alias("suggesting_rule"),
        F.coalesce(F.col("n.rule_description"), F.col("o.rule_description")).alias("rule_description"),
        F.coalesce(F.col("n.code_for_constraint"), F.col("o.code_for_constraint")).alias("code_for_constraint"),
        F.coalesce(F.col("o.level"), F.col("n.level")).alias("level"),
        F.when(
            F.col("o.constraint_name").isNotNull() & F.col("n.constraint_name").isNotNull(),
            F.col("o.frequency") + 1
        ).when(
            F.col("o.constraint_name").isNotNull() & F.col("n.constraint_name").isNull(),
            F.col("o.frequency")
        ).otherwise(F.lit(1)).alias("frequency"),
        F.coalesce(F.col("o.active"), F.lit(0)).alias("active"),
        F.when(
            F.col("o.created_at").isNotNull(),
            F.col("o.created_at")
        ).otherwise(F.col("n.created_at")).alias("created_at"),
        F.current_timestamp().alias("updated_at"),
      )
  )

  df_merged.write.mode("overwrite").saveAsTable(tmp_table)
  spark.sql(f"DROP TABLE IF EXISTS {activate_table}")
  spark.sql(f"ALTER TABLE {tmp_table} RENAME TO {activate_table}")

  print(f"[INFO] Updated activate table {activate_table}")

def update_activate_flag(spark, table, top_low_ratio):
  table = table.replace(".", "_")
  activate_table = f"constraints.{table}_constraints_activated"
  tmp_table = f"{activate_table}_tmp"

  df = spark.table(activate_table)

  df_high = df.filter(F.col("level") == "high").withColumn("active", F.lit(1))
  df_non_high = df.filter((F.col("level") != "high") | F.col("level").isNull())
  
  total = df_non_high.count()
  top_n = int(total * top_low_ratio)

  df_top = df_non_high.orderBy(F.col("frequency").desc()).limit(top_n)
  df_rest = df_non_high.exceptAll(df_top)

  df_top = df_top.withColumn("active", F.lit(1))
  df_rest = df_rest.withColumn("active", F.lit(0))
  print(f"[INFO] df_top count {df_top.count()}")
  print(f"[INFO] df_rest count {df_rest.count()}")

  df_updated = df_high.unionByName(df_top).unionByName(df_rest).withColumn("updated_at", F.current_timestamp())

  df_updated.write.mode("overwrite").saveAsTable(tmp_table)
  df_updated.show(10)
  spark.sql(f"DROP TABLE IF EXISTS {activate_table}")
  spark.sql(f"ALTER TABLE {tmp_table} RENAME TO {activate_table}")

  print(f"[INFO] Updated active flags in {activate_table}")

def read_records(spark, table, read_days, date_hour):
  today = date_hour.date()
  start_date = today - timedelta(days=read_days)
  end_date = today - timedelta(days=1)

  date_list = [
    (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
    for i in range(read_days)
  ]
  dates_str = ",".join([f"'{d}'" for d in date_list])

  print(f"[INFO] Reading partitions: {start_date} → {end_date}")
  print(f"[INFO] Reading dates: {date_list}")

  query = f"""
    SELECT *
    FROM {table}
    WHERE DATE(date_hour) IN ({dates_str})
  """
  df = spark.sql(query)

  print(f"[INFO] Loaded {read_days} days total: {df.count()}")
  df.printSchema()
  df.show(5)

  return df

def main():
  JOB_NAME = "AutoDQ_ConstraintsSuggestion"
  spark = get_spark_session(JOB_NAME)
  job_conf = get_job_config(spark)
  table = job_conf["table"]
  read_days = job_conf["read_days"]
  top_low_ratio = job_conf["top_low_ratio"]
  date_hour = datetime.strptime(job_conf["date_hour"], "%Y-%m-%d %H:%M:%S")
  
  print(f"[INFO] Starting job {JOB_NAME} for table {table} at {date_hour}")
  df = read_records(spark, table, read_days, date_hour)

  print("___START_CONSTRAINTS_SUGGESTION___")
  suggestion_result = (ConstraintSuggestionRunner(spark)
    .onData(df)
    .addConstraintRule(DEFAULT())
    .run()
  )
  suggestions = [
    {
      "constraint_name": s.get("constraint_name"),
      "column_name": s.get("column_name"),
      "current_value": s.get("current_value"),
      "description": s.get("description"),
      "suggesting_rule": s.get("suggesting_rule"),
      "rule_description": s.get("rule_description"),
      "code_for_constraint": s.get("code_for_constraint"),
    }
    for s in suggestion_result.get("constraint_suggestions", [])
  ]

  print("___SAVING_SUGGESTIONS_TO_CONSTRAINTS___")
  save_suggestions(spark, table, suggestions)

  print("___CREATE_AND_INSERT_ACTIVATED_CONSTRAINTS___")
  create_or_update_activate_table(spark, table)
  update_activate_flag(spark, table, top_low_ratio)

  spark.stop()
  print("___JOB_COMPLETED___")

if __name__ == "__main__":
  main()
