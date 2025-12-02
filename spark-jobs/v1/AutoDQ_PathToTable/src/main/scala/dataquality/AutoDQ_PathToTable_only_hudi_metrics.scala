package dataquality

import com.amazon.deequ.analyzers.{Size, Completeness, ApproxCountDistinct, Minimum, Maximum, Mean, Sum, MaxLength, MinLength}
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext}
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame

import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when, split, regexp_replace, date_format}
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType, IntegerType, ArrayType, TimestampType, NumericType, DataType}

import scala.collection.JavaConverters._
import scala.io.Source


  object AutoDQ_PathToTable_only_hudi_metrics {
  private final val JOB_NAME = "AutoDQ_PathToTable_only_hudi_metrics"
  private final val SPARK_MASTER = "spark://bigdataplatform.asia-southeast1-a.c.data-quality-project-470101.internal:7077"
  private final val WRITE_MODE = "append"
  //  private final val FILE_PATH = "hdfs://localhost:9000/sftp/simulation_with_schema1_sample.csv"
  //  private final val INPUT_TABLE = "simulation_with_schema1_sample"
  //  private final val STORAGE_PATH = "hdfs://localhost:9000/hudi/src_sample/simulation_with_schema1_sample"
  //  private final val METRICS_DB = "autoDQ_DB"
  //  private final val METRICS_TABLE = "autoDQ_metrics_sample"
  //  private final val METRICS_PATH = "hdfs://localhost:9000/hudi/autoDQ_metrics_sample"

  /** Src schema */
  private val src_schema = StructType(Seq(
    StructField("to_user", StringType, nullable = true),
    StructField("from_user", StringType, nullable = true),
    StructField("call_uid", StringType, true),
    StructField("call_start_time", TimestampType, true),
    StructField("call_response_time", TimestampType, true),
    StructField("call_ringing_time", TimestampType, true),
    StructField("call_answer_time", TimestampType, true),
    StructField("call_end_time", TimestampType, true),
    StructField("end_method", StringType, true),
    StructField("end_reason", StringType, true),
    StructField("last_sip_method", StringType, true),
    StructField("to_tag_val", ArrayType(StringType, true), true),
    StructField("from_tag_val", StringType, true),
    StructField("session_uid", StringType, true),
    StructField("early_media_mode", StringType, true),
    StructField("caller_user_agent", StringType, true),
    StructField("mo_network_info", StringType, true),
    StructField("mt_network_info", StringType, true),
    StructField("caller_identity_enc", StringType, true),
    StructField("alert_info", StringType, true),
    StructField("src_ip", StringType, true),
    StructField("dst_ip", StringType, true),
    StructField("mo_contact_uri", StringType, true),
    StructField("mt_contact_uri", StringType, true),
    StructField("to_phone_masked", StringType, true),
    StructField("from_phone_masked", StringType, true),
    StructField("start_ts_epoch", LongType, true),
    StructField("response_ts_epoch", LongType, true),
    StructField("ringing_ts_epoch", LongType, true),
    StructField("answer_ts_epoch", LongType, true),
    StructField("end_ts_epoch", LongType, true),
    StructField("to_enc_local", IntegerType, true),
    StructField("from_enc_public", StringType, true),
    StructField("to_phone_enc", StringType, true),
    StructField("from_phone_enc", StringType, true),
    StructField("caller_identity_enc_mask", StringType, true),
    StructField("sdp_req_ip", StringType, true),
    StructField("sdp_req_port", IntegerType, true),
    StructField("sdp_req_media", StringType, true),
    StructField("sdp_resp_ip", StringType, true),
    StructField("sdp_resp_port", IntegerType, true),
    StructField("sdp_resp_media", StringType, true),
    StructField("call_status", StringType, true),
    StructField("call_type", StringType, true),
    StructField("terminated_by", StringType, true),
    StructField("sip_route", StringType, true),
    StructField("pcap_files_list", ArrayType(StringType, true), true),
    StructField("handler_info", StringType, true),
    StructField("mt_user_agent_info", StringType, true),
    StructField("end_method_1", StringType, true),
    StructField("end_reason_1", StringType, true),
    StructField("terminated_by_1", StringType, true),
    StructField("terminate_src_ip", StringType, true),
    StructField("terminate_src_ip_1", StringType, true),
    StructField("dpi_node_ip", StringType, true),
    StructField("call_date", StringType, true),
    StructField("call_hour", StringType, true)
  ))

  /** Load schema từ file JSON trên HDFS */
  private def loadSchemaFromJson(spark: SparkSession, path: String): StructType = {
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val stream = fs.open(new org.apache.hadoop.fs.Path(path))
    val schemaSource = Source.fromInputStream(stream).getLines().mkString
    stream.close()
    DataType.fromJson(schemaSource).asInstanceOf[StructType]
  }

  /** Cast DataFrame từ raw string schema sang schema chuẩn */
  private def castToSchema(dfRaw: DataFrame, targetSchema: StructType): DataFrame = {
    targetSchema.fields.foldLeft(dfRaw) { (df, field) =>
      if (df.columns.contains(field.name)) {
        field.dataType match {
          case ArrayType(StringType, _) =>
            df.withColumn(field.name, split(regexp_replace(col(field.name), "[\\[\\]]", ""), ","))
          case _ =>
            df.withColumn(field.name, col(field.name).cast(field.dataType))
        }
      } else {
        df.withColumn(field.name, lit(null).cast(field.dataType))
      }
    }
  }

  /** Batch job: đọc CSV → cast schema → ghi Hudi → lấy metrics */
  private def BatchProcessing(
                               spark: SparkSession,
                               dfRaw: DataFrame,
                               tsValue: java.sql.Timestamp,
                               targetSchema: StructType,
                               filePath: String
                             ): Unit = {
    val fileName = filePath.split("/").last.split("\\.")(0)
    val inputDb = "schema1_db"
    val inputTable = fileName.replaceAll("_\\d{8}_\\d{4}-\\d{4}$", "")
    val storagePath = s"hdfs://localhost:9000/hudi/src/$inputTable"
    val metricsDb = "autoDQ_DB"
    val metricsTable = s"autoDQ_metrics"
    val metricsPath = s"hdfs://localhost:9000/hudi/$metricsTable"

    println(s"[DEBUG] inputDb = $inputDb")
    println(s"[DEBUG] inputTable = $inputTable")
    println(s"[DEBUG] storagePath = $storagePath")
    println(s"[DEBUG] metricsDb = $metricsDb")
    println(s"[DEBUG] metricsTable = $metricsTable")
    println(s"[DEBUG] metricsPath = $metricsPath")

    val df = castToSchema(dfRaw, targetSchema)
      .withColumn("processing_time", lit(tsValue))

    val HUDI_INPUT_OPTIONS = Map(
      "hoodie.table.name" -> inputTable,
      "hoodie.metadata.enable" -> "false",
      "hoodie.datasource.hive_sync.metastore.uris" -> "thrift://localhost:9083",
      "hoodie.metadata.enable" -> "false",
      "hoodie.write.markers.type" -> "direct",
      "hoodie.metrics.on" -> "true",
      "hoodie.metrics.reporter.type" -> "PROMETHEUS_PUSHGATEWAY",
      "hoodie.metrics.pushgateway.host" -> "localhost",
      "hoodie.metrics.pushgateway.port" -> "9091",
      "hoodie.metrics.pushgateway.delete.on.shutdown" -> "false",
      "hoodie.metrics.pushgateway.job.name" -> JOB_NAME,
      "hoodie.metrics.pushgateway.report.labels" ->
        s"table:${inputTable.replaceAll("[^A-Za-z0-9_]", "_")},database:${inputDb.replaceAll("[^A-Za-z0-9_]", "_")}",

      "hoodie.metrics.pushgateway.random.job.name.suffix" -> "false",
      "hoodie.datasource.hive_sync.enable" -> "true",
      "hoodie.datasource.hive_sync.mode" -> "hms",
      "hoodie.datasource.hive_sync.database" -> inputDb,
      "hoodie.datasource.hive_sync.table" -> inputTable,
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.operation" -> "insert"
    )

    // Ghi DataFrame xuống Hudi với cột processing_time
    println("___START_WRITE_HUDI___")

    df.write.format("hudi")
      .options(HUDI_INPUT_OPTIONS)
      .mode(WRITE_MODE)
      .save(storagePath)
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(JOB_NAME)
      .master(SPARK_MASTER)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .getOrCreate()

    /** Collect ts */
    //    val tsValue = java.sql.Timestamp.valueOf(java.time.LocalDateTime.now())
    val tsValueStrRaw = spark.conf.getOption("spark.ts.value").getOrElse {
      throw new IllegalArgumentException("Missing config: spark.ts.value")
    }
    val tsValueStr = tsValueStrRaw.replace("T", " ")
    val tsValue = java.sql.Timestamp.valueOf(tsValueStr)

    /** Load schema chuẩn từ file JSON trên HDFS (truyền qua --conf schema.path=hdfs://...) */
    val schemaPath = spark.conf.getOption("spark.schema.path").getOrElse {
      throw new IllegalArgumentException("Missing config: spark.schema.path")
    }
    val targetSchema = loadSchemaFromJson(spark, schemaPath)
    /** Load file path */
    val filePath = spark.conf.getOption("spark.file.path")
      .getOrElse(throw new IllegalArgumentException("Missing config: spark.file.path"))
    println(s"[DEBUG] inputTable = $filePath")


    /** Đọc dữ liệu thô (tất cả là String) */
    val dfRaw: DataFrame = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "false")
      .csv(filePath)

    if (dfRaw.isEmpty) {
      println("dfRaw is empty")
    }

    BatchProcessing(spark, dfRaw, tsValue, targetSchema, filePath)
  }
}
