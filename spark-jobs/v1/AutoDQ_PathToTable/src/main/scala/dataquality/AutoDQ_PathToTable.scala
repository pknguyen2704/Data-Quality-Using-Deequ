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

object AutoDQ_PathToTable {
  private final val JOB_NAME = "autoDQ_PathToTable"
  private final val SPARK_MASTER = "spark://bigdataplatform.asia-southeast1-a.c.data-quality-project-470101.internal:7077"
  private final val WRITE_MODE = "append"
//  private final val FILE_PATH = "hdfs://localhost:9000/sftp/simulation_with_schema1_sample.csv"
//  private final val INPUT_TABLE = "simulation_with_schema1_sample"
//  private final val STORAGE_PATH = "hdfs://localhost:9000/hudi/src_sample/simulation_with_schema1_sample"
//  private final val METRICS_DB = "autoDQ_DB"
//  private final val METRICS_TABLE = "autoDQ_metrics_sample"
//  private final val METRICS_PATH = "hdfs://localhost:9000/hudi/autoDQ_metrics_sample"



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

  /** Phân tích dữ liệu với Deequ */
  private def DeequAnalyzer(
                             df: DataFrame,
                             spark: SparkSession,
                             filePath: String,
                             inputTable: String,
                             timestamp: java.sql.Timestamp
                           ): DataFrame = {
    val sizeAnalyzer = Seq(Size())
    val analyzers = df.schema.fields.flatMap { field =>
      val colName = field.name
      val dataType = field.dataType
      val baseAnalyzers = Seq(
        Completeness(colName),
        ApproxCountDistinct(colName)
      )
      val typeSpecificAnalyzers = dataType match {
        case _: NumericType =>
          Seq(Minimum(colName), Maximum(colName), Mean(colName), Sum(colName))
        case StringType =>
          Seq(MaxLength(colName), MinLength(colName))
        case _ => Seq.empty
      }
      baseAnalyzers ++ typeSpecificAnalyzers
    }

    val analysisResult: AnalyzerContext = AnalysisRunner
      .onData(df)
      .addAnalyzers(sizeAnalyzer ++ analyzers)
      .run()

    val metricsDf = successMetricsAsDataFrame(spark, analysisResult)

    metricsDf
      .withColumn("timestamp", lit(timestamp).cast(TimestampType)) // ép chuẩn Timestamp
      .withColumn("path", lit(filePath))
      .withColumn("table", lit(inputTable))
      .withColumn(
        "type",
        when(col("entity") === "Dataset", lit("Dataset"))
          .when(col("entity") === "Column", lit("Column"))
          .otherwise(lit("unknown"))
      )
      .withColumnRenamed("instance", "column")
      .withColumnRenamed("name", "metric")
      .select("timestamp", "path", "table", "type", "column", "metric", "value")
  }

  /** Thu thập đầy đủ metrics từ commit cuối cùng của Hudi */
  private def CollectHudiMetrics(
                                  spark: SparkSession,
                                  tablePath: String,
                                  inputTable: String,
                                  timestamp: java.sql.Timestamp
                                ): DataFrame = {
    import spark.implicits._


    // 1. Khởi tạo metaClient
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val storageConf = HadoopFSUtils.getStorageConf(hadoopConf)
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(storageConf)
      .setBasePath(tablePath)
      .build()

    // 2. Lấy commit cuối cùng
    val timeline = metaClient.getCommitsTimeline.filterCompletedInstants()
    if (timeline.empty()) {
      return Seq.empty[(java.sql.Timestamp, String, String, String, String, String, String)].toDF(
        "timestamp", "path", "table", "type", "column", "metric", "value"
      )
    }
    val lastInstant = timeline.lastInstant().get()
    val commitMetadata = HoodieCommitMetadata.fromBytes(
      timeline.getInstantDetails(lastInstant).get(),
      classOf[HoodieCommitMetadata]
    )

    val extraMetrics = commitMetadata.getExtraMetadata.asScala.toSeq.map {
      case (metric, value) =>
        (timestamp, tablePath, inputTable, "HudiCommit", null.asInstanceOf[String], metric, value)
    }

    val summaryMetrics = Seq(
      ("totalInserts", commitMetadata.fetchTotalInsertRecordsWritten().toString),
      ("totalUpdates", commitMetadata.fetchTotalUpdateRecordsWritten().toString),
      ("totalRecordsWritten", commitMetadata.fetchTotalRecordsWritten().toString),
      ("totalBytesWritten", commitMetadata.fetchTotalBytesWritten().toString),
      ("operationType", commitMetadata.getOperationType.toString)
    ).map { case (metric, value) =>
      (timestamp, tablePath, inputTable, "HudiCommitSummary", null.asInstanceOf[String], metric, value)
    }

    val groupedWriteStats = commitMetadata.getWriteStats.asScala
      .groupBy(_.getPartitionPath)
      .flatMap { case (partition, stats) =>
        val numInserts = stats.map(_.getNumInserts).sum
        val numWrites = stats.map(_.getNumWrites).sum
        val numDeletes = stats.map(_.getNumDeletes).sum
        val fileSizeInBytes = stats.map(_.getFileSizeInBytes).sum

        Seq(
          (timestamp, tablePath, inputTable, "HudiWriteStat", partition, "numInserts", numInserts.toString),
          (timestamp, tablePath, inputTable, "HudiWriteStat", partition, "numWrites", numWrites.toString),
          (timestamp, tablePath, inputTable, "HudiWriteStat", partition, "numDeletes", numDeletes.toString),
          (timestamp, tablePath, inputTable, "HudiWriteStat", partition, "fileSizeInBytes", fileSizeInBytes.toString)
        )
      }.toSeq

    val allMetrics = extraMetrics ++ summaryMetrics ++ groupedWriteStats

    allMetrics.toDF("timestamp", "path", "table", "type", "column", "metric", "value")
      .withColumn("timestamp", col("timestamp").cast(TimestampType))
  }

//  private def ManageAlert() = {
//
//  }
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
      "hoodie.datasource.hive_sync.enable" -> "true",
      "hoodie.datasource.hive_sync.mode" -> "hms",
      "hoodie.datasource.hive_sync.database" -> inputDb,
      "hoodie.datasource.hive_sync.table" -> inputTable,
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.operation" -> "insert"
    )

    val HUDI_METRICS_TABLE_OPTIONS = Map (
      "hoodie.table.name" -> metricsTable,
      "hoodie.metadata.enable" -> "false",
      "hoodie.datasource.hive_sync.metastore.uris" -> "thrift://localhost:9083",
      "hoodie.datasource.hive_sync.enable" -> "true",
      "hoodie.datasource.hive_sync.mode" -> "hms",
      "hoodie.datasource.hive_sync.database" -> metricsDb,
      "hoodie.datasource.hive_sync.table" -> metricsTable,
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.operation" -> "insert"
    )

    println("___START_DEEQU_ANALYZER___")
    val autoDQ_Metrics = DeequAnalyzer(df, spark, filePath, inputTable, tsValue)

    // Ghi DataFrame xuống Hudi với cột processing_time
    println("___START_WRITE_HUDI___")

    df.write.format("hudi")
      .options(HUDI_INPUT_OPTIONS)
      .mode(WRITE_MODE)
      .save(storagePath)

    println("___START_COLLECT_HUDI_METRICS___")
    val hudiMetrics = CollectHudiMetrics(spark, storagePath, inputTable, tsValue)

    val allMetrics = autoDQ_Metrics.unionByName(hudiMetrics)
      .withColumn("timestamp_str", date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

    allMetrics.write.format("hudi")
      .options(HUDI_METRICS_TABLE_OPTIONS)
      .mode(WRITE_MODE)
      .save(metricsPath)
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