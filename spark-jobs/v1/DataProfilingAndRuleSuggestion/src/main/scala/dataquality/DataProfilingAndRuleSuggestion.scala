package dataquality

import com.amazon.deequ.profiles.{ColumnProfilerRunner, ColumnProfiles}
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.writePretty

import java.io.PrintWriter
import java.sql.Timestamp
import java.time.{Instant, LocalDate, ZoneId}
import java.time.format.DateTimeFormatter

object DataProfilingAndRuleSuggestion {
  private final val JOB_NAME = "DataProfilingAndRuleSuggestion"
  private final val HUDI_DATA_PATH = "hdfs://namenode:9000/hudi/data/bronze"
  private final val HUDI_PROFILING_PATH = "hdfs://namenode:9000/hudi/data/profiling"
  private final val OUTPUT_JSON_DIR = "/opt/bitnami/spark/results"

  private val profilingSchema = StructType(Seq(
    StructField("profilingTimestamp", TimestampType, nullable = false),
    StructField("dataTimestamp", TimestampType, nullable = false),
    StructField("profilingTimestamp_Formatted", StringType, nullable = false),
    StructField("dataTimestamp_Formatted", StringType, nullable = false),
    StructField("column", StringType, nullable = false),
    StructField("data_type", StringType, nullable = false),
    StructField("metric", StringType, nullable = false),
    StructField("value", DoubleType, nullable = false)
  ))

  private val hudiOptions = Map(
    "checkpointLocation" -> s"${HUDI_PROFILING_PATH}_checkpoints/",
    "hoodie.table.name" -> "silver_yellow_trip_data_profiling",
    "hoodie.metadata.enable" -> "false",
    "hoodie.datasource.hive_sync.metastore.uris" -> "thrift://hive-metastore:9083",
    "hoodie.datasource.hive_sync.enable" -> "true",
    "hoodie.datasource.hive_sync.mode" -> "hms",
    "hoodie.datasource.hive_sync.database" -> "yellow_trip_data",
    "hoodie.datasource.hive_sync.table" -> "silver_yellow_trip_data_profiling",
    "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
    "hoodie.datasource.write.operation" -> "insert"
  )

  def profileToRows(result: ColumnProfiles, profilingTime: Timestamp, dataTimestamp: Timestamp, cleanedDF: DataFrame): Seq[Row] = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    val totalRows = cleanedDF.count()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("Asia/Ho_Chi_Minh"))
    val profilingTimeStr = formatter.format(profilingTime.toInstant)
    val dataTimeStr = formatter.format(dataTimestamp.toInstant)

    val columnMetrics = result.profiles.toSeq.flatMap { case (colName, profile) =>
      val dataTypeStr = profile.dataType.toString
      val baseMetrics = Seq(
        "completeness" -> profile.completeness,
        "approximateNumDistinctValues" -> profile.approximateNumDistinctValues.toDouble,
        "nullCount" -> Math.round(totalRows * (1.0 - profile.completeness)).toDouble
      )

      val typeSpecificMetrics = profile match {
        case p: com.amazon.deequ.profiles.NumericColumnProfile =>
          Seq(
            "mean" -> p.mean.getOrElse(Double.NaN),
            "maximum" -> p.maximum.getOrElse(Double.NaN),
            "minimum" -> p.minimum.getOrElse(Double.NaN),
            "stdDev" -> p.stdDev.getOrElse(Double.NaN),
            "sum" -> p.sum.getOrElse(Double.NaN)
          )
        case p: com.amazon.deequ.profiles.StringColumnProfile =>
          Seq(
            "minLength" -> p.minLength.map(_.toDouble).getOrElse(Double.NaN),
            "maxLength" -> p.maxLength.map(_.toDouble).getOrElse(Double.NaN)
          )
        case _ => Seq()
      }

      (baseMetrics ++ typeSpecificMetrics).map { case (metric, value) =>
        Row(
          profilingTime,
          dataTimestamp,
          profilingTimeStr,
          dataTimeStr,
          colName,
          dataTypeStr,
          metric,
          value
        )
      }
    }

    val sizeMetricRow = Row(
      profilingTime,
      dataTimestamp,
      profilingTimeStr,
      dataTimeStr,
      "",
      "",
      "Size",
      totalRows.toDouble
    )

    columnMetrics :+ sizeMetricRow
  }

  /** Lưu dữ liệu JSON ra file */
  def saveJsonToFile(jsonStr: String, path: String): Unit = {
    val writer = new PrintWriter(path)
    try {
      writer.write(jsonStr)
    } finally {
      writer.close()
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    val spark = SparkSession.builder()
      .appName(JOB_NAME)
      .master("spark://spark-master:7077")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val inputDateStr = args(0)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    val startTimestamp = Timestamp.valueOf(LocalDate.parse(inputDateStr, formatter).atStartOfDay())
    val endTimestamp = Timestamp.valueOf(LocalDate.parse(inputDateStr, formatter).plusDays(1).atStartOfDay())

    val df = spark.read.format("hudi")
      .load(HUDI_DATA_PATH)
      .filter(col("tpep_dropoff_datetime").geq(startTimestamp) && col("tpep_dropoff_datetime").lt(endTimestamp))

    val cleanedDF = df.drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
      "_hoodie_partition_path", "_hoodie_file_name")

    val profilingTime = Timestamp.from(Instant.now())

    // 1. Chạy ColumnProfilerRunner
    val profileResult = ColumnProfilerRunner()
      .onData(cleanedDF)
      .run()

    // Lưu kết quả profiling của Deequ ra file JSON
    val profileJson = writePretty(profileResult.profiles.map { case (col, prof) => col -> prof })
    saveJsonToFile(profileJson, s"$OUTPUT_JSON_DIR/profiling_${inputDateStr}.json")

    // 2. Chạy Constraint Suggestion
    val suggestionResult = ConstraintSuggestionRunner()
      .onData(cleanedDF)
      .addConstraintRules(Rules.DEFAULT)
      .run()

    val suggestionJson = writePretty(suggestionResult.constraintSuggestions)
    saveJsonToFile(suggestionJson, s"$OUTPUT_JSON_DIR/suggestions_${inputDateStr}.json")

    // 3. Lưu kết quả profiling dạng bảng vào Hudi
    val rows = profileToRows(profileResult, profilingTime, startTimestamp, cleanedDF)
    val profilingDF = spark.createDataFrame(spark.sparkContext.parallelize(rows), profilingSchema)

    profilingDF.write.format("hudi")
      .options(hudiOptions)
      .mode("append")
      .save(HUDI_PROFILING_PATH)

    spark.stop()
  }
}
