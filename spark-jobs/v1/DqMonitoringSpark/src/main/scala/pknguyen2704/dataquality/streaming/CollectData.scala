package pknguyen2704.dataquality.streaming
import pknguyen2704.dataquality.streaming.querylistener.QueryListener
import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners._
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.prometheus.client.Gauge
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.PushGateway
import org.apache.spark.sql.streaming.Trigger


object CollectData {

  private final val JOB_NAME = "CollectData"
  private final val HUDI_BRONZE_BASE_PATH = "hdfs://namenode:9000/hudi/data/bronze"
  private final val PUSHGATEWAY_HOST = "pushgateway:9091"
  private final val KAFKA_TOPIC = "realtime_smart_home_data"
  private final val KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"

  // Define Data Schema
  private val schema = StructType(Seq(
    StructField("timestamp", TimestampType, nullable=true),
    StructField("home_id", IntegerType, nullable=true),
    StructField("device_id", StringType, nullable=true),
    StructField("device_type", StringType, nullable=true),
    StructField("room", StringType, nullable=true),
    StructField("status", StringType, nullable=true),
    StructField("power_watt", DoubleType, nullable=true),
    StructField("user_present", IntegerType, nullable=true),
    StructField("activity", StringType, nullable=true),
    StructField("indoor_temp", DoubleType, nullable=true),
    StructField("outdoor_temp", DoubleType, nullable=true),
    StructField("humidity", DoubleType, nullable=true),
    StructField("light_level", DoubleType, nullable=true),
    StructField("day_of_week", IntegerType, nullable=true),
    StructField("hour_of_day", IntegerType, nullable=true),
    StructField("price_kWh", IntegerType, nullable=true)
  ))

  // Config kafka connection
  private val kafkaOptions = Map(
    "kafka.bootstrap.servers" -> KAFKA_BOOTSTRAP_SERVERS,
    "subscribe" -> KAFKA_TOPIC,
    "startingOffsets" -> "latest"
  )

  //  Config hudi connection
  private val hudiOptions = Map(
    "checkpointLocation" -> s"${HUDI_BRONZE_BASE_PATH}_checkpoints/",
    "hoodie.table.name" -> "realtime_smart_home_data_bronze",
    "hoodie.metadata.enable" -> "false",
    "hoodie.write.markers.type" -> "direct",
    "hoodie.metrics.on" -> "true",
    "hoodie.metrics.reporter.type" -> "PROMETHEUS_PUSHGATEWAY",
    "hoodie.metrics.pushgateway.host" -> "pushgateway",
    "hoodie.metrics.pushgateway.port" -> "9091",
    "hoodie.metrics.pushgateway.delete.on.shutdown" -> "false",
    "hoodie.metrics.pushgateway.job.name" -> JOB_NAME,
    "hoodie.metrics.pushgateway.random.job.name.suffix" -> "false",
    "hoodie.datasource.hive_sync.metastore.uris" -> "thrift://hive-metastore:9083",
    "hoodie.datasource.hive_sync.enable" -> "true",
    "hoodie.datasource.hive_sync.mode" -> "hms",
    "hoodie.datasource.hive_sync.table" -> "realtime_smart_home_data_bronze",
    "hoodie.datasource.hive_sync.database" -> "smart_home_data_bronze",
    "hoodie.datasource.write.recordkey.field" -> "home_id,device_id,timestamp",
    "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
    "hoodie.datasource.write.operation" -> "insert",
  )

  //  Define Deequ Analysis
  private def DqAnalyzer(df: DataFrame): AnalyzerContext = {
    val sizeAnalyzer = Seq(Size())
    val analyzers = df.schema.fields.flatMap { field =>
      val colName = field.name
      val dataType = field.dataType

      // Base analyzer for all columns
      val baseAnalyzers = Seq(
        Completeness(colName),
        ApproxCountDistinct(colName),
        Entropy(colName) // make job run slower =(((
      )
      // Specific analyzer for each column
      val typeSpecificAnalyzers = dataType match {
        case _: NumericType =>
          Seq(
            Minimum(colName),
            Maximum(colName),
            Mean(colName),
            Sum(colName),
            StandardDeviation(colName),
          )
        case StringType =>
          Seq(
            MaxLength(colName),
            MinLength(colName)
          )
        case _ => Seq.empty
      }

      baseAnalyzers ++ typeSpecificAnalyzers
    }
    // Custom analysis using compliance or define multi columns like Correlation
    val customAnalysis = Seq(
      Correlation("indoor_temp", "outdoor_temp"),
      Correlation("indoor_temp", "humidity"),
      Correlation("outdoor_temp", "humidity"),
      Compliance("valid_status", "status IN ('on', 'off')"),
      Compliance("power_watt_positive", "power_watt >= 0"),
      Compliance("indoor_temp_range", "indoor_temp >= -10 AND indoor_temp <= 50"),
      Compliance("outdoor_temp_range", "outdoor_temp >= -30 AND outdoor_temp <= 60"),
      Compliance("humidity_percentage", "humidity >= 0 AND humidity <= 50"),
      Compliance("day_of_week_range", "day_of_week >= 0 AND day_of_week <= 6"),
      Compliance("hour_of_day_range", "hour_of_day >= 0 AND hour_of_day <= 23"),
      Compliance("device_type_not_null", "device_type IS NOT NULL AND device_type != ''"),
      Compliance("activity_valid", "activity IS NOT NULL AND length(activity) > 0"),
      Compliance("price_kWh_positive", "price_kWh >= 0"),
    )

  AnalysisRunner
    .onData(df)
    .addAnalyzers(sizeAnalyzer ++ analyzers ++ customAnalysis)
    .run()
  }

  // Push analyzer metric to push gateway
  private def PushAnalyzerMetric(analyzerResultDF: DataFrame, pushGateWayUrl: String): Unit = {
    val registry = new CollectorRegistry()

    val gaugeDqAnalyzerResult = Gauge.build()
      .name("dq_analyzer_result")
      .help("Data quality analyze per micro batch")
      .labelNames("entity", "instance", "metric_name")
      .register(registry)

    analyzerResultDF.collect().foreach { row =>
      val entity = row.getAs[String]("entity")
      val instance = row.getAs[String]("instance")
      val metric_name = row.getAs[String]("name")
      val value = row.getAs[Double]("value")

      gaugeDqAnalyzerResult.labels(entity, instance, metric_name).set(value)
    }
    val pushGatewayAnalyzer = new PushGateway(pushGateWayUrl)
    pushGatewayAnalyzer.pushAdd(registry, JOB_NAME)
  }
  private def BatchProcessing(spark: SparkSession, batch_df: DataFrame): Unit = {
    batch_df.cache()
    PushAnalyzerMetric(successMetricsAsDataFrame(spark, DqAnalyzer(batch_df)), PUSHGATEWAY_HOST)
    batch_df.write
      .format("hudi")
      .options(hudiOptions)
      .mode("append")
      .save(HUDI_BRONZE_BASE_PATH)

    batch_df.unpersist()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(JOB_NAME)
      .master("spark://spark-master:7077")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .config("spark.executor.instances", "2")
      .config("spark.executor.cores", "2")
      .config("spark.executor.memory", "1500m")
      .config("spark.driver.memory", "1g")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val metricListener = new QueryListener(PUSHGATEWAY_HOST, JOB_NAME)
    spark.streams.addListener(metricListener)
    //    ReadStream
    val line = spark.readStream
      .format("kafka")
      .options(kafkaOptions)
      .load()

    //    Flatten Json input line
    val parsedDF = line.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")

    // WriteStream
    val query = parsedDF.writeStream
      .queryName(JOB_NAME)
      .trigger(Trigger.ProcessingTime("2 second"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        BatchProcessing(spark, batchDF)
      }
      .start()

    query.awaitTermination()
  }
}
