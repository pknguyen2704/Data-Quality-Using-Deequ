package pknguyen2704.dataquality.streaming.querylistener
import org.apache.spark.sql.streaming.StreamingQueryListener
import io.prometheus.client.{CollectorRegistry, Gauge}
import io.prometheus.client.exporter.PushGateway
import com.github.wnameless.json.flattener.JsonFlattener
import java.util
import scala.jdk.CollectionConverters._

class QueryListener(pushGatewayUrl: String, jobName: String) extends StreamingQueryListener {
  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    try {
      val progress = event.progress
      val jsonStr = progress.json

      // Flatten JSON string into Map[String, Object]
      val flattened: util.Map[String, Object] = JsonFlattener.flattenAsMap(jsonStr)
      val flatMetrics: Map[String, Double] = flattened.asScala.flatMap {
        case (key, value) =>
          try {
            val doubleVal = value.toString.toDouble
            Some(key -> doubleVal)
          } catch {
            case _: Throwable => None
          }
      }.toMap

      val registry = new CollectorRegistry()

      val gauge = Gauge.build()
        .name("streaming_query_listener")
        .help(s"Streaming metric streaming_query_listener")
        .labelNames("instance")
        .register(registry)
      flatMetrics.foreach { case (metricName, metricValue) =>
        val instance = metricName.replaceAll("[^a-zA-Z0-9_]", "_")
        gauge.labels(instance).set(metricValue)
      }

      val pushGateway = new PushGateway(pushGatewayUrl)
      pushGateway.pushAdd(registry, jobName)

    } catch {
      case e: Exception =>
        println(s"[CustomError] Failed to push metrics: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    println(s"[CustomQueryListener] Query started: ${event.name}, id: ${event.id}")
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println(s"[CustomQueryListener] Query terminated id: ${event.id}")
  }

}