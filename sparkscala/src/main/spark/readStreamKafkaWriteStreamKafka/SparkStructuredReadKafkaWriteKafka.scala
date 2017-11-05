package spark.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{ col, _ }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object mainObject {
  val conf1 = new SparkConf().setMaster("local[2]").setAppName("StructuredStreaming")
  val IPKAFKA = "34.201.106.47";
  val KAFKA_HOST = IPKAFKA + ":9090," + IPKAFKA + ":9091," + IPKAFKA + ":9092"

  val TOPICIN_ = "datos"
  val TOPICOUT_ = "topicFromKafkaToMongo"

  def exec() {

    val sparksession = SparkSession
      .builder
      .config(conf1)
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    val schemaApp2 = new StructType()
      .add("lon", StringType)
      .add("lat", StringType)
      .add("temp_max", StringType)
      .add("temp_min", StringType)
      .add("temp", StringType)
      .add("pressure", IntegerType)
      .add("humidity", IntegerType)
      .add("id", StringType)
      .add("datetime", StringType)

    val dfApp2 = sparksession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_HOST_)
      .option("subscribe", TOPICIN_)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()
      .select(
        col("key").cast("string").alias("idCity"),
        from_json(col("value").cast("string"), schemaApp2).alias("parsed_value"))
      .select("idCity", "parsed_value.lon", "parsed_value.lat", "parsed_value.temp", "parsed_value.datetime")

    dfApp2.printSchema()

    import spark.implicits._

    val query2 = dfApp2
      .withWatermark("timestamp", "10 minutes")
      .groupBy($"idCity", $"lon", $"lat", window($"datetime", "1 minutes", "1 minutes"))
      .agg(max($"temp").alias("maximotemp"))
      .select(
        col("appClientId").cast("string").alias("key"),
        to_json(struct("app", "location", "sum_bytes", "window.start")).alias("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_HOST)
      .option("topic", TOPICOUT)
      .option("checkpointLocation", "/tmp/structured-streaming")
      .outputMode("update")
      .start()

    query1.awaitTermination()

  }

  def main(args: Array[String]) {
    exec()
    println("hola")

  }
}
