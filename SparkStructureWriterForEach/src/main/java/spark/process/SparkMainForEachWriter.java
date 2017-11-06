package spark.process;

import com.databricks.spark.avro.SchemaConverters;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.spark.sql.streaming.*;
import scala.Function1;
import scala.collection.Seq;
import scala.collection.TraversableOnce;
import scala.runtime.BoxedUnit;
import spark.mongo.SchemaAvro;
import spark.mongo.weather;

import java.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

import org.apache.spark.sql.SparkSession.implicits$;
import org.apache.spark.SparkConf.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession.*;
import org.apache.spark.sql.types.*;

public class SparkMainForEachWriter {

	private static Injection<GenericRecord, byte[]> recordInjection;
	// CCSprivate static String mongodbIp ="172.31.30.154";
	// CCS private static String mongodbIp ="54.175.19.216";
	private static String mongodbIp = "192.168.1.225";
	private static String dbName = "wdb";
	private static String collectionName = "wtable";
	private static StructType type;
	public static String IPKAFKA="192.168.1.106:9092";
	
	public static String KAFKA_HOST = IPKAFKA+":9090,"+IPKAFKA+":9091,"+IPKAFKA+":9092";
	public static String TOPIC = "wtopic";
	public static String TOPIC2MONGO = "topicFromKafkaToMongo";
	public final static String WEATHER_SCHEMA = "{" + "\"type\":\"record\"," + "\"name\":\"myrecord\"," + "\"fields\":["
			+ "{\"name\":\"lon\",\"type\":\"int\"}," + "{\"name\":\"lat\",\"type\":\"int\"},"
			+ "{\"name\":\"temp\",\"type\":\"int\"}," + "{\"name\":\"pressure\",\"type\":\"int\"},"
			+ "{\"name\":\"humidity\",\"type\":\"int\"}," + "{\"name\":\"temp_min\",\"type\":\"int\"},"
			+ "{\"name\":\"temp_max\",\"type\":\"int\"}," + "{\"name\":\"id\",\"type\":\"int\"},"
			+ "{\"name\":\"datetime\",\"type\":\"int\"}" + "]" + "}";
	private static Schema.Parser parser = new Schema.Parser();
	private static Schema schema = parser.parse(WEATHER_SCHEMA);

	static {
		recordInjection = GenericAvroCodecs.toBinary(schema);
		type = (StructType) SchemaConverters.toSqlType(schema).dataType();

	}

	public static void main(String[] args) throws StreamingQueryException, Exception {
		SparkConf conf = new SparkConf().setAppName("sparkprocess").setMaster("local[*]");
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
	
		sparkSession.sqlContext().setConf("spark.sql.shuffle.partitions", "3");

	
		Dataset<Row> ds1 = sparkSession.readStream().format("kafka").option("kafka.bootstrap.servers", IPKAFKA)
				.option("subscribe", TOPIC).option("startingOffsets", "earliest").load();

		
		sparkSession.udf().register("deserialize", (byte[] data) -> {
			GenericRecord record = recordInjection.invert(data).get();
			return RowFactory.create(record.get("id"), record.get("lon"), record.get("lat"),
					record.get("temp"), record.get("pressure"), record.get("humidity"),
					record.get("temp_min"), record.get("temp_max"),
					record.get("datetime"));

		}, DataTypes.createStructType(type.fields()));
		
		
		
		Dataset<Row> ds2 = ds1.select("value").as(Encoders.BINARY()).selectExpr("deserialize(value) as rows")
				.select("rows.*");


		ds2.printSchema();
		Dataset<Row> ds3 = ds2.groupBy(ds2.col("id"),ds2.col("lon"),ds2.col("lat"))
				.max("temp").alias("maxtemp");
		ds3.printSchema();

		StreamingQuery query2 =
				ds3
				.select(ds3.col("id").alias("idCity"),
				ds3.col("lon").alias("Longitud").cast("Double"),
				ds3.col("lat").alias("Latitud").cast("Double"),
				ds3.col("max(temp)"))
				.writeStream()
				.foreach(new WriterForEach())
				.outputMode(OutputMode.Complete())
				.trigger(ProcessingTime.create(1, TimeUnit.SECONDS))
				.start();

		query2.awaitTermination();
	}
}
