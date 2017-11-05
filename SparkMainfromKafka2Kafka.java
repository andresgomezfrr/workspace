package spark.process;

import com.databricks.spark.avro.SchemaConverters;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

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
import org.apache.spark.sql.types.*;
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

import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import org.apache.spark.sql.streaming.ProcessingTime;

import org.apache.spark.sql.SparkSession.implicits$;
import org.apache.spark.SparkConf.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession.*;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.catalog.Column;

public class SparkMainfromKafka2Kafka {

	private static Injection<GenericRecord, byte[]> recordInjection;
	// CCSprivate static String mongodbIp ="172.31.30.154";
	// CCS private static String mongodbIp ="54.175.19.216";
	private static String mongodbIp = "192.168.1.225";
	private static String dbName = "wdb";
	private static String collectionName = "wtable";
	private static StructType type;
    public static String IPKAFKA="34.201.106.47";
	
	public static String KAFKA_HOST = IPKAFKA+":9090,"+IPKAFKA+":9091,"+IPKAFKA+":9092";
	public static String TOPICIN = "datos";
	public static String TOPICOUT = "topicFromKafkaToMongo";
	
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
		 
		LogManager.getLogger("org.apache.spark").setLevel(Level.WARN);
		LogManager.getLogger("akka").setLevel(Level.ERROR);
		

		

		SparkConf conf = new SparkConf().setAppName("sparkprocess").setMaster("local[*]");
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

	
		sparkSession.sqlContext().setConf("spark.sql.shuffle.partitions", "3");
		StructType schemaApp1 = new StructType();
		schemaApp1.add("lon", DataTypes.StringType);
		schemaApp1.add("lat", DataTypes.StringType);
		schemaApp1.add("temp_max", DataTypes.StringType);
		schemaApp1.add("temp_min", DataTypes.StringType);
		schemaApp1.add("temp", DataTypes.StringType);
		schemaApp1.add("pressure", DataTypes.StringType);
		schemaApp1.add("humidity", DataTypes.StringType);
		schemaApp1.add("id", DataTypes.StringType);
		schemaApp1.add("datetime", DataTypes.StringType);
		
		StructType struct1 = new StructType();
		struct1.add("lon", DataTypes.StringType);
		struct1.add("lat", DataTypes.StringType);
		struct1.add("tempmax", DataTypes.StringType);
		struct1.add("datetime", DataTypes.StringType);
		
		//Column(String name, String description, String dataType, boolean nullable, boolean isPartition, boolean isBucket) 
		
		org.apache.spark.sql.Column valueJson = new org.apache.spark.sql.Column("value");//("value", "value json", DataTypes.StringType, true, false, false);
		org.apache.spark.sql.Column keyJson = new org.apache.spark.sql.Column("key");
	
		Dataset<Row> ds1Read = 
				sparkSession
				.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", KAFKA_HOST)
				.option("subscribe", TOPICIN)
				.option("startingOffsets", "earliest").load()
				.select(keyJson.cast(DataTypes.StringType).alias("idCity"))
			    .select(org.apache.spark.sql.functions.from_json(valueJson, schemaApp1)).alias("parsed_value")
			    .select("idCity","parsed_value.lon","parsed_value.lat", "parsed_value.temp", "parsed_value.datetime");
		ds1Read.printSchema();
		
		Dataset<Row> ds2Write = ds1Read;
		
		
		StreamingQuery query = ds2Write
		.groupBy(ds2Write.col("idCity"),ds2Write.col("lon"),ds2Write.col("lat"))
		.max((Seq<String>) ds2Write.col("temp")).alias("maxtemp")			
		.select(ds2Write.col("idCity").cast("String").alias("key"),
		 org.apache.spark.sql.functions
		         .to_json(org.apache.spark.sql.functions
				 .struct("lon", "lat","maxtemp","datetime")).alias("value"))		
		.writeStream()
		.format("kafka")
		.option("kafka.bootstrap.servers", KAFKA_HOST)
		.option("topic", TOPICOUT)
		.option("checkpointLocation", "/tmp/structured-streaming")
		.outputMode("update")
		.trigger(ProcessingTime.create(3, TimeUnit.SECONDS))
		.start();
        query.awaitTermination();
        
 
		
		


	}
}
