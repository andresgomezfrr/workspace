package spark.process;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.spark.sql.fieldTypes.api.java.Timestamp;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

import java.io.Serializable;

public class PrinterForEach extends ForeachWriter<Row> implements Serializable{
	private static String mongodbIp = "192.168.1.225";
	private static String dbName = "wdb";
	private static String collectionName = "wtable";
	MongoClient mongoclient;
	DBCollection table1;
	DB db;
	BasicDBObject document;
	@Override
	public void close(Throwable arg0) {

	}

	@Override
	public boolean open(long arg0, long arg1) {
		return true;
	}

	@Override
	public void process(Row fila) {
		System.out.println("ROW: " + fila.toString());
	}

}
