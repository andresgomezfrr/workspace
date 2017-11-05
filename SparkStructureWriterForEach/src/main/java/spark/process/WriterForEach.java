package spark.process;
import java.io.Serializable;
import java.util.Properties;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.ForeachWriter;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.spark.sql.fieldTypes.api.java.Timestamp;

public class WriterForEach  extends ForeachWriter<Row> implements Serializable{
	private static String mongodbIp = "192.168.1.225";
	private static String dbName = "wdb";
	private static String collectionName = "wtable";
	MongoClient mongoclient;
	DBCollection table1;
	DB db;
	BasicDBObject document;
	@Override
	public void close(Throwable arg0) {
		this.mongoclient.close();
		
		
	}

	@Override
	public boolean open(long arg0, long arg1) {
		this.mongoclient = new MongoClient(mongodbIp, 27017);
		return false;
	}

	@Override
	public void process(org.apache.spark.sql.Row fila) {
		this.db = this.mongoclient.getDB(dbName);
		this.table1 = this.db.getCollection("prueba1");
		this.document = new BasicDBObject();
		
		document.put("numIds", fila.get(0));
		document.put("idCity", fila.get(1));
		document.put("datetime", new Timestamp());
		this.table1.insert(document);
		
	}

}
