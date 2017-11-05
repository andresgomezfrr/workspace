package spark.mongo;

import java.io.Serializable;

public class SchemaAvro implements Serializable{

	
	private  Integer lon;
	private  Integer lat;
	private  Integer temp;
	private  Integer  pressure;
	private  Integer  humidity;
	private  Integer  temp_min;
	private  Integer temp_max;
	private  Integer  id;
	private  Integer  datetime;
	public Integer getLon() {
		return lon;
	}
	public void setLon(Integer lon) {
		this.lon = lon;
	}
	public Integer getLat() {
		return lat;
	}
	public void setLat(Integer lat) {
		this.lat = lat;
	}
	public Integer getTemp() {
		return temp;
	}
	public void setTemp(Integer temp) {
		this.temp = temp;
	}
	public Integer getPressure() {
		return pressure;
	}
	public void setPressure(Integer pressure) {
		this.pressure = pressure;
	}
	public Integer getHumidity() {
		return humidity;
	}
	public void setHumidity(Integer humidity) {
		this.humidity = humidity;
	}
	public Integer getTemp_min() {
		return temp_min;
	}
	public void setTemp_min(Integer temp_min) {
		this.temp_min = temp_min;
	}
	public Integer getTemp_max() {
		return temp_max;
	}
	public void setTemp_max(Integer temp_max) {
		this.temp_max = temp_max;
	}
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public Integer getDatetime() {
		return datetime;
	}
	public void setDatetime(Integer datetime) {
		this.datetime = datetime;
	}
	
}
