package spark.mongo;

import java.io.Serializable;

public class weather implements Serializable{
	private Integer idCity;
	private Integer longitud;
	private Integer latitud;
	private Integer tempmax;
	private Integer tempmin;
    private Integer humedad;
    private Integer presion;
    private Integer datetime;
  /*{"id":1273840,
   * "lon": 77.22,
   * "lat": 28.63,
   * "temp_max": 304.15, 
   * "temp_min": 304.15,
   * "pressure": 1015,
   * "humidity": 33,
   * "datetime": 1509352200}  */
    
	

	public Integer getIdCity() {
		return idCity;
	}

	public void setIdCity(Integer idCity) {
		this.idCity = idCity;
	}

	public Integer getLongitud() {
		return longitud;
	}

	public void setLongitud(Integer longitud) {
		this.longitud = longitud;
	}

	public Integer getLatitud() {
		return latitud;
	}

	public void setLatitud(Integer latitud) {
		this.latitud = latitud;
	}

	public Integer getTempmax() {
		return tempmax;
	}

	public void setTempmax(Integer tempmax) {
		this.tempmax = tempmax;
	}

	public Integer getTempmin() {
		return tempmin;
	}

	public void setTempmin(Integer tempmin) {
		this.tempmin = tempmin;
	}

	public Integer getHumedad() {
		return humedad;
	}

	public void setHumedad(Integer humedad) {
		this.humedad = humedad;
	}

	public Integer getPresion() {
		return presion;
	}

	public void setPresion(Integer presion) {
		this.presion = presion;
	}

	public Integer getDatetime() {
		return datetime;
	}

	public void setDatetime(Integer datetime) {
		this.datetime = datetime;
	}
	
}
