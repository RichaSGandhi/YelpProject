import java.util.List;


public class CityData {
	private String business_id;
	private String city;
	private Double latitude;
	private Double longitude;
	private String business_name;
	private List<String> neighborhood;
	
	public String getBusiness_id() {
		return business_id;
	}
	public void setBusiness_id(String business_id) {
		this.business_id = business_id;
	}
	public String getBusiness_name() {
		return business_name;
	}
	public void setBusiness_name(String business_name) {
		this.business_name = business_name;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public Double getLatitude() {
		return latitude;
	}
	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}
	public Double getLongitude() {
		return longitude;
	}
	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}
	public List<String> getNeighborhood() {
		return neighborhood;
	}
	public void setNeighborhood(List<String> neighborhood) {
		this.neighborhood = neighborhood;
	}
}
