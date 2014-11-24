import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.commons.io.IOUtils;

import com.mysql.jdbc.Statement;

public class MyJson {
	private static String tableName = "yelpbusiness";
	 public static void main(String[] args) {
		 //main12();
		prevMain();
	 }
	 private static void saveRecord(JSONArray nameArray, JSONArray valArray, Connection conn) throws SQLException {
	
	  StringBuffer sb = new StringBuffer("insert into " + tableName + "(");
	  int size = nameArray.size();
	  int count = 0;
	  Iterator<Object> iterator = nameArray.iterator();
	  
	  while (iterator.hasNext()) {
	   if (count < (size - 1))
	    sb.append(iterator.next() + ",");
	   else
	    sb.append(iterator.next() + ")");
	   count++;
	  }
	  sb.append(" values(");
	 
	  for (int i = 0; i < size; i++) {  
	   if (i < (size - 1))
	    sb.append("?,");
	   else
	    sb.append("?)");  
	  }
	  System.out.println(sb.toString());
	  PreparedStatement pstmt = null;
	  try {
	   pstmt = conn.prepareStatement(sb.toString());
	   bindVariables(valArray, pstmt);
	   pstmt.executeUpdate();
	  } catch (SQLException e) {
	   e.printStackTrace();
	  }finally {
		  pstmt.close();
	  }
	 }
	 private static void bindVariables(JSONArray valArray,
	   PreparedStatement pstmt) throws SQLException {
	  Iterator<Object> iterator = valArray.iterator();
	  int cnt = 0;
	  while (iterator.hasNext()) {
	   Object obj = iterator.next();
	   if (obj instanceof String) {
	    pstmt.setString(++cnt, (String) obj);
	   } else if (obj instanceof Integer) {
	    pstmt.setLong(++cnt, (Integer) obj);
	   } else if (obj instanceof Long) {
	    pstmt.setLong(++cnt, (Long) obj);
	   } else if (obj instanceof Double) {
	    pstmt.setDouble(++cnt, (Double) obj);
	   } /*else if(obj instanceof Boolean){
		   pstmt.setString(++cnt, (String) obj);
	   }*/
	  }
	  System.out.println(pstmt.toString());
	 }
	 
	private static Connection getConnection(){
	  Connection con = null;
	  String url = "jdbc:mysql://localhost:3306/";
	  String db =  "yelp";
	  String driver = "com.mysql.jdbc.Driver";
	  String user = "root";
	  String pass = "sept";
	  try {
	   Class.forName(driver);
	   con = DriverManager.getConnection(url + db, user, pass);
	  } catch (ClassNotFoundException e) {
	   e.printStackTrace();
	  } catch (SQLException e) {
		  System.out.println("Connections pool full");
	   e.printStackTrace();
	  }
	  return con;
	 }
	
	
	 public static void main12() {
		  Connection conn = getConnection();
		  List<CityData> businesses = new ArrayList<CityData>();
		  StringBuffer sb = new StringBuffer("Select business_id, city, latitude, longitude,name from yelpbusiness where city=?");
		  try {
			PreparedStatement pstmt = conn.prepareStatement(sb.toString());
			pstmt.setString(1, "Madison");
			ResultSet rs = pstmt.executeQuery();
			int size = 0;
			rs.last();
		    size = rs.getRow();
		    rs.beforeFirst();
			//System.out.println(size);
			while(rs.next()){
				CityData cd= new CityData();
				cd.setBusiness_id(rs.getString(1));
				//System.out.println(cd.getBusiness_id());
				cd.setCity(rs.getString(2));
				cd.setLatitude(rs.getDouble(3));
				cd.setLongitude(rs.getDouble(4));
				cd.setBusiness_name(rs.getString(5));
				List<String> nHL = getAllNeighborhoods(cd.getLongitude(),cd.getLatitude(),conn);
				//System.out.println(cd.getLatitude());
				//Double newLat = new BigDecimal(cd.getLatitude()).setScale(2,3).doubleValue();
				//System.out.println(newLat);
				if (nHL!=null){
				cd.setNeighborhood(nHL);
				
				}
				businesses.add(cd);
			//System.out.println(cd.getBusiness_id() + "," + cd.getCity() + "," + cd.getLatitude() + "," + cd.getLongitude());
			}
			//getAllNeighborhoods(conn);
			findTrendingCategory("Phoenix",85022,conn);
			//System.out.println(businesses.size());
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  
	 }
	 public static List<String> getAllNeighborhoods(Double Longitude, Double Latitude,Connection conn) throws SQLException
		{
			List<String> neighborhoodList = new ArrayList<String>();
			String sb = "select s.name from shapetable s"
					+" where st_contains(geomfromtext(s.the_geom),geomfromtext(?));";
			Double newLat = new BigDecimal(Latitude ).setScale(2, 3).doubleValue();
			Double newLong = new BigDecimal(Longitude ).setScale(2, 3).doubleValue();
			//System.out.println( newLat + " " + newLong);
			PreparedStatement pstmt = null;
			try {
			   pstmt = conn.prepareStatement(sb);
			   pstmt.setString(1, "POINT("+newLong+" "+newLat+")");
			  // System.out.println(pstmt);
			   ResultSet rs = pstmt.executeQuery();
			   while(rs.next()){
			  // System.out.println(rs.getString(1));
			   neighborhoodList.add(rs.getString(1));
			   }
			  } catch (SQLException e) {
			   e.printStackTrace();
			  }finally {
				  pstmt.close();
			  }
			return neighborhoodList;
		}
	 public static void prevMain(){
		 try {
			  
			   Connection conn = getConnection();
			   ClassLoader cl = MyJson.class.getClassLoader();
			   InputStream is = cl.getResourceAsStream("5000data.json");
			   String str = IOUtils.toString(is);
			   System.out.println("STARTING NOW");
			   JSONObject jsonObject = (JSONObject) JSONSerializer.toJSON(str);
			   System.out.println("STARTING to read JSON");
			   JSONArray jsonArr = jsonObject.getJSONArray("profiles");
			   JSONObject obj = null;
			   JSONArray nameArr = null;
			   JSONArray valArr = null;
			   System.out.println("STARTING For loop NOW");
			   for (int i = 0; i < jsonArr.size(); i++) {
			    obj = jsonArr.getJSONObject(i);
			    nameArr = obj.names();
			    nameArr.remove(13);
			    nameArr.remove(8);
			    nameArr.remove(2);
			    nameArr.add("zip");
			   // System.out.println(nameArr);
			    valArr = obj.toJSONArray(nameArr);
			    valArr.set(2, valArr.get(2).toString());
			   // ArrayList categories = new ArrayList();
			    String cat = valArr.get(3).toString();
			    String replaced = cat.substring(1,cat.length()-1);
			    valArr.set(3, replaced);
			    String Address = valArr.get(1).toString();
			    String zip = Address.substring(Address.length()-6, Address.length());
			   
			   //System.out.println(nameArr);
			   valArr.set(12, zip);
			   // System.out.println(valArr);
			    //String[] listCategories = replaced.split(",");
			    saveRecord(nameArr, valArr, conn);
			   }
			  } catch (Exception e) {
			   e.printStackTrace();
			  }
	 }
	 public static void findTrendingCategory(String city, Integer zip, Connection conn){
		 String sb = "select b.categories from yelpbusiness b"
					+" where b.city = ? and b.zip = ?;";
		 HashMap<String,Integer> categoryMap = new HashMap <String,Integer>();
		 Integer intialCount = 1;
		 PreparedStatement pstmt = null;
		 
		 try{
		 pstmt = conn.prepareStatement(sb);
		 pstmt.setString(1, city);
		 pstmt.setInt(2, zip);
		 ResultSet categories = pstmt.executeQuery();
		 while(categories.next()){
			 String[] category = categories.getString(1).split(",");
			 
			 for (int i = 0; i<category.length-1;i++){
				category[i] = category[i].substring(1, category[i].length()-1);
				//System.out.println(category[i]);
				 if (!category[i].equals("Shopping")){
					//System.out.println("Gym present");
				 if (categoryMap.containsKey(category[i])){
					 Integer count = categoryMap.get(category[i]);
					 categoryMap.put(category[i], count+1);
				 }else{
					 categoryMap.put(category[i], intialCount);
				 }
				 }
			 }
			 
		 }

		 int maxValueInMap=(Collections.max( categoryMap.values()));  // This will return max value in the Hashmap
	        for (Entry<String, Integer> entry : categoryMap.entrySet()) {  // Itrate through hashmap
	        	//System.out.println(entry.getKey() + entry.getValue());
	        	List<String> maxCategory  =  new ArrayList<String>();
	        	if (entry.getValue()==maxValueInMap) {
	            	
	                System.out.println(entry.getKey() + entry.getValue());     // Print the key with max value
	                maxCategory.add(entry.getKey());
	            }
	        }
		 }catch(Exception e){
			 e.printStackTrace();
		 }
		 
		 
	 }
	 
}
