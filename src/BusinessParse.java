import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.apache.commons.io.IOUtils;

public class BusinessParse {
	private static String tableName = "yelpbusiness";
	 public static void main(String[] args) {
	  try {
	   ClassLoader cl = BusinessParse.class.getClassLoader();
	   InputStream is = cl.getResourceAsStream("FinalBusiness.json");
	   String str = IOUtils.toString(is);
	   JSONObject jsonObject = (JSONObject) JSONSerializer.toJSON(str);
	   JSONArray jsonArr = jsonObject.getJSONArray("profiles");
	   JSONObject obj = null;
	   JSONArray nameArr = null;
	   JSONArray valArr = null;
	   Connection conn = getConnection();
	   for (int i = 0; i < jsonArr.size(); i++) {
	    obj = jsonArr.getJSONObject(i);
	    nameArr = obj.names();
	    nameArr.remove(13);
	    nameArr.remove(8);
	    nameArr.remove(2);
	    //System.out.println(nameArr);
	    valArr = obj.toJSONArray(nameArr);
	    valArr.set(2, valArr.get(2).toString());
	   // ArrayList categories = new ArrayList();
	    String cat = valArr.get(3).toString();
	    String replaced = cat.substring(1,cat.length()-1);
	    valArr.set(3, replaced);
	    //System.out.println(replaced);
	    //System.out.println(valArr);
	    //String[] listCategories = replaced.split(",");
	   // boolean f = 
	    saveRecord(nameArr, valArr,conn);
	   // if (f != true){
	    //	Thread.sleep(1000);
	    //}
	   }
	  } catch (Exception e) {
		  System.out.println("I am here");
	   e.printStackTrace();
	  }
	 }
	 private static void saveRecord(JSONArray nameArray, JSONArray valArray,Connection conn) {
		 Boolean flag = false;
	 
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
	 // System.out.println(sb.toString());
	  
	  try {
	   PreparedStatement pstmt = conn.prepareStatement(sb.toString());
	   bindVariables(valArray, pstmt);
	  //flag = pstmt.execute();
	   pstmt.executeUpdate();
	  } catch (SQLException e) {
		System.out.println("I am failing here");  
	   e.printStackTrace();
	   
	  }
	  //return flag;
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
	  //System.out.println(pstmt.toString());
	 }
	 private static Connection getConnection() {
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
	   e.printStackTrace();
	  }
	  return con;
	 }
	
	

}
