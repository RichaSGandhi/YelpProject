import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import org.apache.commons.io.IOUtils;
public class YelpJsonParser {
 private static String tableName = "BusinessData";
 public static void main(String[] args) {
  try {
   ClassLoader cl = YelpJsonParser.class.getClassLoader();
   InputStream is = cl.getResourceAsStream("FinalBusiness.json");
   String str = IOUtils.toString(is);
   JSONObject jsonObject = (JSONObject) JSONSerializer.toJSON(str);
   JSONArray jsonArr = jsonObject.getJSONArray("profiles");
   JSONObject obj = null;
   JSONArray nameArr = null;
   JSONArray valArr = null;
   
   for (int i = 0; i < jsonArr.size(); i++) {
    obj = jsonArr.getJSONObject(i);
    nameArr = obj.names();
    valArr = obj.toJSONArray(nameArr);
    saveRecord(nameArr, valArr);
   }
  } catch (Exception e) {
   e.printStackTrace();
  }
 }
 private static void saveRecord(JSONArray nameArray, JSONArray valArray) {
  Connection conn = getConnection();
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
  try {
   PreparedStatement pstmt = conn.prepareStatement(sb.toString());
   bindVariables(valArray, pstmt);
   pstmt.executeUpdate();
  } catch (SQLException e) {
   e.printStackTrace();
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
   }
  }
 }
 private static Connection getConnection() {
  Connection con = null;
  String url = "jdbc:mysql://localhost:3306/";
  String db =  "Yelp";
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