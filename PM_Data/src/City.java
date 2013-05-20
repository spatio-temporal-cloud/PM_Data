import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.DriverManager;

import org.json.JSONArray;



public class City {
	public static void main(String[] args) throws Exception {  
	    URL cityList = new URL("http://pm25.in/api/querys.json?token=5j1znBVAsnSf5xQyNQyq");  
	    BufferedReader in = new BufferedReader(new InputStreamReader(cityList.openStream()));  
	    String result = in.readLine();
	    String [] tmp = result.split(":");
	    JSONArray res_arr = new JSONArray(tmp[1]);
	    java.sql.Connection conn = DriverManager.getConnection(
	    		"jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8", "pm", "ccntgrid");
	    java.sql.Statement stmt = conn.createStatement();
	    for(int i=0;i<res_arr.length();i++){
	    	String sql = "insert into City(cityNameCh) values('"+res_arr.get(i)+"')";
	    	System.out.println(sql);
	    	stmt.executeUpdate(sql);
	    }
	    stmt.close();
	    conn.close();
	}
}
