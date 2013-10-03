package bigspatialdata.air.pm25in;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.json.JSONArray;



public class City {
	public static void main(String[] args) throws Exception {  
		if(args.length!=1){
			 System.out.println("usage: one configure file is needed");
			 System.exit(0);
		 }
		 Properties conf = ConfProperties.getProperties(args[0]);
	    URL cityList = new URL(conf.getProperty("city.URL"));  
	    BufferedReader in = new BufferedReader(new InputStreamReader(cityList.openStream()));  
	    String result = in.readLine();
	    String [] tmp = result.split(":");
	    JSONArray res_arr = new JSONArray(tmp[1]);
	    java.sql.Connection conn = DriverManager.getConnection(
	    		conf.getProperty("db.name"), conf.getProperty("db.user"), conf.getProperty("db.password"));
	    java.sql.Statement stmt = conn.createStatement();
	    for(int i=0;i<res_arr.length();i++){
	    	String sql = "insert into City(cityNameCh) values('"+res_arr.get(i)+"')";
	    	System.out.println(sql);
	    	
	    	if(!checkCity(res_arr.get(i).toString(),conf)){
	    		System.out.println(res_arr.get(i).toString());
	    		stmt.executeUpdate(sql);
	    	}
	    }
	    stmt.close();
	    conn.close();
	}
	
	public static boolean checkCity(String city, Properties conf){
		Boolean tag=true;
		try {
			java.sql.Connection conn = DriverManager.getConnection(
					conf.getProperty("db.name"), conf.getProperty("db.user"), conf.getProperty("db.password"));
			java.sql.Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery("select * from City where cityNameCh = '" + city + "';");
			if(result.next()){
				tag = true;
			}else{
				tag = false;
			}
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Unable to read data from mysql.");
		}
		return tag;
	}
}
