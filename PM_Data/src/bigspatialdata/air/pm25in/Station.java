package bigspatialdata.air.pm25in;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;



public class Station {
	
	public static void main(String args[]) throws IOException, JSONException, SQLException{
		 if(args.length!=1){
			 System.out.println("usage: one configure file is needed");
			 System.exit(0);
		 }
		 Properties conf = ConfProperties.getProperties(args[0]);
		
		 URL cityList = new URL(conf.getProperty("station.URL"));  
		 BufferedReader in = new BufferedReader(new InputStreamReader(cityList.openStream()));  
		 String result = "";
		 String line = "";
		 while((line=in.readLine())!=null){
			 result += line;
		 }
		 JSONArray arr = new JSONArray(result);

		 java.sql.Connection conn = DriverManager.getConnection(
		    		conf.getProperty("db.name"), conf.getProperty("db.user"), conf.getProperty("db.password"));
		 java.sql.Statement stmt = conn.createStatement();
		 for(int i=0;i<arr.length();i++){
				JSONObject city = arr.getJSONObject(i);
				String cityNameCh = city.getString("city");
				String stations = city.getString("stations");
				JSONArray stas = new JSONArray(stations);
				for(int j=0;j<stas.length();j++){
					JSONObject station = stas.getJSONObject(j);
					String stationNameCh = station.getString("station_name");
					String stationCode = station.getString("station_code");
					
					if(!checkStation(stationCode, conf)){
						System.out.println(stationCode + " " +  stationNameCh + " " + cityNameCh);
						String sql = "insert into Station(stationCode, stationNameCh, cityNameCh) values('"+ 
								stationCode +"','"+stationNameCh+"','"+cityNameCh+"')";
						stmt.executeUpdate(sql);
					}
				}
		}
		 stmt.close();
		 conn.close();
	}
	public static boolean checkStation(String stationCode, Properties conf){
		Boolean tag=true;
		try {
			java.sql.Connection conn = DriverManager.getConnection(
					conf.getProperty("db.name"), conf.getProperty("db.user"), conf.getProperty("db.password"));
			java.sql.Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery("select * from Station where stationCode = '" + stationCode + "';");
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
