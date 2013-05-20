import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class Station {
	public static void main(String arg[]) throws IOException, JSONException, SQLException{
		 URL cityList = new URL("http://pm25.in/api/querys/station_names.json?token=5j1znBVAsnSf5xQyNQyq");  
		 BufferedReader in = new BufferedReader(new InputStreamReader(cityList.openStream()));  
		 String result = "";
		 String line = "";
		 while((line=in.readLine())!=null){
			 result += line;
		 }
		 JSONArray arr = new JSONArray(result);

		 java.sql.Connection conn = DriverManager.getConnection(
		    		"jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8", "pm", "ccntgrid");
		 java.sql.Statement stmt = conn.createStatement();
		 for(int i=0;i<arr.length();i++){
				JSONObject city = arr.getJSONObject(i);
				String cityNameCh = city.getString("city");
				System.out.println(cityNameCh+"......");
				String stations = city.getString("stations");
				JSONArray stas = new JSONArray(stations);
				for(int j=0;j<stas.length();j++){
					JSONObject station = stas.getJSONObject(j);
					String stationNameCh = station.getString("station_name");
					String stationCode = station.getString("station_code");
					System.out.println(stationNameCh);
					String sql = "insert into Station(stationCode, stationNameCh, cityNameCh) values('"+ 
						stationCode +"','"+stationNameCh+"','"+cityNameCh+"')";
					stmt.executeUpdate(sql);
				}
				
		}
		 stmt.close();
		 conn.close();
	}
}
