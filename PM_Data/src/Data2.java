import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.TimerTask;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class Data2 extends TimerTask {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		String url = "http://www.pm25.in/api/querys/all_cities.json?token=4pUYzxZdXBZBz7deR78x";
		String result = Data.callAPI(url);
		try {
			JSONArray arr = new JSONArray(result);
			ArrayList<String> sqls = new ArrayList<String>();
			String time_point="";
			for(int i=0;i<arr.length();i++){
				JSONObject obj = arr.getJSONObject(i);
				time_point = obj.getString("time_point").split("T")[0] + " "+obj.getString("time_point").split("T")[1].split("Z")[0];
				String sql = "insert into Station_Data(StationNameCh,StationCode,cityNameCh,AQI,Quality,CO,SO2,NO2,O3,PM10,PM2p5,primary_pollutant,time_point) " +
				"values('"+obj.getString("position_name")+"','"+obj.getString("station_code") +"','"+obj.getString("area")+"',"+
				obj.getInt("aqi")+",'"+obj.getString("quality")+"',"+obj.getDouble("co")+","+
				obj.getInt("so2")+","+obj.getInt("no2") + ","+obj.getInt("o3") +","+obj.getInt("pm10")+","+obj.getInt("pm2_5")+
				",'"+obj.getString("primary_pollutant")+"','"+time_point+"')";
				sqls.add(sql);
			}
			Data.addData(sqls);
			calCityData(time_point);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			System.out.println("Result returned from API has format error!");
			e.printStackTrace();
		}
		
	}
	
	public static void calCityData(String time_point){
		ArrayList<String> cities = Data.getCities();
		ArrayList<String> sqls = new ArrayList<String>();
		for(int i=0;i<cities.size();i++){
			String cityNameCh=cities.get(i);
			String sql = "select * from Station_Data where time_point='" + 
					"' and cityNameCh='" + cityNameCh +"'";
			int NO2=0,SO2=0,AQI=0,PM10=0,PM2p5=0,O3=0;
			int NO2_N=0,SO2_N=0,AQI_N=0,PM10_N=0,PM2p5_N=0,O3_N=0,CO_N=0;
			double CO=0;
			String quality="";
			try {
				java.sql.Connection conn = DriverManager.getConnection(
				    		"jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8", "pm", "ccntgrid");
				java.sql.Statement stmt = conn.createStatement();
				ResultSet result = stmt.executeQuery(sql);
				while(result.next()){
					if(result.getInt("NO2")!=0){
						NO2 += result.getInt("NO2");
						NO2_N++;
					}
					if(result.getInt("SO2")!=0){
						SO2 += result.getInt("SO2");
						SO2_N++;
					}
					if(result.getInt("AQI")!=0){
						AQI += result.getInt("AQI");
						AQI_N++;
					}
					if(result.getInt("PM10")!=0){
						PM10 += result.getInt("PM10");
						PM10_N++;
					}
					if(result.getInt("PM2p5")!=0){
						PM2p5 += result.getInt("PM2p5");
						PM2p5_N++;
					}
					if(result.getInt("O3")!=0){
						O3 += result.getInt("O3");
						O3_N++;
					}
					if(result.getDouble("CO")!=0){
						CO += result.getDouble("CO");
						CO_N++;
					}
					if(AQI/AQI_N <= 50){
						quality="优";
					}else if(AQI/AQI_N <=100){
						quality="良";
					}else if(AQI/AQI_N <=150){
						quality="轻度污染";
					}else if(AQI/AQI_N <=200){
						quality="中度污染";
					}else if(AQI/AQI_N <=300){
						quality="重度污染";
					}else{
						quality="严重污染";
					}
					
				}
				if(AQI_N!=0){
					AQI = AQI/AQI_N;
				}
				if(CO_N!=0){
					CO=CO/CO_N;
				}
				if(SO2_N!=0){
					SO2 = SO2/SO2_N;
				}
				if(NO2_N!=0){
					NO2 = NO2/NO2_N;
				}
				if(O3_N!=0){
					O3=O3/O3_N;
				}
				if(PM10_N!=0){
					PM10=PM10/PM10_N;
				}
				if(PM2p5_N!=0){
					PM2p5_N=PM2p5/PM10_N;
				}
				String sqlInsert = "insert into City_Data(cityNameCh,AQI,Quality,CO,SO2,NO2,O3,PM10,PM2p5,time_point) " +
						"values('"+cityNameCh+"',"+AQI+",'"+quality+"',"+CO+","+
						SO2+"," + NO2 + ","+O3 +","+PM10+","+PM2p5+
						",'"+time_point+"')";
				sqls.add(sqlInsert);
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Unable to read city list. Failed to connect to mysql!");
			}
		}
		Data.addData(sqls);
	}
	
	

}
