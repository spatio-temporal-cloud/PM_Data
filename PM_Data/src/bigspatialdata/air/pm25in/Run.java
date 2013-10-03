package bigspatialdata.air.pm25in;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.TimerTask;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class Run extends TimerTask {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("Update data at " + new Date());
		System.out.println("Call api for station data...");
		PM25InData data = new PM25InData(Schedule.conf);
		String result = data.getDataOfAllCities();
		try {
			JSONArray arr = new JSONArray(result);
			ArrayList<String> sqls = new ArrayList<String>();
			JSONObject obj = arr.getJSONObject(0);
			String time_point = obj.getString("time_point").split("T")[0] + " "+obj.getString("time_point").split("T")[1].split("Z")[0];
			System.out.println("in run: " + time_point);
			if(!data.checkCityDataExist(time_point)){
				for(int i=0;i<arr.length();i++){
					obj = arr.getJSONObject(i);					
					time_point = obj.getString("time_point").split("T")[0] + " "+obj.getString("time_point").split("T")[1].split("Z")[0];
					String sql = "insert into Station_Data(StationNameCh,StationCode,cityNameCh,AQI,Quality,CO,SO2,NO2,O3,PM10,PM2p5,primary_pollutant,time_point) " +
						"values('"+obj.getString("position_name")+"','"+obj.getString("station_code")+"','"+obj.getString("area")+"',"+
						obj.getInt("aqi")+",'"+obj.getString("quality")+"',"+obj.getDouble("co")+","+
						obj.getInt("so2")+","+obj.getInt("no2") + ","+obj.getInt("o3") +","+obj.getInt("pm10")+","+obj.getInt("pm2_5")+
						",'"+obj.getString("primary_pollutant")+"','"+time_point+"')";
					sqls.add(sql);					
				}
				data.addDataToDB(sqls);
				System.out.println("Calculate city data...");
				data.calCityData(time_point);
			}else{
				System.out.println("Data alread exists...");
			}
			System.out.println("Finished!");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			System.out.println("Result returned from API has format error!");
			e.printStackTrace();
		}
		
	}
	
	
	
	
	

}
