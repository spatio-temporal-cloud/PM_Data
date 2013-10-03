package bigspatialdata.air.pm25in;

import java.util.ArrayList;
import java.util.Date;
import java.util.TimerTask;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class PM25InSchedule extends TimerTask {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		System.out.println("Update data at " + new Date());
		System.out.println("Call api for station data...");
		PM25InData data = new PM25InData(PM25InRun.conf);
		String result = data.getDataOfAllCities();
		try {
			JSONArray arr = new JSONArray(result);
			ArrayList<String> sqls = new ArrayList<String>();
			JSONObject obj = arr.getJSONObject(0);
			String time = obj.getString("time_point").split("T")[0]
					+ " "
					+ obj.getString("time_point").split("T")[1].split("Z")[0];
			for (int i = 0; i < arr.length(); i++) {
				obj = arr.getJSONObject(i);
				String time_point = obj.getString("time_point").split("T")[0]
						+ " "
						+ obj.getString("time_point").split("T")[1].split("Z")[0];
				if (!data.checkStationDataExist(time_point,
						obj.getString("station_code")) && !obj.getString("position_name").equals("441200403")) {
					String sql = "insert into Station_Data(StationNameCh,StationCode,cityNameCh,AQI,Quality,CO,SO2,NO2,O3,PM10,PM2p5,primary_pollutant,time_point) "
							+ "values('"
							+ obj.getString("position_name")
							+ "','"
							+ obj.getString("station_code")
							+ "','"
							+ obj.getString("area")
							+ "',"
							+ obj.getInt("aqi")
							+ ",'"
							+ obj.getString("quality")
							+ "',"
							+ obj.getDouble("co")
							+ ","
							+ obj.getInt("so2")
							+ ","
							+ obj.getInt("no2")
							+ ","
							+ obj.getInt("o3")
							+ ","
							+ obj.getInt("pm10")
							+ ","
							+ obj.getInt("pm2_5")
							+ ",'"
							+ obj.getString("primary_pollutant")
							+ "','"
							+ time_point + "')";
					sqls.add(sql);
				}
			}
			data.addDataToDB(sqls);
			System.out.println("Time point: " + time);
			System.out.println("Calculate city data...");
			data.calCityData(time);
			System.out.println("Finished!");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			System.out.println("Result returned from API has format error!");
			e.printStackTrace();
		}

	}

}
