package bigspatialdata.weather.openweathermap;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TimerTask;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;



public class WeatherSchedule extends TimerTask {
	@Override
	public void run() {
		// TODO Auto-generated method stub
		WeatherData data = new WeatherData(WeatherRun.conf);
		ArrayList<String> cities = data.getCitiesEn();
		ArrayList<String> sqls = new ArrayList<String>();
		System.out.println("Update data at " + new Date());
		try {
			for(int i=0;i<cities.size();i++){
				String result = data.getDataForCity(cities.get(i));
				JSONObject obj = new JSONObject(result);
				String update_time="0000-00-00 00:00:00";
				if(obj.has("dt")){
					update_time = dateTimeString(obj.getLong("dt"));
				}
				if(data.checkWeatherExist(cities.get(i),update_time)){
					String sun_rise="0000-00-00 00:00:00";
					String sun_set="0000-00-00 00:00:00";
					if(obj.has("sys")){
						JSONObject sys = obj.getJSONObject("sys");
						if(sys.has("sunrise")){
							sun_rise = dateTimeString(sys.getLong("sunrise"));
						}
						if(sys.has("sunset")){
							sun_set = dateTimeString(sys.getLong("sunset"));
						}
					}
					String weather_id = "";
					String weather_main = "";
					String weather_description = "";
					String weather_icon="";
					if(obj.has("weather")){
						JSONArray weather = obj.getJSONArray("weather");
						if(weather.length()>0){
							if(weather.getJSONObject(0).has("id")){
								weather_id = weather.getJSONObject(0).getString("id");
							}
							if(weather.getJSONObject(0).has("main")){
								weather_main = weather.getJSONObject(0).getString("main");
							}
							if(weather.getJSONObject(0).has("description")){
								weather_description = weather.getJSONObject(0).getString("description");
							}
							if(weather.getJSONObject(0).has("icon")){
								weather_icon = weather.getJSONObject(0).getString("icon");
							}
						}
					}
					
					double temp=-1;
					double temp_min=-1;
					double temp_max=-1;
					double humidity=-1;
					double pressure=-1;
					if(obj.has("main")){
						JSONObject main = obj.getJSONObject("main");
						if(main.has("temp")){
							temp = main.getDouble("temp");
						}
						if(main.has("temp_min")){
							temp_min = main.getDouble("temp_min");
						}
						if(main.has("temp_max")){
							temp_max = main.getDouble("temp_max");
						}
						if(main.has("humidity")){
							humidity = main.getDouble("humidity");
						}
						if(main.has("pressure")){
							pressure = main.getDouble("pressure");
						}
					}
					double rain_3h=-1;
					if(obj.has("rain") && obj.getJSONObject("rain").has("3h")){
						rain_3h = obj.getJSONObject("rain").getDouble("3h");
					}
					double clouds=-1;
					if(obj.has("clouds") && obj.getJSONObject("clouds").has("all")){
						clouds = obj.getJSONObject("clouds").getDouble("all");
					}
					double speed=-1;
					double deg=-1;
					if(obj.has("wind")){
						JSONObject wind = obj.getJSONObject("wind");
						if(wind.has("speed")){
							speed = wind.getDouble("speed");
						}
						if(wind.has("deg")){
							deg = wind.getDouble("deg");
						}
					}
					String sql = "insert into Weather_Data(cityNameEn,sun_rise,sun_set,temperature,temperature_min,temperature_max," +
							"humidity,pressure,wind_speed_value,wind_direction_value," +
							"rain_3h,clouds_value,weather_id,weather_main,weather_description,weather_icon,lastupdate) values('"+cities.get(i)+"'," +
							"'"+sun_rise+"','"+sun_set+"',"+temp+","+temp_min+","+temp_max+","+humidity+","+pressure+","+speed+","+deg+"," +
							rain_3h+","+clouds+",'"+weather_id+"','"+weather_main+"','"+weather_description+"','"+weather_icon+"','"+update_time+"');";
					sqls.add(sql);
				}
			}
			if(sqls.size()>0){
				data.addDataToDB(sqls);
			}
			System.out.println("Finished");
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static String dateTimeString(long t){
		SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(t*1000);
//      cal.add(Calendar.HOUR, 8);
        return sdf.format(cal.getTime());
	}
	
	
}
