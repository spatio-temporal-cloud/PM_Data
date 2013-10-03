package bigspatialdata.weather.openweathermap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class WeatherData {
	private Properties conf;

	public Properties getConf() {
		return conf;
	}

	public void setConf(Properties conf) {
		this.conf = conf;
	}

	public WeatherData(Properties conf){
		this.conf = conf;
	}
	
	public String getDataForCity(String city){
		String url = conf.getProperty("URL")+"?q="+city+"&mode=json";
		String result = "";
		String line = "";
		URL cityList;
		try {
			cityList = new URL(url);
			BufferedReader in = new BufferedReader(new InputStreamReader(
					cityList.openStream()));
			while ((line = in.readLine()) != null) {
				result += line;
			}
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	
	public boolean addDataToDB(String sql) {
		try {
			java.sql.Connection conn = DriverManager.getConnection(
					conf.getProperty("db.name"), conf.getProperty("db.user"),
					conf.getProperty("db.password"));
			java.sql.Statement stmt = conn.createStatement();
				stmt.executeUpdate(sql);
			stmt.close();
			conn.close();
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Unable to add data to mysql.");
			return false;
		}
	}
	
	public boolean checkWeatherExist(String cityNameEn,String time){
		boolean tag=true;
		try {
			java.sql.Connection conn = DriverManager.getConnection(
					conf.getProperty("db.name"), conf.getProperty("db.user"),
					conf.getProperty("db.password"));
			java.sql.Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery("select * from Weather_Data where lastupdate='"+time+"' and cityNameEn='"+cityNameEn+"';");
			if(result.next()){
				tag=true;
			}else{
				tag=false;
			}
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Unable to read city list. Failed to connect to mysql!");
		}
		return tag;
	}
	public ArrayList<String> getCitiesEn(){
		ArrayList<String> cities = new ArrayList<String>();
		try {
			java.sql.Connection conn = DriverManager.getConnection(
					conf.getProperty("db.name"), conf.getProperty("db.user"),
					conf.getProperty("db.password"));
			java.sql.Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery("select cityNameEn from City;");
			while(result.next()){
				String city = result.getString("cityNameEn");
				cities.add(city);
			}
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Unable to read city list. Failed to connect to mysql!");
		}
		 
		return cities;
	}
	
	public Map<String,String> getCities(){
		Map<String, String> cities = new HashMap<String, String>();
		try {
			java.sql.Connection conn = DriverManager.getConnection(
					conf.getProperty("db.name"), conf.getProperty("db.user"),
					conf.getProperty("db.password"));
			java.sql.Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery("select cityNameCh,cityNameEn from City;");
			while(result.next()){
				String city = result.getString("cityNameCh");
				String cityEn = result.getString("cityNameEn");
				cities.put(city, cityEn);
			}
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Unable to read city list. Failed to connect to mysql!");
		}
		 
		return cities;
	}
}
