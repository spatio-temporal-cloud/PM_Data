package bigspatialdata.air.pm25in;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

public class Data {

	public static void addData(ArrayList<String> sqls) {
		try {
			java.sql.Connection conn = DriverManager
					.getConnection(
							"jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8",
							"pm", "ccntgrid");
			java.sql.Statement stmt = conn.createStatement();
			for (int i = 0; i < sqls.size(); i++) {
				stmt.executeUpdate(sqls.get(i));
			}
			stmt.close();
			conn.close();
			System.out.println(sqls.size() + " new records added");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Unable to add data to mysql.");
		}
	}

	public static String callAPI(String url) {
		String result = "";
		String line = "";
		try {
			URL cityList = new URL(url);
			BufferedReader in = new BufferedReader(new InputStreamReader(
					cityList.openStream()));
			while ((line = in.readLine()) != null) {
				result += line;
			}
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Failed to call API:  " + url);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Failed to call API:  " + url);
		}

		return result;
	}

	public static ArrayList<String> getCities() {
		ArrayList<String> cities = new ArrayList<String>();
		try {
			java.sql.Connection conn = DriverManager
					.getConnection(
							"jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8",
							"pm", "ccntgrid");
			java.sql.Statement stmt = conn.createStatement();
			ResultSet result = stmt
					.executeQuery("select cityNameCh from City;");
			while (result.next()) {
				String city = result.getString("cityNameCh");
				cities.add(city);
			}
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out
					.println("Unable to read city list. Failed to connect to mysql!");
		}

		return cities;
	}

	public static boolean checkCityData(JSONObject obj) throws JSONException {
		boolean tag = false;
		String cityNameCh = obj.getString("area");
		String time_point = obj.getString("time_point").split("T")[0] + " "
				+ obj.getString("time_point").split("T")[1].split("Z")[0];
		String sql = "select cityNameCh from City_Data where cityNameCh='"
				+ cityNameCh + "' and time_point='" + time_point + "'";
		try {
			java.sql.Connection conn = DriverManager
					.getConnection(
							"jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8",
							"pm", "ccntgrid");
			java.sql.Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery(sql);
			if (result.next()) {
				tag = true;
			} else {
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
