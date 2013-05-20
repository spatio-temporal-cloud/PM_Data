import java.util.ArrayList;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.json.JSONArray;
import org.json.JSONException;

public class Test {
	public static void main(String [] args) throws JSONException {
		ArrayList<String> cities = Data.getCities();
//		ArrayList<String> cities = new ArrayList<String>();
//		cities.add("中山");
		for(int i=0;i<cities.size();i++){
			System.out.println(cities.get(i));
			String result = Data.callAPI("http://pm25.in/api/querys/aqi_details.json?city="+cities.get(i)+"&token=5j1znBVAsnSf5xQyNQyq");
			JSONArray arr = new JSONArray(result);
		}
		
	}
	
}
