package bigspatialdata.weather.openweathermap;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Timer;


public class WeatherRun {
	public static Properties conf;
	public static void main(String args[]){
		if(args.length!=1){
			System.out.println("usage: java -jar Weather_Data.jar configure_file");
			System.exit(0);
		}
		
		conf = new Properties();
		InputStream in;
		try {
			in = new BufferedInputStream (new FileInputStream(args[0]));
			conf.load(in); 
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		
		
		long intervel = Integer.parseInt(conf.getProperty("interval.min")) * 60 * 1000;
		if(intervel<600000){
			System.out.println("intervel is less than 20 minutes");
			System.exit(0);
		}
		Timer time = new Timer();
		time.schedule(new WeatherSchedule(), 0,intervel);
	}
}
