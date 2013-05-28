package bigspatialdata.weather.data;

import java.util.Timer;

import bigspatialdata.pm.data.Data2;

public class Weather_Schedule {
	public static void main(String args[]){
		if(args.length!=1){
			System.out.println("usage: java -jar Weather_Data.jar intervel");
			System.exit(0);
		}
		long intervel = Integer.parseInt(args[0]) * 60 * 1000;
		if(intervel<1800000){
			System.out.println("intervel is less than 30 minutes");
			System.exit(0);
		}
		Timer time = new Timer();
		time.schedule(new Weather_Data(), 0,intervel);
	}
}
