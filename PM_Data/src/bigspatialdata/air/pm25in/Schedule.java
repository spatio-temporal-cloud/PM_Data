package bigspatialdata.air.pm25in;
import java.util.Timer;


public class Schedule {
	public static void main(String args[]){
		if(args.length!=1){
			System.out.println("usage: java -jar PM_Data.jar intervel");
			System.exit(0);
		}
		long intervel = Integer.parseInt(args[0]) * 60 * 1000;
		if(intervel<1200000){
			System.out.println("intervel is less than 20 minutes");
			System.exit(0);
		}
		Timer time = new Timer();
		time.schedule(new GetAndStore(), 0,intervel);
	}
}
