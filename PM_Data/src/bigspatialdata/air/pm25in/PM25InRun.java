package bigspatialdata.air.pm25in;
import java.util.Properties;
import java.util.Timer;


public class PM25InRun {
	public static Properties conf;
	public static void main(String args[]){
		if(args.length!=1){
			System.out.println("usage: java -jar PM_Data.jar configure_file");
			System.exit(0);
		}
		conf = ConfProperties.getProperties(args[0]);
		long intervel = Integer.parseInt(conf.getProperty("interval.min")) * 60 * 1000;
		if(intervel<1200000){
			System.out.println("intervel is less than 20 minutes");
			System.exit(0);
		}
		Timer time = new Timer();
		time.schedule(new PM25InSchedule(), 0,intervel);
	}
}
