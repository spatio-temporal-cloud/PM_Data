package bigspatialdata.air.pm25in;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class ConfProperties {
	public static Properties getProperties(String filePath){
		Properties props = new Properties();
		InputStream in;
		try {
			in = new BufferedInputStream (new FileInputStream(filePath));
			props.load(in); 
			return props;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}  
		
		 
	}
}
