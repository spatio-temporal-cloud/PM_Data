package rdf.pm.spatial.bigdata;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.XSD;

public class RDFCreate {
	public static String uri="http://www.bigspatialdata.org/pm";
	public static Model measurements(String id){
		Model m = ModelFactory.createDefaultModel();
		String [] values = getStationRecord(id);
		Map<String, Property> properties = getProperties(m);
		Resource measurements = m.createResource(uri+"#" + id);
		
		Resource AQI = m.createResource(values[3],XSD.xint);
		m.add(measurements,properties.get("hasAQI"),AQI);
		
		
		return m;
	}
	
	public static Map<String, Property> getProperties(Model m){
		Map<String, Property> properties = new HashMap<String, Property>();
		properties.put("hasAQI", m.createProperty(uri+"#hasAQI"));
		return properties;
	}
	public static String [] getStationRecord(String id){
		String sql = "select * from Station_Data where ID=" + id;
		String [] values = null;
		try {
			java.sql.Connection conn = DriverManager.getConnection(
			    		"jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8", "pm", "ccntgrid");
			java.sql.Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery(sql);
			if(result.next()){
				values = new String[13];
				values[0] = result.getString("cityNameCh");
				values[1] = result.getString("stationNameCh");
				values[2] = result.getString("stationCode");
				values[3] = result.getString("AQI");
				values[4] = result.getString("Quality");
				values[5] = result.getString("CO");
				values[6] = result.getString("SO2");
				values[7] = result.getString("NO2");
				values[8] = result.getString("O3");
				values[9] = result.getString("PM10");
				values[10] = result.getString("PM2p5");
				values[11] = result.getString("primary_pollutant");
				values[12] = result.getString("time_point");
			}
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Unable to read data from mysql.");
		}
		return values;
	}
}
