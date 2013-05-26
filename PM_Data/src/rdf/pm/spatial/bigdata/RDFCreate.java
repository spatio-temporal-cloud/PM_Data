package rdf.pm.spatial.bigdata;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.XSD;

public class RDFCreate {
	public static String uri="http://www.bigspatialdata.org/pm";
	public static Model measurements(String id){
		Model m = ModelFactory.createDefaultModel();
		String [] values = getStationRecord(id);
		Resource measurements = m.createResource(uri+"#measurements_" + id);
		
		m.add(measurements,m.createProperty(uri+"#hasAQI"),m.createResource(values[3],XSD.integer));
		m.add(measurements,m.createProperty(uri+"#hasQualityCh"),m.createResource(values[4],XSD.xstring));
		m.add(measurements,m.createProperty(uri+"#hasQualityEn"),m.createResource(getQualityEn(values[4]),XSD.xstring));
		m.add(measurements,m.createProperty(uri+"#hasCO"),m.createResource(values[5],XSD.xdouble));
		m.add(measurements,m.createProperty(uri+"#hasSO2"),m.createResource(values[6],XSD.integer));
		m.add(measurements,m.createProperty(uri+"#hasNO2"),m.createResource(values[7],XSD.integer));
		m.add(measurements,m.createProperty(uri+"#hasO3"),m.createResource(values[8],XSD.integer));
		m.add(measurements,m.createProperty(uri+"#hasPM10"),m.createResource(values[9],XSD.integer));
		m.add(measurements,m.createProperty(uri+"#hasPM2p5"),m.createResource(values[10],XSD.integer));
		m.add(measurements,m.createProperty(uri+"#hasPrimaryPollutant"),m.createResource(values[11],XSD.xstring));
		
		Resource station = m.createResource(uri+"#station_" + values[2]);
		m.add(measurements,m.createProperty(uri+"#hasStation"),station);
		return m;
	}
	
	
	public static String getQualityEn(String quality){
		if(quality=="优"){
			return "excellent";
		}else if(quality=="良"){
			return "good";
		}else if(quality=="轻度污染"){
			return "light pollution";
		}else if(quality=="中度污染"){
			return "moderate pollution";
		}else if(quality=="重度污染"){
			return "serious pollution";
		}else if(quality=="严重污染"){
			return "severe pollution";
		}else{
			return "";
		}
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
