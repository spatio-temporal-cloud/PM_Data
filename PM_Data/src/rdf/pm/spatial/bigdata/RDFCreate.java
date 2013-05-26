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
	public static String time_uri = "http://www.w3.org/2006/time";
	public static String timezone_uri = "http://www.w3.org/2006/timezone";
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
		m.add(station,m.createProperty(uri+"#hasStationCode"),m.createResource(values[2],XSD.xstring));
		m.add(station,m.createProperty(uri+"#hasStationNameCh"),m.createResource(values[1],XSD.xstring));
		
		String [] cityInfo = getCityInfo(values[0]);
		Resource city = m.createResource(uri+"#city_" + cityInfo[1]);
		m.add(station,m.createProperty(uri+"#locatedCity"),city);
		
		m.add(city,m.createProperty(uri+"#hasCityNameCh"),m.createResource(values[0],XSD.xstring));
		m.add(city,m.createProperty(uri+"#hasCityNameEn"),m.createResource(cityInfo[1],XSD.xstring));
		
		Resource dateTimeInterval = m.createResource(uri + "#DateTimeIntervel_" + dataTimeFormat(values[12]));
		m.add(measurements,m.createProperty(uri+"#hasTime"),dateTimeInterval);
		m.add(dateTimeInterval,m.createProperty(time_uri + "#xsdDateTime"),m.createResource(values[12],XSD.dateTime));
		
		Resource dateTimeDescription = m.createResource(uri + "#DateTimeDescription_" + dataTimeFormat(values[12]));
		m.add(dateTimeInterval,m.createProperty(time_uri + "#hasDateTimeDescription"),dateTimeDescription);
		
		Resource unitHour = m.createResource(uri + "#unitHour");
		m.add(dateTimeDescription,m.createProperty(time_uri + "#unitType"),unitHour);
		m.add(dateTimeDescription,m.createProperty(time_uri + "#year"),m.createResource(values[12].split(" ")[0].split("-")[0],XSD.gYear));
		m.add(dateTimeDescription,m.createProperty(time_uri + "#month"),m.createResource(values[12].split(" ")[0].split("-")[1],XSD.gMonth));
		m.add(dateTimeDescription,m.createProperty(time_uri + "#day"),m.createResource(values[12].split(" ")[0].split("-")[2],XSD.gDay));
		m.add(dateTimeDescription,m.createProperty(time_uri + "#hour"),m.createResource(values[12].split(" ")[1].split("-")[0],XSD.nonNegativeInteger));
		Resource timeZone = m.createResource(timezone_uri+"#Beijing");
		
		m.add(dateTimeDescription,m.createProperty(time_uri + "#timeZone"),timeZone);
		m.add(timeZone,m.createProperty(timezone_uri+"#GMToffset"),m.createResource("8",XSD.duration));
		return m;
	}
	
	public static String dataTimeFormat(String time_point){
		String [] tmp = time_point.split(" ");
		String [] tmp2 = tmp[0].split("-");
		String [] tmp3 = tmp[1].split(":");
		return tmp2[0]+tmp2[1]+tmp2[2]+tmp3[0]+tmp3[1]+tmp3[2];
	}
	
	public static String [] getCityInfo(String cityNameCh){
		String [] cityInfo= new String[2];
		try {
			java.sql.Connection conn = DriverManager.getConnection(
			    		"jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8", "pm", "ccntgrid");
			java.sql.Statement stmt = conn.createStatement();
			String sql = "select cityCode,cityNameEn from City where cityNameCh = '" + cityNameCh + "'";
			ResultSet result = stmt.executeQuery(sql);
			if(result.next()){
				cityInfo[0] = result.getString("cityCode");
				cityInfo[1] = result.getString("cityNameEn");
			}
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Unable to read data from mysql.");
		}
		return cityInfo;
	}
	
	public static String getQualityEn(String quality){
		if(quality.equals("优")){
			return "excellent";
		}else if(quality.equals("良")){
			return "good";
		}else if(quality.equals("轻度污染")){
			return "light pollution";
		}else if(quality.equals("中度污染")){
			return "moderate pollution";
		}else if(quality.equals("重度污染")){
			return "serious pollution";
		}else if(quality.equals("严重污染")){
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
