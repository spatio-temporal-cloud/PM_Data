import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class Test {
	public static void main(String [] args) {
		String time_point="2013-05-21 09:00:00";
//		ArrayList<String> cities = Data.getCities();
		ArrayList<String> cities = new ArrayList<String>();
		cities.add("湘潭");
		ArrayList<String> sqls = new ArrayList<String>();
		for(int i=0;i<cities.size();i++){
			String cityNameCh=cities.get(i);
			String sql = "select * from Station_Data where time_point='" + time_point+
					"' and cityNameCh='" + cityNameCh +"'";
			System.out.println(sql);
			int NO2=0,SO2=0,AQI=0,PM10=0,PM2p5=0,O3=0;
			int NO2_N=0,SO2_N=0,AQI_N=0,PM10_N=0,PM2p5_N=0,O3_N=0,CO_N=0;
			double CO=0;
			String quality="";
			try {
				java.sql.Connection conn = DriverManager.getConnection(
				    		"jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8", "pm", "ccntgrid");
				java.sql.Statement stmt = conn.createStatement();
				ResultSet result = stmt.executeQuery(sql);
				while(result.next()){
					if(result.getInt("NO2")!=0){
						NO2 += result.getInt("NO2");
						NO2_N++;
					}
					if(result.getInt("SO2")!=0){
						SO2 += result.getInt("SO2");
						SO2_N++;
					}
					if(result.getInt("AQI")!=0){
						AQI += result.getInt("AQI");
						AQI_N++;
					}
					if(result.getInt("PM10")!=0){
						PM10 += result.getInt("PM10");
						PM10_N++;
					}
					if(result.getInt("PM2p5")!=0){
						PM2p5 += result.getInt("PM2p5");
						PM2p5_N++;
					}
					if(result.getInt("O3")!=0){
						O3 += result.getInt("O3");
						O3_N++;
					}
					if(result.getDouble("CO")!=0){
						CO += result.getDouble("CO");
						CO_N++;
					}
					if(AQI/AQI_N <= 50){
						quality="优";
					}else if(AQI/AQI_N <=100){
						quality="良";
					}else if(AQI/AQI_N <=150){
						quality="轻度污染";
					}else if(AQI/AQI_N <=200){
						quality="中度污染";
					}else if(AQI/AQI_N <=300){
						quality="重度污染";
					}else{
						quality="严重污染";
					}
					
				}
				if(AQI_N!=0){
					double tmp = (double)AQI/AQI_N;
					AQI = (int)(tmp+0.5);
				}
				if(CO_N!=0){
					CO = CO/CO_N;
				}
				if(SO2_N!=0){
					double tmp = (double)SO2/SO2_N;
					SO2 = (int)(tmp+0.5);
				}
				if(NO2_N!=0){
					double tmp = (double)NO2/NO2_N;
					NO2 = (int)(tmp+0.5);
				}
				if(O3_N!=0){
					double tmp = (double)O3/O3_N;
					O3 = (int)(tmp+0.5);
				}
				if(PM10_N!=0){
					double tmp = (double)PM10/PM10_N;
					PM10 = (int)(tmp+0.5);
				}
				if(PM2p5_N!=0){
					double tmp = (double)PM2p5/PM2p5_N;
					PM2p5 = (int)(tmp+0.5);
				}
				String sqlInsert = "insert into City_Data(cityNameCh,AQI,Quality,CO,SO2,NO2,O3,PM10,PM2p5,time_point) " +
						"values('"+cityNameCh+"',"+AQI+",'"+quality+"',"+CO+","+
						SO2+"," + NO2 + ","+O3 +","+PM10+","+PM2p5+
						",'"+time_point+"')";
				sqls.add(sqlInsert);
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Unable to read city list. Failed to connect to mysql!");
			}
		}
		Data.addData(sqls);
	}
	
}
