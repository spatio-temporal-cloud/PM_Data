package bigspatialdata.pm.corr;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


public class Weather_corr {
	public static double[][] select(String cityNameEn, String cityNameCh,
			String factor, String start_day, String end_day) throws Exception {
		String url = "jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8";
		// String driver="";
		java.sql.Connection conn = DriverManager.getConnection(url, "pm",
				"ccntgrid");
		String sql = "SELECT PM2p5,time_point FROM City_Data WHERE cityNameCh='"
				+ cityNameCh + "'AND time_point>='" + start_day+"' AND time_point<'"+end_day+"';";
		
		
		PreparedStatement prestmt = conn.prepareStatement(sql);
		ResultSet rs1 = prestmt.executeQuery();
		
	
		ArrayList<Double> factors = new ArrayList<Double>();
		ArrayList<Double> PM2p5 = new ArrayList<Double>();
		
		while(rs1.next()){
			double pm25=rs1.getDouble("PM2p5");
			String time_point= rs1.getString("time_point");
			
			double factor_value = change(cityNameEn,factor,time_point);
			PM2p5.add(pm25);
			factors.add(factor_value);
		}


		double[][] data = new double[factors.size()][2];
		for (int i = 0; i < factors.size(); i++) 		{
			data[i][0] = factors.get(i);
			data[i][1] = PM2p5.get(i);
		}
		rs1.close();
		prestmt.close();
		conn.close();
		return data;
	}
	
	
	public static double change(String cityNameEn, String factor, String time_point) throws Exception {
		String url = "jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8";
		// String driver="";
		java.sql.Connection conn = DriverManager.getConnection(url, "pm",
				"ccntgrid");
		String sql1 = "SELECT "+factor+" FROM Weather_Data WHERE cityNameEn='" + cityNameEn+ "'AND lastupdate<='"+time_point+"' order by lastupdate desc ;";
		String sql2 = "SELECT "+factor+" FROM Weather_Data WHERE cityNameEn='" + cityNameEn+ "'AND lastupdate>='"+time_point+"' order by lastupdate asc ;";
		PreparedStatement prestmt1 = conn.prepareStatement(sql1);
		ResultSet rs1 = prestmt1.executeQuery();
		PreparedStatement prestmt2 = conn.prepareStatement(sql2);
		ResultSet rs2 = prestmt2.executeQuery();
		double b = 0,c = 0;
		while (rs1.next()) {
			b = rs1.getDouble(factor);
			break;
		}
		while(rs2.next()){
			c = rs2.getDouble(factor);
			break;
		}
		double a = (b+c)/2;
		return a;
}

	public static void main(String[] args) throws Exception {

		
		ArrayList<String> cities = new ArrayList<String>();
		ArrayList<String> cities2 = new ArrayList<String>();
		String url = "jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8";

		java.sql.Connection conn = DriverManager.getConnection(url, "pm",
				"ccntgrid");
		String sql = "SELECT cityNameEn,cityNameCh FROM City";
		PreparedStatement prestmt = conn.prepareStatement(sql);
		ResultSet rs = prestmt.executeQuery();
		while (rs.next()) {
//			cities.add(rs.getString("cityNameCh"));
//			cities2.add(rs.getString("cityNameEn"));
		}
//		cities.add("北京");
//		cities2.add("beijing");
//		cities.add("天津");
//		cities2.add("tianjin");
//		cities.add("西安");
//		cities2.add("xian");
//		cities.add("沈阳");
//		cities2.add("shenyang");
//		cities.add("武汉");
//		cities2.add("wuhan");
//		cities.add("重庆");
//		cities2.add("chongqing");
//		cities.add("南京");
//		cities2.add("nanjing");
//		cities.add("上海");
//		cities2.add("shanghai");
//		cities.add("杭州");
//		cities2.add("hangzhou");
//		cities.add("厦门");
//		cities2.add("xiamen");
//		cities.add("广州");
//		cities2.add("guangzhou");
		cities.add("昆明");
		cities2.add("kunming");
//		cities.add("海口");
//		cities2.add("haikou");
//		cities.add("乌鲁木齐");
//		cities2.add("wulumuqi");
//		cities.add("拉萨");
//		cities2.add("lasa");
		
		
		ArrayList<String> factors = new ArrayList<String>();
		factors.add("temperature");
		factors.add("humidity");
		factors.add("pressure");
		factors.add("wind_speed_value");

		ArrayList<String> days = new ArrayList<String>();
//		days.add("2013-05-29");
//		days.add("2013-05-31");
//		days.add("2013-06-02");
//		days.add("2013-06-04");
//		days.add("2013-06-06");
//		days.add("2013-06-08");
//		days.add("2013-06-10");
//		days.add("2013-06-12");
//		days.add("2013-06-14");
//		days.add("2013-06-16");
//		days.add("2013-06-18");
//		days.add("2013-06-20");
//		days.add("2013-06-22");
//		days.add("2013-06-24");
//		days.add("2013-06-26");
//		days.add("2013-06-28");
//		days.add("2013-06-30");

		days.add("2013-07-19");
		days.add("2013-07-20");
		
		File file = new File("/home/jychen/corr_weather.txt"); // Žæ·ÅÊý×éÊýŸÝµÄÎÄŒþ
		FileWriter out = new FileWriter(file); // ÎÄŒþÐŽÈëÁ÷
		out.write("Begin_time" + "  " + "End_time" + "  " + "cityNamech" + " "
				+ "factor" + " " + "corr");
		out.write("\r\n");

		for (int t = 0; t < days.size() - 1; t++) {
			String start_day = days.get(t);
			String end_day = days.get(t + 1);
			for (int j = 0; j < cities.size(); j++) {
				System.out.println(cities.get(j));
				for (int l = 0; l < factors.size(); l++) {
					double[][] data = select(cities2.get(j), cities.get(j),
							factors.get(l), start_day, end_day);

					double corr = cor_coefficient(data);
					out.write(start_day + " " + end_day + " ");

					out.write(cities.get(j) + " " + factors.get(l) + ": "
							+ corr);
					out.write("\r\n");
//					System.out.println(cities.get(j) + " " + factors.get(l)
//							+ ": " + corr);
					System.out.print(corr + " ");
				}
				System.out.println("");
			}

		}
		out.close();

	}

	// ÏÔÊŸ¶ÁÈ¡³öµÄÊý×é
	public static double cor_coefficient(double[][] arr2) {
		int n = arr2.length;
		double sum1 = 0;
		double sum2 = 0;
		int length = 0;
		double[][] cor1 = new double[n][2];
		double[][] cor2 = new double[n][2];
		for (int i = 0; i < n; i++) {
			sum1 = sum1 + arr2[i][0];
			sum2 = sum2 + arr2[i][1];
			length++;
		}
		double avg1 = sum1 / length;
		double avg2 = sum2 / length;
		for (int i = 0; i < n; i++) {
			cor1[i][0] = arr2[i][0] - avg1;
			cor1[i][1] = arr2[i][1] - avg2;
			cor2[i][0] = cor1[i][0] * cor1[i][0];
			cor2[i][1] = cor1[i][1] * cor1[i][1];

		}
		double sum3 = 0;
		double[] sum4 = { 0.0, 0.0 };
		for (int i = 0; i < n; i++) {
			sum3 = sum3 + cor1[i][1] * cor1[i][0];
			sum4[0] = cor2[i][0] + sum4[0];
			sum4[1] = cor2[i][1] + sum4[1];
		}
		double p = sum4[0] * sum4[1];
		double cor = sum3 / Math.sqrt(p);
		return cor;
	}

}
