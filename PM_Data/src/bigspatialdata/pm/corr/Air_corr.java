package bigspatialdata.pm.corr;

import java.io.File;
import java.io.FileWriter;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

public class Air_corr {
	public static double[][] select(String cityNameCh, String factor, String start_time,String end_time) throws Exception {
		String url = "jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8";
		// String driver="";
		java.sql.Connection conn = DriverManager.getConnection(url, "pm",
				"ccntgrid");
		java.sql.Statement stmt = conn.createStatement();
		String sql = "SELECT "+factor+",PM2p5 FROM City_Data WHERE cityNameCh='"
				+ cityNameCh
				+ "' AND time_point>='"+start_time+"' AND time_point<'"+end_time+"';";
		PreparedStatement prestmt = conn.prepareStatement(sql);
		ResultSet rs = prestmt.executeQuery();
		ArrayList<Double> factors = new ArrayList<Double>();
		ArrayList<Double> PM2p5 = new ArrayList<Double>();
		while (rs.next()) {
			factors.add(rs.getDouble(factor));
			PM2p5.add(rs.getDouble("PM2p5"));
		}
		double[][] data = new double[factors.size()][2];
		for (int i = 0; i < factors.size(); i++) {
			data[i][0] = factors.get(i);
			data[i][1] = PM2p5.get(i);
		}
		rs.close();
		prestmt.close();
		conn.close();
		return data;
	}

	public static void main(String[] args) throws Exception {

		// œ«Êý×éÖÐµÄÊýŸÝÐŽÈëµœÎÄŒþÖÐ¡£Ã¿ÐÐž÷ÊýŸÝÖ®ŒäTABŒäžô

		ArrayList<String> cities = new ArrayList<String>();

		String url = "jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8";
		// String driver="";
		java.sql.Connection conn = DriverManager.getConnection(url, "pm",
				"ccntgrid");
		java.sql.Statement stmt = conn.createStatement();
		String sql = "SELECT cityNameCh FROM City";
		PreparedStatement prestmt = conn.prepareStatement(sql);
		ResultSet rs = prestmt.executeQuery();

		while (rs.next()) {
			cities.add(rs.getString("cityNameCh"));
		}
		
		ArrayList<String> factors = new ArrayList<String>();
		factors.add("CO");
		factors.add("SO2");
		factors.add("NO2");
		factors.add("O3");
		factors.add("PM10");
		ArrayList<String> days = new ArrayList<String>();


		days.add("2013-07-19");
		days.add("2013-07-20");
		
		File file = new File("/home/jychen/corr_air.txt"); // Žæ·ÅÊý×éÊýŸÝµÄÎÄŒþ
		FileWriter out = new FileWriter(file); // ÎÄŒþÐŽÈëÁ÷
		out.write("Begin_days"+"  "+"End_days"+"  "+"cityNamech"+" "+"factor"+" "+"corr");
		out.write("\r\n");
		for (int t = 0; t < days.size() - 1; t++) {
			String begin_day = days.get(t);
			String end_day = days.get(t+1);
			
			for (int j = 0; j < cities.size(); j++) {
				System.out.println(cities.get(j));
				for (int l = 0; l < factors.size(); l++) {
					double[][] data = select(cities.get(j), factors.get(l),begin_day,end_day);

					double corr = cor_coefficient(data);
	//				out.write(begin_day+" ");
	//				out.write(end_day+" ");
//					out.write(cities.get(j) + " " + factors.get(l)
//					+ ": " + corr);
//					out.write("\r\n");
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