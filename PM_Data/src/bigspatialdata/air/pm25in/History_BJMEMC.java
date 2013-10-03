package bigspatialdata.air.pm25in;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jsoup.Jsoup;

public class History_BJMEMC {

	public static String type0 = "城市环境评价点";
	public static String[] sites0 = { "东城东四", "东城天坛", "西城官园", "西城万寿西宫",
			"朝阳奥体中心", "朝阳农展馆", "海淀万柳", "海淀北部新区", "海淀北京植物园", "丰台花园", "丰台云岗",
			"石景山古城", "房山良乡", "大兴黄村镇", "亦庄开发区", "通州新城", "顺义新城", "昌平镇", "门头沟龙泉镇",
			"平谷镇", "怀柔镇", "密云镇", "延庆镇" };
	public static String type1 = "对照点及区域点";
	public static String[] sites1 = { "昌平定陵", "京西北八达岭", "京东北密云水库", "京东东高村",
			"京东南永乐店", "京南榆垡", "京西南琉璃河" };
	public static String type2 = "交通污染控制点";
	public static String[] sites2 = { "前门东大街", "永定门内大街", "西直门北大街", "南三环西路",
			"东四环北路" };
	public static String startDate = "2013-01-07";
	public static String endDate = "2013-01-07";

	public static void main(String args[]) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date start_date = sdf.parse(startDate);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(start_date);
			System.out.println("Begin to Crawl...");
			String date = startDate;
			while (true) {
				for (int i = 0; i < sites0.length; i++) {
					testStore(crawl("城市环境评价点", sites0[i],date),sites0[i],date);
				}
				for (int i = 0; i < sites1.length; i++) {
					testStore(crawl("对照点及区域点", sites1[i],date),sites1[i],date);
				}
				for (int i = 0; i < sites2.length; i++) {
					testStore(crawl("交通污染控制点", sites2[i],date),sites2[i],date);
				}
				if(date.equals(endDate)){
					break;
				}else{
					calendar.set(Calendar.DAY_OF_MONTH,calendar.get(Calendar.DAY_OF_MONTH)+1);
					date=sdf.format(calendar.getTime());
				}
			}
			System.out.println("================== crawl over");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void testStore(Map<String, String> result,String site,String date){
		if(result==null){
			System.out.println(site + " " + date + " failed" );
		}else{
			System.out.println(site + " " + date + " " + result.get("AQI"));
		}
	}
	public static void store(Map<String, String> result,String site,String date) {
		if(result==null){
			System.out.println("Fail: " + site + " " + date);
		}else{
			try {
				java.sql.Connection conn = DriverManager.getConnection(
				    		"jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8", "pm", "ccntgrid");
				java.sql.Statement stmt = conn.createStatement();
				String sql = "select * from BJ_History where Station='"+site+"' and time_point='"+date+"'";
				ResultSet res = stmt.executeQuery(sql);
				if(res.next()){
					System.out.println("Exit: " + site + " " + date);
					return;
				}else{
					System.out.println("Store: " + site + " " + date);
					sql = "insert into BJ_History(AQI,Quality,Level,Station,primary_pollutant,Description,time_point) " +
							"values("+result.get("AQI")+",'"+result.get("Quality")+"','"+result.get("Level")+"','" +
									result.get("Station")+"','"+result.get("Primary")+"','"+result.get("Descrption")+"','"+
							date+"');";
					stmt.executeUpdate(sql);
				}
				stmt.close();
				conn.close();
		}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Failed to connect to mysql!");
			}
		
		}
	}

	public static Map<String, String> crawl(String ddlType, String ddlName,
			String txtTime) {
		Map<String, String> result = null;
		try {
			String htmlContent = execute("");
			if(htmlContent==null){
				return null;
			}
			result = paraseHtml(htmlContent);
			String postData = "__EVENTTARGET="
					+ java.net.URLEncoder.encode(result.get("__EVENTTARGET"),
							"GBK");
			postData += "&__EVENTARGUMENT="
					+ java.net.URLEncoder.encode(result.get("__EVENTARGUMENT"),
							"GBK");
			postData += "&__LASTFOCUS="
					+ java.net.URLEncoder.encode(result.get("__LASTFOCUS"),
							"GBK");
			postData += "&__VIEWSTATE="
					+ java.net.URLEncoder.encode(result.get("__VIEWSTATE"),
							"GBK");
			postData += "&__EVENTVALIDATION="
					+ java.net.URLEncoder.encode(
							result.get("__EVENTVALIDATION"), "GBK");

			postData += "&ddlType=" + URLEncoder.encode(ddlType, "GBK");
			postData += "&ddlName=" + URLEncoder.encode(ddlName, "GBK");
			postData += "&txtTime=" + txtTime;
			postData += "&btnSearch=" + URLEncoder.encode("搜索", "GBK");

			String tmp = execute(postData);
			if(tmp==null){
				return null;
			}
			result = paraseHtml(tmp);

		} catch (SocketTimeoutException e) {
			// TODO Auto-generated catch block
			System.out.println("Connect timed out");
			System.exit(0);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return result;
	}

	public static Map<String, String> paraseHtml(String s) {
		Map<String, String> result = new HashMap<String, String>();

		Document doc = Jsoup.parse(s);
		Element __EVENTTARGET = doc.getElementById("__EVENTTARGET");
		result.put("__EVENTTARGET", __EVENTTARGET.attr("value"));
		Element __EVENTARGUMENT = doc.getElementById("__EVENTARGUMENT");
		result.put("__EVENTARGUMENT", __EVENTARGUMENT.attr("value"));
		Element __LASTFOCUS = doc.getElementById("__LASTFOCUS");
		result.put("__LASTFOCUS", __LASTFOCUS.attr("value"));
		Element __VIEWSTATE = doc.getElementById("__VIEWSTATE");
		result.put("__VIEWSTATE", __VIEWSTATE.attr("value"));
		Element __EVENTVALIDATION = doc.getElementById("__EVENTVALIDATION");
		result.put("__EVENTVALIDATION", __EVENTVALIDATION.attr("value"));

		Element marqueebox = doc.getElementById("marqueebox");
		Elements tds = marqueebox.select("td");
		result.put("Station", tds.get(0).val());
		result.put("Primary", tds.get(1).val());
		Element marqueebox1 = doc.getElementById("marqueebox1");
		Elements tds2 = marqueebox1.select("td");
		result.put("AQI", tds2.get(0).html());
		result.put("Level", tds2.get(1).html());
		result.put("Description", tds2.get(2).html());
		return result;
	}

	public static String execute(String postData) throws SocketTimeoutException {
		final String encode = "gbk";
		final String website = "http://jc.bjmemc.com.cn";
		final int connectTimeOut = 15000;
		final int readDataTimeOut = 50000;
		HttpURLConnection httpConn = null;
		String htmlContent = null;
		String reqUrl = "/AirQualityDaily/DataSearch.aspx";
		try {
			URL url = new URL(website + reqUrl);
			httpConn = (HttpURLConnection) url.openConnection();
			httpConn.setDoInput(true);
			httpConn.setDoOutput(true);
			httpConn.setUseCaches(false);
			httpConn.setConnectTimeout(connectTimeOut);
			httpConn.setReadTimeout(readDataTimeOut);
			httpConn.setRequestProperty("Accept",
					"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
			httpConn.setRequestProperty("Connection", "keep-alive");
			httpConn.setRequestProperty("Cookie",
					"ASP.NET_SessionId=muw03145ed0phmudoczeqg45");
			httpConn.setRequestProperty("Host", "jc.bjmemc.com.cn");
			httpConn.setRequestProperty("Referer",
					"http://jc.bjmemc.com.cn/AirQualityDaily/DataSearch.aspx");// ÓÐÐ©ŒÓÈëÁËÀŽÂ·ÅÐ¶Ï£¬ÄÇÕâžöŸÍÊÇ±ØÐèÒªŒÓµÄÁË
			httpConn.setRequestProperty("User-Agent",
					"Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20100101 Firefox/21.0");

			httpConn.setRequestMethod("POST");
			httpConn.connect();

			OutputStream outStream = httpConn.getOutputStream();
			outStream.write(postData.getBytes());
			outStream.flush();
			outStream.close();
			if(httpConn.getResponseCode()!=200){
				return null;
			}
			if (HttpURLConnection.HTTP_OK == httpConn.getResponseCode()) {
				InputStream inStream = httpConn.getInputStream();
				Reader reader = new BufferedReader(new InputStreamReader(
						inStream, encode));
				StringBuffer content = new StringBuffer();
				char[] buffer = new char[1000];
				int n;
				while ((n = reader.read(buffer)) != -1) {
					content.append(buffer, 0, n);
				}
				htmlContent = content.toString();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (null != httpConn) {
				try {
					httpConn.disconnect();
				} catch (Exception e) {
				}
			}
		} // end-try-catch-finally
		return htmlContent;
	}
}
