package bigspatialdata.pm.data;

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
import java.util.HashMap;
import java.util.Map;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jsoup.Jsoup;

public class History_BJMEMC {

	public static void main(String args[]) {
		System.out.println("Begin to Crawl...");
		Map<String, String> result = crawl("城市环境评价点","东城东四","2013-06-18");
		System.out.println(result.get("AQI"));
		System.out.println("================== crawl over. ");
		
	}

	public static Map<String, String> crawl(String ddlType,String ddlName,String txtTime) {
		Map<String, String> result = null;
		try {
			String htmlContent = execute("");
			result = paraseHtml(htmlContent);
			String postData = "__EVENTTARGET=" + java.net.URLEncoder.encode(result.get("__EVENTTARGET"),"GBK");
			postData += "&__EVENTARGUMENT=" + java.net.URLEncoder.encode(result.get("__EVENTARGUMENT"),"GBK");
			postData +=	"&__LASTFOCUS=" + java.net.URLEncoder.encode(result.get("__LASTFOCUS"),"GBK");
			postData += "&__VIEWSTATE=" + java.net.URLEncoder.encode(result.get("__VIEWSTATE"),"GBK");
			postData += "&__EVENTVALIDATION=" + java.net.URLEncoder.encode(result.get("__EVENTVALIDATION"),"GBK");
		
			postData += "&ddlType=" + URLEncoder.encode(ddlType, "GBK");
			postData += "&ddlName=" + URLEncoder.encode(ddlName, "GBK");
			postData += "&txtTime=" + txtTime;
			postData += "&btnSearch=" + URLEncoder.encode("搜索","GBK");
			
			result = paraseHtml(execute(postData));
			
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
			httpConn.setRequestProperty(
					"Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"); 
			httpConn.setRequestProperty(
					"Connection", "keep-alive");
			httpConn.setRequestProperty(
					"Cookie","ASP.NET_SessionId=muw03145ed0phmudoczeqg45");
			httpConn.setRequestProperty(
					"Host","jc.bjmemc.com.cn");
			httpConn.setRequestProperty(
					"Referer", "http://jc.bjmemc.com.cn/AirQualityDaily/DataSearch.aspx");//ÓÐÐ©ŒÓÈëÁËÀŽÂ·ÅÐ¶Ï£¬ÄÇÕâžöŸÍÊÇ±ØÐèÒªŒÓµÄÁË
			httpConn.setRequestProperty(
			        "User-Agent", "Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20100101 Firefox/21.0");

			httpConn.setRequestMethod("POST");
			httpConn.connect();

			OutputStream outStream = httpConn.getOutputStream();			
			outStream.write(postData.getBytes());
			outStream.flush();
			outStream.close();
			
			System.out.println("Response code: " + httpConn.getResponseCode());
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
		}finally {
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
