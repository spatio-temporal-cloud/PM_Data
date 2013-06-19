package bigspatialdata.pm.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
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
		crawl();
	}

	public static void crawl() {
		String htmlContent = execute("");
		Map<String, String> result = paraseHtml(htmlContent);
		String postData = "__EVENTTARGET=" + result.get("__EVENTTARGET");
		postData += "&__EVENTARGUMENT=" + result.get("__EVENTARGUMENT");
		postData +=	"&__LASTFOCUS=" + result.get("__LASTFOCUS");
		postData += "&__VIEWSTATE=" + result.get("__VIEWSTATE");
		postData += "&__EVENTVALIDATION=" + result.get("__EVENTVALIDATION");
		try {
			postData += "&ddlType=" + URLEncoder.encode("城市环境评价点", "GBK");
			postData += "&ddlName=" + URLEncoder.encode("东城东四", "GBK");
			postData += "&txtTime=" + "2013-06-12";
			postData += "&btnSearch=" + URLEncoder.encode("搜索","GBK");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		result = paraseHtml(execute(postData));
		System.out.println("AQI: " + result.get("AQI"));
	}

	public static Map<String, String> paraseHtml(String s) {
		Map<String, String> result = new HashMap<String, String>();
		System.out.println("html: " + s);
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
		result.put("AQI", tds2.get(0).val());
		result.put("Level", tds2.get(1).val());
		result.put("Description", tds2.get(2).val());
		return result;
	}

	public static String execute(String postData) {
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
					"User-Agent",
					"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727)");
			httpConn.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
//			httpConn.setRequestProperty("Accept-Encoding", "gzip, deflate");
			httpConn.setRequestProperty("Accept-Language", "zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3");
			httpConn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			httpConn.setRequestProperty("Connection", "Keep-Alive");
			httpConn.setRequestProperty("Cookie", "ASP.NET_SessionId=x5calfjvpx5absb4x4p0ndyp");
			httpConn.setRequestProperty("Host", "jc.bjmemc.com.cn");
			httpConn.setRequestProperty("Cache-Control", "no-cache");
			httpConn.setRequestProperty("Referer", "http://jc.bjmemc.com.cn/AirQualityDaily/DataSearch.aspx");
			httpConn.setRequestMethod("POST");
			httpConn.connect();

			OutputStream outStream = httpConn.getOutputStream();

			outStream.write(postData.getBytes());
			outStream.flush();
			outStream.close();
			System.out.println("Request url: " + (website + reqUrl));
			System.out.println("Post data: " + postData);
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
			System.out.println("================== crawl over. ");
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
