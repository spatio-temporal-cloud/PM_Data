package bigspatialdata.city.data;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

import bigspatialdata.pm.data.Data;

public class City_Code {
	public static void city_code() throws JSONException{
		ArrayList<String> cities = Data.getCities();
		ArrayList<String> sqls = new ArrayList<String>();
		for(int i=0;i<cities.size();i++){
			if(cityCodeExist(cities.get(i))){
				continue;
			}
			String cityNameCh = cities.get(i);
			String url = "http://www.youdao.com/smartresult-xml/search.s?type=zip&q="+cityNameCh+"&jsFlag=true";
			System.out.println(url);
			String result = Data.callAPI(url);
			if(result.indexOf("phone")!=-1){
				String tmp = result.substring(result.indexOf("{"),result.length()-1);
				String tmp2 = tmp.substring(0,tmp.indexOf("}")+1);
				JSONObject obj = new JSONObject(tmp2);
				String cityCode = obj.getString("phone");
				System.out.println(cities.get(i) + " "+cityCode);
				String sql = "update City set cityCode='"+cityCode+"' where cityNameCh='"+cityNameCh+"'";
				sqls.add(sql);
			}else{
				System.out.println(cities.get(i)+" has no result");
			}
		}
		if(sqls.size()!=0){
			Data.addData(sqls);
			System.out.println(sqls.size()+" records added");
		}
	}
	public static boolean cityCodeExist(String cityNameCh){
		boolean tag=true;
		try {
			java.sql.Connection conn = DriverManager.getConnection(
			    		"jdbc:mysql://10.214.0.147/pm0?useUnicode=true&characterEncoding=UTF-8", "pm", "ccntgrid");
			java.sql.Statement stmt = conn.createStatement();
			ResultSet result = stmt.executeQuery("select * from City where cityCode!='NULL' and cityNameCh='"+cityNameCh+"';");
			if(result.next()){
				tag=true;
			}else{
				tag=false;
			}
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Unable to read city list. Failed to connect to mysql!");
		}
		return tag;
	}
}
