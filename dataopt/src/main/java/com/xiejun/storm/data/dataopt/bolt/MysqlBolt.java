package com.xiejun.storm.data.dataopt.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.xiejun.storm.data.dataopt.util.ConfCheck;
import com.xiejun.storm.data.dataopt.util.MacroDef;
import com.xiejun.storm.data.dataopt.util.MysqlOpt;
import com.xiejun.storm.data.dataopt.xml.MysqlXml;

public class MysqlBolt implements IRichBolt {
	
	private OutputCollector collector;
	
	private static boolean flag_load = false;
	
	private long register = 0;
	
	String mysqlXml = "Mysql.xml";
	
	MysqlOpt mysql = new MysqlOpt();
	
	private boolean flag_par = true;
	
	private boolean flag_xml = true;
	
	String form = "monitor";
	
	public MysqlBolt(String MysqlXML){
		if(MysqlXML == null){
			flag_par = false;
		}else{
			this.mysqlXml = MysqlXML;
		}
		
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		String str = arg0.getString(0);
		
		if(this.flag_par == false){
			
			System.out.println("MysqlBolt------------------Error: can not get the path of MysqlBolt.xml!");
			
		}else{
			
			if(flag_load == false){
				
				Loading();
				
				if(register != 0){
					System.out.println("MysqlBolt--------------Config Change" + this.mysqlXml);
				}else{
					System.out.println("MysqlBolt--------------Config Loaded" + this.mysqlXml);
				}
				
				//xiejun20161215
				flag_load = true;
				
			}
			
			if(this.flag_xml == true){
				
				String sql = send_str(str);
				
				if(this.mysql.insertSQL(sql) == false){
					System.out.println("MysqlBolt----------------Error: can not insert tuple into database!");
					
					System.out.println("MysqlBolt----------------Error Tuple: " + str);
					
					System.out.println("SQL: " + sql);
				}
				
			}
			
		}
		
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		System.out.println("MysqlBolt-------------------------Start!");
		
		this.collector = arg2;
		
		if(this.flag_par == false){
			System.out.println("MysqlBolt-------------------Error: can not get the path of MysqlBolt.xml");
		}else{
			new ConfCheck(this.mysqlXml, MacroDef.HEART_BEAT, MacroDef.Thread_type_mysqlbolt).start();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public static void isload() {
		// TODO Auto-generated method stub
		flag_load = false;
	}
	
	public void Loading(){
		
		new MysqlXml(this.mysqlXml).read();
		
		String host_port = MysqlXml.Host_port;
		
		String database = MysqlXml.DatabaseName;
		
		String username = MysqlXml.UserName;
		
		String password = MysqlXml.Password;
		
		this.form = MysqlXml.Form;
		
		if(this.mysql.connSQL(host_port, database, username, password) == false){
			
			System.out.println("MysqlBolt-------------Config error, please check Mysql-conf: " + this.mysqlXml);
			
			flag_xml = false;
			
		}else{
			System.out.println("MysqlBolt-----------------Config Loaded: " + this.mysqlXml);
		}
	}
	
	public String send_str(String str){
		
		String send_temp = null;
		
		String[] field = str.split(MacroDef.FLAG_TABS);
		
		for(int i = 0; i < field.length; i++){
			
			if(i == 0){
				send_temp = "'" + field[0] + "','";
			}else if(i == (field.length - 1)){
				send_temp = send_temp + field[i] + "'";
			}else{
				send_temp = send_temp + field[i] + "','";
			}
		}
		
		String send = "insert into " + this.form + "(domain, value, time, validity, seller) values (" + send_temp + ");";
		
		return send;
		
	}

}
