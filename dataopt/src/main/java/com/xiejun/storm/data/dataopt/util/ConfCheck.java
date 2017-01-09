package com.xiejun.storm.data.dataopt.util;

import java.io.File;

import com.xiejun.storm.data.dataopt.bolt.FilterBolt;
import com.xiejun.storm.data.dataopt.bolt.MetaBolt;
import com.xiejun.storm.data.dataopt.bolt.MysqlBolt;
import com.xiejun.storm.data.dataopt.spout.MetaSpout;

public class ConfCheck extends Thread{
	
	private String xmlpath = "xxx.xml";
	private int heartbeat = 1000;
	private String type = "type";
	
	public ConfCheck(String XmlPath, int HeartBeat, String type){
		this.xmlpath = XmlPath;
		this.heartbeat = HeartBeat;
		this.type = type;
	}
	
	public void run(){
		long init_time = 0;
		
		for(int i = 0;;i++){
			try{
				File file = new File(this.xmlpath);
				long lasttime = file.lastModified();
				if(i == 0){
					init_time = lasttime;
				}else{
					if(init_time != lasttime){
						init_time = lasttime;
						if(this.type.equals(MacroDef.Thread_type_metaqspout)){
							MetaSpout.isload();
						}else if(this.type.equals(MacroDef.Thread_type_metaqbolt)){
							MetaBolt.isload();
						}else if(this.type.equals(MacroDef.Thread_type_filterbolt)){
							FilterBolt.isload();
						}else if(this.type.equals(MacroDef.Thread_type_mysqlbolt)){
							MysqlBolt.isload();
						}
					}
				}
				
				Thread.sleep(this.heartbeat);
				
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	
	public static void main(String[] args) throws InterruptedException{
		new ConfCheck("MysqlBolt.xml", 1000, "test").start();
		Thread.sleep(100000);
	}

}
