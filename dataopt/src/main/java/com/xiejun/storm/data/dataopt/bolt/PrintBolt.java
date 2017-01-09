package com.xiejun.storm.data.dataopt.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class PrintBolt extends BaseBasicBolt {

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		try{
			
			String mesg = input.getString(0);
			
			if(mesg != null){
				System.out.println("Tuple: " + mesg);
			}
			
		}catch(Exception e){
			e.printStackTrace();
			
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("mesg"));
	}

}
