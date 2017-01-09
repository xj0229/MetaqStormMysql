package com.xiejun.storm.data.dataopt.spout;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.xiejun.storm.data.dataopt.util.MacroDef;

public class ReadlogSpout implements IRichSpout {
	
	private SpoutOutputCollector collector;
	
	FileInputStream fis;
	
	InputStreamReader isr;
	
	BufferedReader br;

	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	public void activate() {
		// TODO Auto-generated method stub
		
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		String str = "";
		
		try{
			while((str = this.br.readLine()) != null){
				this.collector.emit(new Values(str));
				Thread.sleep(100);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
		
		String file = "domain.log";
		
		try{
			
			this.fis = new FileInputStream(file);
			
			this.isr = new InputStreamReader(fis, MacroDef.ENCODING);
			
			this.br = new BufferedReader(isr);
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("str"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
