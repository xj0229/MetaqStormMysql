package com.xiejun.storm.data.dataopt.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;
import com.xiejun.storm.data.dataopt.util.ConfCheck;
import com.xiejun.storm.data.dataopt.util.MacroDef;
import com.xiejun.storm.data.dataopt.xml.MetaXml;

public class MetaBolt implements IRichBolt {
	
	private MetaClientConfig metaClientConfig;
	
	private static boolean flag_load = false;
	
	private transient MessageSessionFactory sessionFactory;
	
	private transient MessageProducer messageProducer;
	
	public static String Topic;
	
	private transient SendResult sendResult;
	
	String metaBoltXml = "MetaBolt.xml";
	
	private boolean flag_par = true;
	
	private long meta_debug = 10000;
	
	private long register = 0;
	
	private long reg_tmp = 0;
	
	public MetaBolt(String MetaXml){
		super();
		
		if(MetaXml == null){
			flag_par = false;
		}else{
			this.metaBoltXml = MetaXml;
		}
		
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		String str = arg0.getString(0);
		
		if(this.flag_par == false){
			System.out.println("MetaBolt---------------------Error:can not get the path of MetaBolt.xml!");
		}else{
			try{
				if(flag_load == false){
					Loading();
					
					if(register != 0){
						System.out.println("MetaBolt------------------------Config Change:" + this.metaBoltXml);
					}else{
						System.out.println("MetaBolt------------------------Config Loaded:" + this.metaBoltXml);
					}
				}
				
				if(str != null){
					str += MacroDef.FLAG_ROW;
					
					this.sendResult = this.messageProducer.sendMessage(new Message(this.Topic, str.getBytes()));
					
					if(!sendResult.isSuccess()){
						
						System.err.println("MetaBolt-----------------------Send message failed, error message:" + sendResult.getErrorMessage());
						
						System.err.println("MetaBolt-----------------------Error Tuple:" + str);
					}else{
						this.register++;
						
						if(this.register >= this.reg_tmp){
							
							if(MacroDef.meta_flag == true){
								System.out.println("MetaBolt---------------------Send Tuple Count:" + this.register);
							}
							
							this.reg_tmp = this.register + this.meta_debug;
							
						}
					}
				}
				
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		System.out.println("MetaBolt----------------------start!");
		
		this.reg_tmp = MacroDef.meta_debug;
		
		if(this.flag_par == false){
			System.out.println("MetaBolt-------------------Error:can not get the path of MetaBolt.xml!");
		}else{
			new ConfCheck(this.metaBoltXml, MacroDef.HEART_BEAT, MacroDef.Thread_type_metaqbolt).start();
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
		
		new MetaXml(this.metaBoltXml).read();
		
		this.Topic = MetaXml.MetaTopic;
		
		ZKConfig zkconf = new ZKConfig();
		
		zkconf.zkConnect = MetaXml.MetaZkConnect;
		
		zkconf.zkRoot = MetaXml.MetaZkRoot;
		
		MetaClientConfig metaConf = new MetaClientConfig();
		
		metaConf.setZkConfig(zkconf);
		
		this.metaClientConfig = metaConf;
		
		if(this.Topic == null){
			throw new IllegalArgumentException(this.Topic + ": is null!");
		}
		
		try{
			this.sessionFactory = new MetaMessageSessionFactory(this.metaClientConfig);
			
			this.messageProducer = this.sessionFactory.createProducer();
			
			this.messageProducer.publish(this.Topic);
		}catch(final MetaClientException e){
			e.printStackTrace();
		}
		
	}

}
