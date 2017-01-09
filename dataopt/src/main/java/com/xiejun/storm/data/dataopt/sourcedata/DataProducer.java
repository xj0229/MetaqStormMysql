package com.xiejun.storm.data.dataopt.sourcedata;

import java.util.Random;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils;

public class DataProducer {
	
	private static String topic = "xiejundata-test";
	private static String zkRoot = "/meta";
	private static String zkConnect = "192.168.1.113:2181";
	
	private static MetaClientConfig metaClientConfig;
	private transient static MessageSessionFactory sessionFactory;
	private transient static MessageProducer messageProducer;
	private transient static SendResult sendResult;
	
	public static void main(String[] args) throws MetaClientException, InterruptedException{
		Random random = new Random();
		
		String[] net0 = {"baidu", "google", "hadoop", "sony", "storm"};
		
		String[] net1 = {"com", "net", "cn", "edu", "org"};
		
		String[] times = {"2000", "2001", "1998", "2007", "1949"};
		
		String[] value = {"1326", "1446", "300", "900", "200"};
		
		String[] validity = {"3", "5", "20", "100", "45"};
		
		String[] seller = {"Huang", "Lina", "Nina", "Litao", "sid"};
		
		ZkUtils.ZKConfig zkconf = new ZkUtils.ZKConfig();
		
		zkconf.zkConnect = zkConnect;
		
		zkconf.zkRoot = zkRoot;
		
		MetaClientConfig metaconf = new MetaClientConfig();
		
		metaconf.setZkConfig(zkconf);
		
		metaClientConfig = metaconf;
		
		sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
		
		messageProducer = sessionFactory.createProducer();
		
		messageProducer.publish(topic);
		
		while(true){
			String net = "www." + net0[random.nextInt(5)] + "." + net1[random.nextInt(5)];
			
			String records = net + "\t" + value[random.nextInt(5)] + "\t" + times[random.nextInt(5)] + "\t" + validity[random.nextInt(5)] + "\t" + seller[random.nextInt(5)];
			
			try{
				
				sendResult = messageProducer.sendMessage(new Message(topic, records.getBytes()));
			}catch(Exception e){e.printStackTrace();}

			
			if(sendResult.isSuccess()){
				System.out.println(records + " emit success!");
			}else{
				System.out.println(records + " emit failed!");
			}
			
			Thread.sleep(100);
			
		}
		
	}
	

}
