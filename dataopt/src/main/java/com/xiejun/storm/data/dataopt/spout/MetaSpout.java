package com.xiejun.storm.data.dataopt.spout;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;
import com.xiejun.storm.data.dataopt.util.ConfCheck;
import com.xiejun.storm.data.dataopt.util.MacroDef;
import com.xiejun.storm.data.dataopt.util.MetaMessageWrapper;
import com.xiejun.storm.data.dataopt.util.StringScheme;
import com.xiejun.storm.data.dataopt.xml.SpoutXml;

@SuppressWarnings("serial")
public class MetaSpout implements IRichSpout {
	
	public static final String FETCH_MAX_SIZE = "meta.fetch.max_size";
	public static final String TOPIC = "meta.topic";
	public static final int DEFAULT_MAX_SIZE = 128 * 1024;
	private transient MessageConsumer messageConsumer;
	private transient MessageSessionFactory sessionFactory;
	private MetaClientConfig metaClientConfig;
	private ConsumerConfig consumerConfig;
	static final Log log = LogFactory.getLog(MetaSpout.class);
	public static final long WAIT_FOR_NEXT_MESSAGE = 1L;
	private transient ConcurrentHashMap<Long, MetaMessageWrapper> id2wrapperMap;
	private transient SpoutOutputCollector collector;
	private transient LinkedTransferQueue<MetaMessageWrapper> messageQueue;
	private long spout_debug = 5;
	private long register = 0;
	private long reg_tmp = 0;
	private boolean spout_flag = true;
	String topic = "storm-test";
	private final Scheme scheme = new StringScheme();
	private boolean flag_par = true;
	private String spoutXml = "MetaSpout.xml";
	private static boolean flag_load = false;
	private Map conf = null;
	
	public MetaSpout(String SpoutXml){
		super();
		if(SpoutXml == null){
			this.flag_par = false;
		}else{
			this.spoutXml = SpoutXml;
		}
		
	}
	

	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		if(arg0 instanceof Long){
			final long id = (Long)arg0;
			
			final MetaMessageWrapper wrapper = this.id2wrapperMap.remove(id);
			
			if(wrapper == null){
				System.out.println("MetaSpout-------------ack");
				log.warn(String.format("do not know how to ack(%s:%s)", arg0.getClass().getName(), arg0));
				return;
			}
			
			wrapper.success = true;
			
			wrapper.latch.countDown();
			
		}else{
			System.out.println("MetaSpout-------------ack");
			log.warn(String.format("do not know how to ack(%s:%s)", arg0.getClass().getName(), arg0));
		}
		
	}

	public void activate() {
		// TODO Auto-generated method stub
		
	}

	public void close() {
		// TODO Auto-generated method stub
		try{
			this.messageConsumer.shutdown();
		}catch(final MetaClientException e){
			log.error("Shutdown consumer failed", e);
		}
		
		try{
			this.sessionFactory.shutdown();
		}catch(final MetaClientException e){
			log.error("Shutdown seesion factory failed", e);
		}
	}

	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		if(arg0 instanceof Long){
			final long id = (Long)arg0;
			
			final MetaMessageWrapper wrapper = this.id2wrapperMap.remove(id);
			
			if(wrapper == null){
				System.out.println("MetaSpout-------------fail");
				log.warn(String.format("do not know how to reject(%s:%s)", arg0.getClass().getName(), arg0));
				return;
			}
			
			wrapper.success = false;
			
			wrapper.latch.countDown();
			
		}else{
			System.out.println("MetaSpout-------------fail");
			log.warn(String.format("do not know how to reject(%s:%s)", arg0.getClass().getName(), arg0));
		}
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		if(this.flag_par == false){
			System.out.println("MetaSpout-------Error:cannot get the path of Spout.xml!");
		}else{
			if(flag_load == false){
				Loading();
				if(register != 0){
					System.out.println("MetaSpout------Config Change: " + this.spoutXml);
				}else{
					System.out.println("MetaSpout------Config Loaded: " + this.spoutXml);
				}
			}
			
			if(this.messageConsumer != null){
				try{
					final MetaMessageWrapper wrapper = this.messageQueue.poll(WAIT_FOR_NEXT_MESSAGE, TimeUnit.MILLISECONDS);
					
					if(wrapper == null){
						return;
					}
					
					final Message message = wrapper.message;
					
					this.register++;
					
					if(this.register >= this.reg_tmp){
						if(this.spout_flag == true){
							System.out.println("MetaSpout-------Send Tuple Count: " + this.register);
						}
						
						this.reg_tmp = this.register + this.spout_debug;
					}
					
					this.collector.emit(this.scheme.deserialize(ByteBuffer.wrap(message.getData())), message.getId());
					
				}catch(final InterruptedException e){}
			}
		}
		
		
		
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		System.out.println("MetaSpout---------------------Start!");
		
		this.collector = arg2;
		this.conf = arg0;
		this.spout_debug = MacroDef.SPOUT_DEBUG;
		this.spout_flag = MacroDef.SPOUT_FLAG;
		
		this.reg_tmp = this.spout_debug;
		
		if(this.flag_par == false){
			System.out.println("MetaSpout---------Error:can't get the path of Spout.xml!");
		}else{
			new ConfCheck(this.spoutXml, MacroDef.HEART_BEAT, MacroDef.Thread_type_metaqspout).start();
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(this.scheme.getOutputFields());
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static void isload(){
		flag_load = false;
	}
	
	public void Loading(){
		
		new SpoutXml(this.spoutXml).read();
		
		String MetaRevTopic = SpoutXml.MetaRevTopic;
		
		String MetaZkConnect = SpoutXml.MetaZkConnect;
		
		String MetaZkRoot = SpoutXml.MetaZkRoot;
		
		String MetaConsumerGroup = SpoutXml.MetaConsumerConf;
		
		ZKConfig zkconf = new ZKConfig();
		
		zkconf.zkConnect = MetaZkConnect;
		
		zkconf.zkRoot = MetaZkRoot;
		
		MetaClientConfig metaconf = new MetaClientConfig();
		
		metaconf.setZkConfig(zkconf);
		
		this.metaClientConfig = metaconf;
		
		this.consumerConfig = new ConsumerConfig(MetaConsumerGroup);
		
		this.topic = MetaRevTopic;
		
		if(this.topic == null){
			throw new IllegalArgumentException(TOPIC + "is null");
		}
		
		Integer maxSize = (Integer)conf.get(FETCH_MAX_SIZE);
		
		if(maxSize == null){
			log.warn("Using default FETCH_MAX_SIZE");
			maxSize = DEFAULT_MAX_SIZE;
		}
		
		this.id2wrapperMap = new ConcurrentHashMap<Long, MetaMessageWrapper>();
		
		this.messageQueue = new LinkedTransferQueue<MetaMessageWrapper>();
		
		try{
			this.setUpMeta(this.topic, maxSize);
		}catch(final MetaClientException e){
			log.error("Setup meta consumer failed", e);
		}
		
	}
	
	private void setUpMeta(final String topic, final Integer maxSize) throws MetaClientException{
		
		this.sessionFactory = new MetaMessageSessionFactory(this.metaClientConfig);
		
		this.messageConsumer = this.sessionFactory.createConsumer(this.consumerConfig);
		
		this.messageConsumer.subscribe(topic, maxSize, new MessageListener(){

			public Executor getExecutor() {
				// TODO Auto-generated method stub
				return null;
			}

			public void recieveMessages(Message arg0) throws InterruptedException {
				// TODO Auto-generated method stub
				final MetaMessageWrapper wrapper = new MetaMessageWrapper(arg0);
				MetaSpout.this.id2wrapperMap.put(arg0.getId(), wrapper);
				MetaSpout.this.messageQueue.offer(wrapper);
				try{
					wrapper.latch.await();
				}catch(final InterruptedException e){
					Thread.currentThread().interrupt();
				}
				
				if(!wrapper.success){
					throw new RuntimeException("MetaSpout Obtain data fail!");
				}
				
			}}).completeSubscribe();
	}
	
/*	private ByteBuffer byteArrayToByteBuffer(byte[] byteArray){
		//byte[] to Object
		ByteArrayInputStream bais = new ByteArrayInputStream(byteArray);
		ObjectInputStream ois = new ObjectInputStream(bais);
		Object obj = ois.readObject();
		ois.close();
		bais.close();
		//Object to ByteBuffer
		byte[] bytes = ByteUtil.get
		
		
		
	}*/

}
