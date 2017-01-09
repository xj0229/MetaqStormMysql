package com.xiejun.storm.data.dataopt;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import com.xiejun.storm.data.dataopt.bolt.FilterBolt;
import com.xiejun.storm.data.dataopt.bolt.MetaBolt;
import com.xiejun.storm.data.dataopt.bolt.MysqlBolt;
import com.xiejun.storm.data.dataopt.bolt.PrintBolt;
import com.xiejun.storm.data.dataopt.spout.MetaSpout;
import com.xiejun.storm.data.dataopt.spout.ReadlogSpout;

public class Topology {
	
	private static TopologyBuilder builder = new TopologyBuilder();
	
	public static void main(String[] args){
		Config config = new Config();
		
		builder.setSpout("metaspout", new ReadlogSpout(),1);
		
		//builder.setSpout("metaspout", new MetaSpout("MetaSpout.xml"),1);
		
		builder.setBolt("filterbolt", new FilterBolt("FilterBolt.xml"),1).shuffleGrouping("metaspout");
		
		builder.setBolt("mysql", new MysqlBolt("MysqlBolt.xml"),1).shuffleGrouping("filterbolt");
		
		builder.setBolt("meta", new MetaBolt("MetaBolt.xml"),1).shuffleGrouping("filterbolt");
		
		builder.setBolt("print", new PrintBolt(),1).shuffleGrouping("filterbolt");
		
		config.setDebug(false);
		
		if(args != null && args.length > 0){
			config.setNumWorkers(1);
			
			try {
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else{
			config.setMaxTaskParallelism(1);
			
			LocalCluster cluster = new LocalCluster();
			
			cluster.submitTopology("simple", config, builder.createTopology());
		}
		
		
		
		
		//builder.setBolt("monitor", new MonitorBolt(),3)
		
		
	}

}
