package com.xiejun.storm.data.dataopt.bolt;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.xiejun.storm.data.dataopt.util.ConfCheck;
import com.xiejun.storm.data.dataopt.util.MacroDef;
import com.xiejun.storm.data.dataopt.xml.FilterXml;

@SuppressWarnings("serial")
public class FilterBolt implements IRichBolt {
	
	private OutputCollector collector;
	
	private static boolean flag_load = false;
	
	private long register = 0;
	
	String filterXml = "";
	
	private boolean flag_par = true;
	
	String MatchLogic = "AND";
	
	String MatchType = "regular::range::routine0";
	
	String MatchField = "1::2::5";
	
	String FieldValue = ".*baidu.*::1000,2000::ina";
	
	public FilterBolt(String xmlpath){
		if(xmlpath == null){
			flag_par = false;
		}else{
			this.filterXml = xmlpath;
		}
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		String str = arg0.getString(0);
		
		if(this.flag_par == false){
			System.out.println("MonitorBolt----------Error:can not get the path of filter.xml!");
		}else{
			
			if(flag_load == false){
				Loading();
		
				if(register != 0){
					System.out.println("FilterBolt-------Config Change:" + this.filterXml);
				}else{
					System.out.println("FilterBolt-------Config Loaded:" + this.filterXml);
				}
				
				//xiejun
				flag_load = true;
				
			}
			
			boolean moni = MyFilter(str, this.MatchLogic, this.MatchType, this.MatchField, this.FieldValue);
			
			if(moni == true){
				this.collector.emit(new Values(str));
			}
		}
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		System.out.println("FilterBolt------------------start!");
		
		this.collector = arg2;
		
		if(this.flag_par == false){
			System.out.println("MetaSpout------------Error:can not get the path of FilterXml.xml!");
		}else{
			new ConfCheck(this.filterXml, MacroDef.HEART_BEAT, MacroDef.Thread_type_filterbolt).start();
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

	public static void isload() {
		// TODO Auto-generated method stub
		flag_load = false;
	}
	
	public void Loading(){
		System.out.println("FilterXml:" + this.filterXml);
		
		new FilterXml(this.filterXml).read();
		
		this.MatchLogic = FilterXml.MatchLogic;
		
		this.MatchType = FilterXml.MatchType;
		
		this.MatchField = FilterXml.MatchField;
		
		this.FieldValue = FilterXml.FieldValue;
	}
	
	private boolean MyFilter(String str, String logic, String type, String field, String value){
		
		String[] types = type.split(MacroDef.FLAG_COLON);
		String[] fields = field.split(MacroDef.FLAG_COLON);
		String[] values = value.split(MacroDef.FLAG_COLON);
		
		int flag_init = types.length;
		
		int flag = 0;
		
		if(logic.equals(MacroDef.RULE_AND)){
			for(int i = 0; i < flag_init; i++){
				if(types[i].equals(MacroDef.RULE_REGULAR)){
					boolean regu = regular(str, fields[i], values[i]);
					
					if(regu == true){
						flag ++;
					}
					
				}else if(types[i].equals(MacroDef.RULE_RANGE)){
					boolean ran = range(str, fields[i], values[i]);
					
					if(ran == true){
						flag ++;
					}
				}else if(types[i].equals(MacroDef.RULE_ROUTINE0)){
					boolean rou0 = routine0(str, fields[i], values[i]);
					
					if(rou0 == true){
						flag++;
					}
				}else if(types[i].equals(MacroDef.RULE_ROUTINE1)){
					boolean rou1 = routine1(str, fields[i], values[i]);
					
					if(rou1 == true){
						flag ++;
					}
				}
			}
			
			if(flag == flag_init){
				return true;
			}else{
				return false;
			}
			
		}else if(logic.equals(MacroDef.RULE_OR)){
			
			for(int i = 0; i < flag_init; i++){
				if(types[i].equals(MacroDef.RULE_REGULAR)){
					boolean regu = regular(str, fields[i], values[i]);
					
					if(regu == true){
						flag ++;
					}
					
				}else if(types[i].equals(MacroDef.RULE_RANGE)){
					boolean ran = range(str, fields[i], values[i]);
					
					if(ran == true){
						flag ++;
					}
				}else if(types[i].equals(MacroDef.RULE_ROUTINE0)){
					boolean rou0 = routine0(str, fields[i], values[i]);
					
					if(rou0 == true){
						flag++;
					}
				}else if(types[i].equals(MacroDef.RULE_ROUTINE1)){
					boolean rou1 = routine1(str, fields[i], values[i]);
					
					if(rou1 == true){
						flag ++;
					}
				}
			}
			
			if(flag != 0){
				return true;
			}else{
				return false;
			}
		}
		
		return false;
	}
	
	//Regular
	private boolean regular(String str, String field, String value){
		
		String[] strs = str.split(MacroDef.FLAG_TABS);
		
		Pattern p = Pattern.compile(value);
		
		Matcher m = p.matcher(strs[Integer.parseInt(field) - 1]);
		
		boolean result = m.matches();
		
		if(result == true){
			return true;
		}else{
			return false;
		}
	}
	
	//
	private boolean range(String str, String field, String value){
		
		String[] strs = str.split(MacroDef.FLAG_TABS);
		
		String[] values = value.split(MacroDef.FLAG_COMMA);
		
		int strss = Integer.parseInt(strs[Integer.parseInt(field) - 1]);
		
		if(values.length == 1){
			if(strss > Integer.parseInt(values[0])){
				return true;
			}else{
				return false;
			}
		}else if(values.length == 2 && values[0].length() == 0){
			if(strss < Integer.parseInt(values[1])){
				return true;
			}else{
				return false;
			}
		}else if(values.length == 2 && values[0].length() != 0){
			if(strss > Integer.parseInt(values[0]) && strss < Integer.parseInt(values[1])){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
		
	}
	
	//
	private boolean routine0(String str, String field, String value){
		String[] strs = str.split(MacroDef.FLAG_TABS);
		
		String strss = strs[Integer.parseInt(field) - 1];
		
		if(strss.contains(value) && !strss.equals(value)){
			return true;
		}else{
			return false;
		}
	}
	
	//
	private boolean routine1(String str, String field, String value){
		String[] strs = str.split(MacroDef.FLAG_TABS);
		
		String strss = strs[Integer.parseInt(field) - 1];
		
		if(strss.equals(value)){
			return true;
		}else{
			return false;
		}
	}

	
}
