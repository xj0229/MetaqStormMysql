package com.xiejun.storm.data.dataopt.util;

public class MacroDef {
	
	public static final String FLAG_COMMA = ",";
	public static final String FLAG_TABS = "\t";
	public static final String FLAG_COLON = "::";
	public static final String FLAG_ROW = "\n";
	
	
	public static final String RULE_AND = "AND";
	public static final String RULE_OR = "OR";
	public static final String RULE_REGULAR = "regular";
	public static final String RULE_RANGE = "range";
	public static final String RULE_ROUTINE0 = "routine0";
	public static final String RULE_ROUTINE1 = "routine1";
	
	
	public static final String ENCODING = "UTF-8";
	
	
	public static final String Parameter = "Parameter";
	
	
	public static final String MatchLogic = "MatchLogic";
	public static final String MatchType = "MatchType";
	public static final String MatchField = "MatchField";
	public static final String FieldValue = "FieldValue";
	
	
	public static final String Host_port = "Host_port";
	public static final String Database = "Database";
	public static final String Username = "Username";
	public static final String Password = "Password";
	public static final String Form = "Form";
	
	
	public static final String MetaRevTopic = "MetaRevTopic";
	public static final String MetaZkConnect = "MetaZkConnect";
	public static final String MetaZkRoot = "MetaZkRoot";
	public static final String MetaConsumerConf = "MetaConsumerConf";
	
	
	public static final String MetaTopic = "MetaTopic";
	
	
	public static final long SPOUT_DEBUG = 1000;
	public static final boolean SPOUT_FLAG = false;
	
	
	public static final long meta_debug = 1000;
	public static final boolean meta_flag = false;
	
	public static final int HEART_BEAT = 1000;
	
	public static final String Thread_type_metaqspout = "MetaSpout";
	public static final String Thread_type_filterbolt = "FilterBolt";//filter
	public static final String Thread_type_mysqlbolt = "MysqlBolt";
	public static final String Thread_type_metaqbolt = "MetaBolt";

}