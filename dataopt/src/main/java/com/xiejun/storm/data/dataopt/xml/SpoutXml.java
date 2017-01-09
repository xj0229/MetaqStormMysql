package com.xiejun.storm.data.dataopt.xml;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.xiejun.storm.data.dataopt.util.MacroDef;

public class SpoutXml {
	
	private static String xmlfilePath;
	
	public static String MetaRevTopic;
	
	public static String MetaZkConnect;
	
	public static String MetaZkRoot;
	
	public static String MetaConsumerConf;
	
	@SuppressWarnings("static-access")
	public SpoutXml(String str){
		this.xmlfilePath = str;
	}
	
	@SuppressWarnings("static-access")
	public void read(){
		try{
			File file = new File(this.xmlfilePath);
			
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			
			DocumentBuilder db = dbf.newDocumentBuilder();
			
			Document doc = db.parse(file);
			
			NodeList nl = doc.getElementsByTagName(MacroDef.Parameter);
			
			Element e = (Element)nl.item(0);
			
			MetaRevTopic = e.getElementsByTagName(MacroDef.MetaRevTopic).item(0).getFirstChild().getNodeValue();
			
			MetaZkConnect = e.getElementsByTagName(MacroDef.MetaZkConnect).item(0).getFirstChild().getNodeValue();
			
			MetaZkRoot = e.getElementsByTagName(MacroDef.MetaZkRoot).item(0).getFirstChild().getNodeValue();
			
			MetaConsumerConf = e.getElementsByTagName(MacroDef.MetaConsumerConf).item(0).getFirstChild().getNodeValue();
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	

}
