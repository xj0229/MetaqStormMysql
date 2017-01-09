package com.xiejun.storm.data.dataopt.xml;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.xiejun.storm.data.dataopt.util.MacroDef;

public class MetaXml {
	
	private static String xmlfilePath;
	
	public static String MetaTopic;
	
	public static String MetaZkConnect;
	
	public static String MetaZkRoot;
	
	@SuppressWarnings("static-access")
	public MetaXml(String str){
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
			
			MetaTopic = e.getElementsByTagName(MacroDef.MetaTopic).item(0).getFirstChild().getNodeValue();
			
			MetaZkConnect = e.getElementsByTagName(MacroDef.MetaZkConnect).item(0).getFirstChild().getNodeValue();
			
			MetaZkRoot = e.getElementsByTagName(MacroDef.MetaZkRoot).item(0).getFirstChild().getNodeValue();
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
