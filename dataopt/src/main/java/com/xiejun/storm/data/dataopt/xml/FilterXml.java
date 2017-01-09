package com.xiejun.storm.data.dataopt.xml;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.xiejun.storm.data.dataopt.util.MacroDef;

public class FilterXml {
	
	private static String xmlfilePath;
	
	public static String MatchLogic;
	
	public static String MatchType;
	
	public static String MatchField;
	
	public static String FieldValue;
	
	@SuppressWarnings("static-access")
	public FilterXml(String str){
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
			
			MatchLogic = e.getElementsByTagName(MacroDef.MatchLogic).item(0).getFirstChild().getNodeValue();
			
			MatchType = e.getElementsByTagName(MacroDef.MatchType).item(0).getFirstChild().getNodeValue();
			
			MatchField = e.getElementsByTagName(MacroDef.MatchField).item(0).getFirstChild().getNodeValue();
			
			FieldValue = e.getElementsByTagName(MacroDef.FieldValue).item(0).getFirstChild().getNodeValue();
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
