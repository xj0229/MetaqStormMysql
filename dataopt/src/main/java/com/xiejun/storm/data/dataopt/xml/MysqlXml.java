package com.xiejun.storm.data.dataopt.xml;

import java.io.File;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.xiejun.storm.data.dataopt.util.MacroDef;

public class MysqlXml {
	
	private static String xmlfilePath;
	
	public static String Host_port;
	
	public static String DatabaseName;
	
	public static String Form;
	
	public static String UserName;
	
	public static String Password;
	
	@SuppressWarnings("static-access")
	public MysqlXml(String str){
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
			
			Host_port = e.getElementsByTagName(MacroDef.Host_port).item(0).getFirstChild().getNodeValue();
			
			DatabaseName = e.getElementsByTagName(MacroDef.Database).item(0).getFirstChild().getNodeValue();
			
			Form = e.getElementsByTagName(MacroDef.Form).item(0).getFirstChild().getNodeValue();
			
			UserName = e.getElementsByTagName(MacroDef.Username).item(0).getFirstChild().getNodeValue();
			
			Password = e.getElementsByTagName(MacroDef.Password).item(0).getFirstChild().getNodeValue();
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	
}
