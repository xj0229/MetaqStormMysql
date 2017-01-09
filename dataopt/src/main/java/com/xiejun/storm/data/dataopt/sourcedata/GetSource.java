package com.xiejun.storm.data.dataopt.sourcedata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

public class GetSource {
	
	public static void main(String[] args){
		
		Random random = new Random();
		
		int note_num = 10000;
		
		String[] net0 = {"baidu", "google", "hadoop", "sony", "storm"};
		
		String[] net1 = {"com", "net", "cn", "edu", "org"};
		
		String[] times = {"2000", "2001", "1998", "2007", "1949"};
		
		String[] value = {"1326", "1446", "300", "900", "200"};
		
		String[] validity = {"3", "5", "20", "100", "45"};
		
		String[] seller = {"Huang", "Lina", "Nina", "Litao", "sid"};
		
		FileOutputStream fos = null;
		
		try{
			fos = new FileOutputStream(new File("domain.log"));
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}
		
		OutputStreamWriter osw = null;
		
		try{
			osw = new OutputStreamWriter(fos, "UTF-8");
		}catch(UnsupportedEncodingException e){
			e.printStackTrace();
		}
		
		BufferedWriter bw = new BufferedWriter(osw);
		
		for(int i = 0; i < note_num; i++){
			String net = "www." + net0[random.nextInt(5)] + "." + net1[random.nextInt(5)];
			
			String records = net + "\t" + value[random.nextInt(5)] + "\t" + times[random.nextInt(5)] + "\t" + validity[random.nextInt(5)] + "\t" + seller[random.nextInt(5)];
			
			try{
				bw.write(records);
				bw.newLine();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		
		try{
			bw.close();
			osw.close();
			fos.close();
			System.out.println("Write OK!");
		}catch(IOException e){
			e.printStackTrace();
		}
		
	}

}
