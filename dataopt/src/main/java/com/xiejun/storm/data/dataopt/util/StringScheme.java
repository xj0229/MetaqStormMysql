package com.xiejun.storm.data.dataopt.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

@SuppressWarnings("serial")
public class StringScheme implements Scheme{

	public List<Object> deserialize(ByteBuffer ser) {
		// TODO Auto-generated method stub
		try{
			return new Values(new String(ser.array(), MacroDef.ENCODING));
		}catch(UnsupportedEncodingException e){
			throw new RuntimeException(e);
		}
	}

	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return new Fields("str");
	}

}
