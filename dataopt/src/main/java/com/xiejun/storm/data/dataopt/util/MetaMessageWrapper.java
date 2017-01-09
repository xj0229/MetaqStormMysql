package com.xiejun.storm.data.dataopt.util;

import java.util.concurrent.CountDownLatch;

import com.taobao.metamorphosis.Message;

public final class MetaMessageWrapper {
	public final Message message;
	
	public final CountDownLatch latch;
	
	public volatile boolean success = false;
	
	public MetaMessageWrapper(final Message message){
		super();
		this.message = message;
		this.latch = new CountDownLatch(1);
	}

}
