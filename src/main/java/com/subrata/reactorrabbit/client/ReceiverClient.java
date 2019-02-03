package com.subrata.reactorrabbit.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.subrata.reactorrabbit.SampleReceiver;

public class ReceiverClient {
	public static void main(String[] args) throws InterruptedException {
        int count = 20000;
        CountDownLatch receiveLatch = new CountDownLatch(count);
        SampleReceiver receiver = new SampleReceiver();
        receiver.consume(Constants.queue, receiveLatch);
        receiveLatch.await(10, TimeUnit.MINUTES);
        receiver.close();
	}
}
