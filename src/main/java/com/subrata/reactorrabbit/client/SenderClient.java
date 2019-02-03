package com.subrata.reactorrabbit.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.subrata.reactorrabbit.SampleSender;

public class SenderClient {
	
	/**
	 * Observations :
	 * 1) It is very difficult to hit the memory limit ([{rabbit, [{vm_memory_high_watermark, 0.01}]}].)
	 * 2) As soon as memory limit goes , producer slows down little and becomes green immediately.
	 * 3) After inserting 73 lacs message once it gave error and tells correctly the reason with message.
	 * 4) It has retry option in case of connection fails, it tries that many times.
	 * 5) Best part Producer sends ack immediately once message delivered and super fast delivery.
	 * 6) When exception happened , producer ack value is false, so on failure we get 2 info (i.e exception,msg body and ack result.)
	 */
	public static void main(String[] args) throws InterruptedException {
        int count = 2000000;
        CountDownLatch sendLatch = new CountDownLatch(count);
        SampleSender sender = new SampleSender();
        sender.send(Constants.queue, count, sendLatch);
        sendLatch.await(10, TimeUnit.MINUTES);
        System.out.println("*************** SenderClient.main() total messages sent :"+sender.getTotalSent());
        sender.close();
	}

}
