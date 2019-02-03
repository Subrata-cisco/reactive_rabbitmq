/*
 * Copyright (c) 2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.subrata.reactorrabbit;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.OutboundMessageResult;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.RabbitFluxException;
import reactor.rabbitmq.SendOptions;
import reactor.rabbitmq.Sender;

/**
 *
 */
public class SampleSender {

    private static final String QUEUE = "demo-queue";
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleSender.class);

    private final Sender sender;
    private int countRequested = 0;

    public SampleSender() {
        this.sender = RabbitFlux.createSender();
    }

    public void send(String queue, int count, CountDownLatch latch) {
    	countRequested = count;
    	SendOptions sendOptions = new SendOptions().exceptionHandler(((sendContext, e) -> {
    		OutboundMessage msg = sendContext.getMessage();
			System.out.println("********** Sending failed for msg :("
					+ new String(msg.getBody())+") with error :(" + e.getMessage() + "), will retry based on retry count given..");
	        throw new RabbitFluxException(e);
    	}));
    	
    	
        Flux<OutboundMessageResult> confirmations = sender.sendWithPublishConfirms(Flux.range(1, count)
            .map(i -> new OutboundMessage("", queue, ("Message_" + i).getBytes())),sendOptions);

        sender.declareQueue(QueueSpecification.queue(queue))
            .thenMany(confirmations)
                //.doOnError(e -> System.out.println("*************** Send failed :"+ e))
                //.retry(3)
                .retryBackoff(3, Duration.ofSeconds(1))
                .retry(3, ex -> {
                	   System.out.println("********************* Checking the Condition :"+System.currentTimeMillis()+" count :"+latch.getCount()+" :"+(!(ex instanceof IllegalStateException))+" Exception :"+ex);
                	   boolean shouldTry = !(ex instanceof IllegalStateException);
                	   if(!shouldTry) {
                		   while(latch.getCount()<countRequested) {
                			   latch.countDown();
                		   }
                	   }
                	   return  shouldTry;
                })
                .subscribe(r -> {
                	//System.out.println("*********** SampleSender.send() ack :"+r.isAck());
                    if (r.isAck()) {
                    	global_count++;
                        //System.out.println("SampleSender.send() Message {} sent successfully "+ new String(r.getOutboundMessage().getBody()));
                        latch.countDown();
                    }
                });
    }

    public void close() {
        this.sender.close();
    }
    
    public int getTotalSent() {
    	return global_count;
    }
    
    int global_count = 0;    
}
