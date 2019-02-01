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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

    public SampleSender() {
        this.sender = RabbitFlux.createSender();
    }

    public void send(String queue, int count, CountDownLatch latch) {
    	
    	SendOptions sendOptions = new SendOptions().exceptionHandler(((sendContext, e) -> {
    		OutboundMessage msg = sendContext.getMessage();
    		System.out.println("********** SampleSender.send() Exception handler :"+e.getMessage()+"  for msg :"+new String(msg.getBody()));
            throw new RabbitFluxException(e);
    	}));
    	
    	
        Flux<OutboundMessageResult> confirmations = sender.sendWithPublishConfirms(Flux.range(1, count)
            .map(i -> new OutboundMessage("", queue, ("Message_" + i).getBytes())),sendOptions);

        sender.declareQueue(QueueSpecification.queue(queue))
            .thenMany(confirmations)
                .doOnError(e -> System.out.println("*************** Send failed :"+ e))
                //.retry(3)
                .subscribe(r -> {
                	System.out.println("*********** SampleSender.send() ack :"+r.isAck());
                    if (r.isAck()) {
                        LOGGER.info("Message {} sent successfully", new String(r.getOutboundMessage().getBody()));
                        System.out.println("SampleSender.send() Message {} sent successfully "+ new String(r.getOutboundMessage().getBody()));
                        latch.countDown();
                    }
                });
    }

    public void close() {
        this.sender.close();
    }

    public static void main(String[] args) throws Exception {
        int count = 20;
        CountDownLatch latch = new CountDownLatch(count);
        SampleSender sender = new SampleSender();
        sender.send(QUEUE, count, latch);
        latch.await(10, TimeUnit.SECONDS);
        sender.close();
    }

}
