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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Delivery;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.Sender;

/**
 *
 */
public class SampleReceiver {

    private static final String QUEUE = "demo-queue";
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleReceiver.class);

    private final Receiver receiver;
    private final Sender sender;

    public SampleReceiver() {
        this.receiver = RabbitFlux.createReceiver();
        this.sender = RabbitFlux.createSender();
    }
    
    long time = System.currentTimeMillis();

    public Disposable consume(String queue, CountDownLatch latch) {
        Mono<AMQP.Queue.DeclareOk> queueDeclaration = sender.declareQueue(QueueSpecification.queue(queue));
        Flux<Delivery> messages = receiver.consumeAutoAck(queue);
        return queueDeclaration
        		.thenMany(messages)
        		.retryBackoff(3, Duration.ofSeconds(1))
        		.delayUntil(a -> Flux.just(1,2).hide().delayElements(Duration.ofSeconds(1)))
        		.subscribe(m -> {
        			long delay =  System.currentTimeMillis() - time;
        			time = System.currentTimeMillis();
		            System.out.println("*************** Received message {}"+ new String(m.getBody())+" with delay (ms) :"+delay);
		            latch.countDown();
		         });
    }

    public void close() {
        this.sender.close();
        this.receiver.close();
    }

}
