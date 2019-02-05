package com.subrata.purerabbit;

import java.io.IOException;
import java.util.Date;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class PureRabbitReceiver {
	
	private final static String QUEUE_NAME = "rabbitmq-queue";
	  private final static String BROKER_HOST = "localhost";
	  private final static int BROKER_PORT = 5672;
	  private final static String USERNAME = "guest";
	  private final static String PASSWORD = "guest";

	  public PureRabbitReceiver() {}

	  public static void main(String[] argv) throws Exception {
	    ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost(BROKER_HOST);
	    factory.setPort(BROKER_PORT);
	    factory.setUsername(USERNAME);
	    factory.setPassword(PASSWORD);
	    
	    com.rabbitmq.client.Connection connection = factory.newConnection();
	    connection.addBlockedListener(new BlockedListener() {
	        public void handleBlocked(String reason) throws IOException {
	            System.out.println("************ PureRabbitReceiver.main handleBlocked()");
	        }

	        public void handleUnblocked() throws IOException {
	            System.out.println("************ PureRabbitReceiver.main(...) handleUnblocked()");
	        }
	    });
	    
	    Channel channel = connection.createChannel();

	    channel.queueDeclare(QUEUE_NAME, false, false, false, null);

	    Consumer consumer = new DefaultConsumer(channel) {
	      @Override
	      public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
	          throws IOException {
	        String message = new String(body, "UTF-8");
	        System.out.println("************ Received '" +message);
	      }
	    };

	    channel.basicConsume(QUEUE_NAME, true, consumer);

	  }

}
