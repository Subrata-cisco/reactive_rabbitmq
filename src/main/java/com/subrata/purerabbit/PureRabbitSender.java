package com.subrata.purerabbit;

import java.io.IOException;

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

public class PureRabbitSender {

	private final static String QUEUE_NAME = "rabbitmq-queue";
	private final static String BROKER_HOST = "localhost";
	private final static int BROKER_PORT = 5672;
	private final static String USERNAME = "guest";
	private final static String PASSWORD = "guest";
	
	private static boolean keepSending = true;

	public PureRabbitSender() {
	}

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(BROKER_HOST);
		factory.setPort(BROKER_PORT);
		factory.setUsername(USERNAME);
		factory.setPassword(PASSWORD);

		com.rabbitmq.client.Connection connection = factory.newConnection();
		connection.addBlockedListener(new BlockedListener() {
			public void handleBlocked(String reason) throws IOException {
				System.out.println("*************** Sender handleBlocked()  reason is :"+reason);
			}

			public void handleUnblocked() throws IOException {
				System.out.println("*************** Sender handle unnnnn Blocked()");
			}
		});
		Channel channel = connection.createChannel();

		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		String message = "from rabbit mq Hello World message !";
		while (keepSending) {
			channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
			//System.out.println("************* Sent '" + message);
		}

		channel.close();
		connection.close();
	}

}
