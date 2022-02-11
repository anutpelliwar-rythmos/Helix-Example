package com.example.demo;

import javax.validation.constraints.NotNull;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

@Order(4)
@Component
public class Emitter implements CommandLineRunner {

  private @NotNull Properties properties;

  private static final String EXCHANGE_NAME = "topic_logs";

  public static void send() throws Exception {

    final String mqServer = "mqServer";
    int count = Integer.MAX_VALUE;

    System.out.println("Sending " + count + " messages with random topic id");

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(mqServer);
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, "topic");

    for (int i = 0; i < count; i++) {
      int rand = ((int) (Math.random() * 10000) % SetupConsumerCluster.partition);
      String routingKey = "topic_" + rand;
      String message = "message_" + rand;

      channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
      System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");

      Thread.sleep(1000);
    }

    connection.close();
  }

  @Override
  public void run(String... args) throws Exception {
    send();

  }

}

