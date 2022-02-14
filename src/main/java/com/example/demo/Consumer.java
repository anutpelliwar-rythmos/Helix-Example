package com.example.demo;

import java.util.List;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import lombok.NonNull;

@Order(3)
@Component
public class Consumer implements CommandLineRunner {

  @Autowired
  private @NonNull Properties properties;
  private static Logger LOG = LoggerFactory.getLogger(Consumer.class);
  private String mqServer;
  private HelixManager manager = null;

  public void connect(String zkAddr, String clusterName, String consumerId) {
    try {
      manager = HelixManagerFactory.getZKHelixManager(clusterName, consumerId,
          InstanceType.PARTICIPANT, zkAddr);

      StateMachineEngine stateMach = manager.getStateMachineEngine();

      ConsumerStateModelFactory modelFactory = new ConsumerStateModelFactory(consumerId, mqServer);
      stateMach.registerStateModelFactory(properties.getStateModel(), modelFactory);

      manager.connect();

      Thread.currentThread().join();
    } catch (InterruptedException e) {
      System.err.println(" [-] " + consumerId + " is interrupted ...");
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      disconnect();
    }
  }

  public void disconnect() {
    if (manager != null) {
      manager.disconnect();
    }
  }

  private void startConsumer() {

    final String hostName = System.getenv("HOSTNAME");

    final String clusterName = properties.getHelixClusterName();
    final String zkAddr = properties.getZookeeperAddress();
    final String consumerId = "consumer." + hostName;

    ZkClient zkclient = null;
    LOG.info("Starting the HelixConsumer {} for cluster {} ", consumerId, clusterName);

    // add node to cluster if not already added
    zkclient = new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
        ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);

    List<String> nodes = admin.getInstancesInCluster(clusterName);
    LOG.info("Existing Instances in the cluster {} are {}", clusterName, nodes.toString());
    if (!nodes.contains(consumerId)) {
      InstanceConfig config = new InstanceConfig(consumerId);
      config.setHostName("localhost");
      config.setInstanceEnabled(true);
      admin.addInstance(clusterName, config);
    }
    connect(zkAddr, clusterName, consumerId);
  }

  @Override
  public void run(String... args) throws Exception {
    startConsumer();
  }
}
