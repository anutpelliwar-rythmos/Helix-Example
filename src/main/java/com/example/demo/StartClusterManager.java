package com.example.demo;

import java.util.List;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import lombok.NonNull;

@Order(2)
@Component("clusterManager")
public class StartClusterManager implements CommandLineRunner {

  private @NonNull Properties properties;

  private static Logger LOG = LoggerFactory.getLogger(StartClusterManager.class);

  public void startConsumerCluster() {

    HelixManager manager = null;
    ZkClient zkclient = null;

    final String clusterName = properties.getHelixClusterName();
    final String zkAddr = properties.getZookeeperAddress();

    String hostName = System.getenv("HOSTNAME");
    final String controllerName = "controller." + hostName;
    LOG.info("Starting theClusterManager {} for cluster {} with Partitions {}", controllerName,
        clusterName, properties.getPartitionCount());

    try {
      // add node to cluster if not already added
      zkclient = new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
          ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);

      List<String> nodes = admin.getInstancesInCluster(clusterName);
      LOG.info(" Cluster Manager ::  Existing Instances in the cluster {} are {}", clusterName,
          nodes.toString());

      if (!nodes.contains(controllerName)) {
        InstanceConfig config = new InstanceConfig(controllerName);
        config.setInstanceEnabled(true);
        LOG.info("Adding Contoller Instance {} to the cluster {} ", controllerName, clusterName);
        admin.addInstance(clusterName, config);
      }

      manager = HelixManagerFactory.getZKHelixManager(clusterName, controllerName,
          InstanceType.CONTROLLER, zkAddr);

      manager.connect();

    } catch (Exception e) {

      LOG.error("HelixController {} failed to connect to cluster {} with Partitions {}",
          controllerName, clusterName, properties.getPartitionCount(), e);

      if (manager != null) {
        manager.disconnect();
      }
      if (zkclient != null) {
        zkclient.close();
      }
    }
  }


  @Override
  public void run(String... args) throws Exception {
    startConsumerCluster();
  }

}
