package com.example.demo;

import java.util.List;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.OnlineOfflineSMD;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import lombok.NonNull;


@Order(1)
@Component
public class SetupConsumerCluster implements CommandLineRunner {

  private @NonNull Properties properties;

  public static int partition = 0;


  @SuppressWarnings("deprecation")
  public void setupConsumerCluster() {
    String zkAddr = properties.getZookeeperAddress();
    String clusterName = properties.getHelixClusterName();
    partition = properties.getPartitionCount();

    ZkClient zkclient = null;

    try {
      zkclient = new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
          ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);

      // add cluster
      admin.addCluster(clusterName, true);

      // add state model definition
      if (!admin.getStateModelDefs(clusterName).contains(properties.getStateModel())) {
        admin.addStateModelDef(clusterName, properties.getStateModel(), OnlineOfflineSMD.build());
      }
      // add resource "topic" which has 6 partitions
      String resourceName = properties.getResourceName();
      List<String> resources = admin.getResourcesInCluster(clusterName);
      if (!resources.contains(resourceName)) {
        admin.addResource(clusterName, resourceName, partition, properties.getStateModel(),
            RebalanceMode.FULL_AUTO.toString());
      }
      admin.rebalance(clusterName, resourceName, 1);

    } finally {
      if (zkclient != null) {
        zkclient.close();
      }
    }
  }


  @Override
  public void run(String... args) throws Exception {

    setupConsumerCluster();
  }

}
