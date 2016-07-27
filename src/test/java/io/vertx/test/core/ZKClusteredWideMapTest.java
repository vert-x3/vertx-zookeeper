package io.vertx.test.core;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;

/**
 *
 */
public class ZKClusteredWideMapTest extends ClusterWideMapTestDifferentNodes {

  private MockZKCluster zkClustered = new MockZKCluster();

  public void after() throws Exception {
    super.after();
    zkClustered.stop();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return zkClustered.getClusterManager();
  }


}
