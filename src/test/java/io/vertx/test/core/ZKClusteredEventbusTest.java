package io.vertx.test.core;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;

import java.util.concurrent.TimeUnit;

/**
 *
 */

public class ZKClusteredEventbusTest extends ClusteredEventBusTest {

  private MockZKCluster zkClustered;

  public ZKClusteredEventbusTest() {
    try {
      zkClustered = new MockZKCluster();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void after() throws Exception {
    super.after();
    zkClustered.stop();
  }

  public void await(long delay, TimeUnit timeUnit) {
    //fail fast if test blocking
    super.await(10, TimeUnit.SECONDS);
  }

  @Override
  protected ClusterManager getClusterManager() {
    return zkClustered.getClusterManager();
  }

}
