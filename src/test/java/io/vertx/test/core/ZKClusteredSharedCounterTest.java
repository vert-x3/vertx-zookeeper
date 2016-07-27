package io.vertx.test.core;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ZKClusteredSharedCounterTest extends ClusteredSharedCounterTest {

  private MockZKCluster zkClustered = new MockZKCluster();

  public void await(long delay, TimeUnit timeUnit) {
    super.await(15, TimeUnit.SECONDS);
  }

  public void after() throws Exception {
    super.after();
    zkClustered.stop();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return zkClustered.getClusterManager();
  }
}
