package io.vertx.test.core;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;

import java.util.concurrent.CountDownLatch;
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

  protected void startNodes(int numNodes, VertxOptions options) {
    CountDownLatch latch = new CountDownLatch(numNodes);
    vertices = new Vertx[numNodes];
    for (int i = 0; i < numNodes; i++) {
      int index = i;
      clusteredVertx(options.setClusterHost("localhost").setClusterPort(0).setClustered(true)
        .setClusterManager(getClusterManager()), ar -> {
        try {
          if (ar.failed()) {
            ar.cause().printStackTrace();
          }
          assertTrue("Failed to start node", ar.succeeded());
          vertices[index] = ar.result();
        } finally {
          latch.countDown();
        }
      });
    }
    try {
      Thread.sleep(1500L);
    } catch (InterruptedException e) {
      //
    }
    try {
      assertTrue(latch.await(2, TimeUnit.MINUTES));
    } catch (InterruptedException e) {
      fail(e.getMessage());
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
