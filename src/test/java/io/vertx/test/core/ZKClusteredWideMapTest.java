package io.vertx.test.core;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;

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

  @Override
  @Test
  public void testMapPutTtl() {
    getVertx().sharedData().getClusterWideMap("unsupported", onSuccess(map -> {
      map.put("k", "v", 13, onFailure(cause -> {
        assertThat(cause, instanceOf(UnsupportedOperationException.class));
        testComplete();
      }));
    }));
    await();
  }

  @Override
  @Test
  public void testMapPutIfAbsentTtl() {
    getVertx().sharedData().getClusterWideMap("unsupported", onSuccess(map -> {
      map.putIfAbsent("k", "v", 13, onFailure(cause -> {
        assertThat(cause, instanceOf(UnsupportedOperationException.class));
        testComplete();
      }));
    }));
    await();
  }
}
