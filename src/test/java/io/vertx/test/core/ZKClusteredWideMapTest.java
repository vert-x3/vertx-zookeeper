package io.vertx.test.core;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;
import org.junit.Test;

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

  /**
   * As zookeeper cluster manager using distribution lock, so there is some cost
   * on checking kv in application layer.
   * I have increase delay time that checking value is exist in map.
   * 30ms is ok in my laptop.
   */
  @Override
  @Test
  public void testMapPutTtl() {
    getVertx().sharedData().<String, String>getClusterWideMap("foo", onSuccess(map -> {
      map.put("pipo", "molo", 150, onSuccess(vd -> {
        vertx.setTimer(180, l -> {
          getVertx().sharedData().<String, String>getClusterWideMap("foo", onSuccess(map2 -> {
            map2.get("pipo", onSuccess(res -> {
              assertNull(res);
              testComplete();
            }));
          }));
        });
      }));
    }));
    await();
  }

  @Override
  @Test
  public void testMapPutIfAbsentTtl() {
    getVertx().sharedData().<String, String>getClusterWideMap("foo", onSuccess(map -> {
      map.putIfAbsent("pipo", "molo", 150, onSuccess(vd -> {
        assertNull(vd);
        vertx.setTimer(200, l -> {
          getVertx().sharedData().<String, String>getClusterWideMap("foo", onSuccess(map2 -> {
            map2.get("pipo", onSuccess(res -> {
              assertNull(res);
              testComplete();
            }));
          }));
        });
      }));
    }));
    await();
  }

}
