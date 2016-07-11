package io.vertx.test.core;

import io.vertx.core.net.impl.ServerID;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;

/**
 * Created by stream.
 */
public class ZKAsyncMultiMapTest extends AsyncMultiMapTest {

  private MockZKCluster zkClustered = new MockZKCluster();

  @Override
  protected ClusterManager getClusterManager() {
    return zkClustered.getClusterManager();
  }

  @Test
  public void shouldNotAddToMapCacheIfKeyDoesntAlreadyExist() throws Exception {
    String nonexistentKey = "non-existent-key." + UUID.randomUUID();

    map.get(nonexistentKey, ar -> {
      if (ar.succeeded()) {
        try {
          ChoosableIterable<ServerID> s = ar.result();
          Map<String, ChoosableIterable<ServerID>> cache = getCacheFromMap();

          // System.err.println("CACHE CONTENTS: " + cache);

          // check result
          assertNotNull(s);
          assertTrue(s.isEmpty());

          // check cache
          assertNotNull(cache);
          assertFalse(
            "Map cache should not contain key " + nonexistentKey,
            cache.containsKey(nonexistentKey));

        } catch (Exception e) {
          fail(e.toString());
        } finally {
          testComplete();
        }
      } else {
        fail(ar.cause().toString());
      }
    });

    await();
  }

  @SuppressWarnings("unchecked")
  private Map<String, ChoosableIterable<ServerID>> getCacheFromMap() throws Exception {
    Field field = map.getClass().getDeclaredField("cache");
    field.setAccessible(true);
    return (Map<String, ChoosableIterable<ServerID>>) field.get(map);
  }
}
