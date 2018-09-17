/*
 * Copyright 2018 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.vertx.core.shareddata;

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

  public void after() throws Exception {
    super.after();
    zkClustered.stop();
  }

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
