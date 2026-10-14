/*
 *  Copyright (c) 2011-2026 The original author or authors
 *  ------------------------------------------------------
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  and Apache License v2.0 which accompanies this distribution.
 *
 *       The Eclipse Public License is available at
 *       http://www.eclipse.org/legal/epl-v10.html
 *
 *       The Apache License v2.0 is available at
 *       http://www.opensource.org/licenses/apache2.0.php
 *
 *  You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.spi.cluster.zookeeper;

import io.vertx.core.Future;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Conditional operations of the async map must be atomic across nodes.
 */
public class AsyncMapAtomicityTest extends VertxTestBase {

  private static final int NODES = 3;
  private static final int CALLS_PER_NODE = 4;
  private static final int KEYS = 10;

  private final MockZKCluster zkCluster = new MockZKCluster();
  private final List<AsyncMap<String, String>> maps = new ArrayList<>();

  @Override
  protected ClusterManager getClusterManager() {
    return zkCluster.getClusterManager();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    startNodes(NODES);
    for (int i = 0; i < NODES; i++) {
      maps.add(vertices[i].sharedData().<String, String>getAsyncMap("atomicity").await());
    }
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    zkCluster.stop();
  }

  @Test
  public void testPutIfAbsent() throws Exception {
    for (int k = 0; k < KEYS; k++) {
      String key = "put-" + k;
      List<String> values = new ArrayList<>();
      List<String> results = race((map, call) -> {
        String value = "value-" + call;
        values.add(value);
        return map.putIfAbsent(key, value);
      });
      List<String> winners = new ArrayList<>();
      for (int i = 0; i < results.size(); i++) {
        if (results.get(i) == null) {
          winners.add(values.get(i));
        }
      }
      assertEquals("Callers that put " + key, 1, winners.size());
      String winner = winners.get(0);
      for (String result : results) {
        assertTrue(result == null || result.equals(winner));
      }
      assertEquals(winner, maps.get(0).get(key).await(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testRemove() throws Exception {
    for (int k = 0; k < KEYS; k++) {
      String key = "remove-" + k;
      maps.get(0).put(key, "value").await(10, TimeUnit.SECONDS);
      List<String> results = race((map, call) -> map.remove(key));
      assertEquals("Callers that removed " + key, 1, results.stream().filter(Objects::nonNull).count());
      assertNull(maps.get(0).get(key).await(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testRemoveIfPresent() throws Exception {
    for (int k = 0; k < KEYS; k++) {
      String key = "remove-if-present-" + k;
      maps.get(0).put(key, "value").await(10, TimeUnit.SECONDS);
      List<Boolean> results = race((map, call) -> map.removeIfPresent(key, "value"));
      assertEquals("Callers that removed " + key, 1, results.stream().filter(b -> b).count());
      assertNull(maps.get(0).get(key).await(10, TimeUnit.SECONDS));
    }
  }

  // Run the operation concurrently from every node, the results are in call order
  private <T> List<T> race(BiFunction<AsyncMap<String, String>, Integer, Future<T>> op) throws Exception {
    List<Future<T>> futures = new ArrayList<>();
    int call = 0;
    for (int i = 0; i < CALLS_PER_NODE; i++) {
      for (AsyncMap<String, String> map : maps) {
        futures.add(op.apply(map, call++));
      }
    }
    Future.all(futures).await(30, TimeUnit.SECONDS);
    List<T> results = new ArrayList<>();
    for (Future<T> future : futures) {
      results.add(future.result());
    }
    return results;
  }
}
