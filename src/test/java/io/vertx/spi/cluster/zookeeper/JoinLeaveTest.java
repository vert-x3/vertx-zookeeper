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
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.test.core.VertxTestBase;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * A cluster manager can join again after it left the cluster or failed to join.
 */
public class JoinLeaveTest extends VertxTestBase {

  @Test
  public void testJoinAfterLeave() throws Exception {
    MockZKCluster zkCluster = new MockZKCluster();
    try {
      ZookeeperClusterManager clusterManager = new ZookeeperClusterManager(zkCluster.getDefaultConfig());
      clusterManager.init(vertx);
      join(clusterManager).await();
      putAndGet(clusterManager, "before").await(10, TimeUnit.SECONDS);
      leave(clusterManager).await();
      assertFalse(clusterManager.isActive());

      join(clusterManager).await();
      assertTrue(clusterManager.isActive());
      assertWaitUntil(() -> clusterManager.getNodes().contains(clusterManager.getNodeId()));
      putAndGet(clusterManager, "after").await(10, TimeUnit.SECONDS);
      leave(clusterManager).await();
    } finally {
      zkCluster.stop();
    }
  }

  @Test
  public void testJoinAfterFailedJoin() throws Exception {
    try (TestingServer server = new TestingServer(false)) {
      JsonObject config = new MockZKCluster().getDefaultConfig()
        .put("zookeeperHosts", server.getConnectString())
        .put("connectTimeout", 1000)
        .put("retry", new JsonObject().put("initialSleepTime", 100).put("maxTimes", 1));
      ZookeeperClusterManager clusterManager = new ZookeeperClusterManager(config);
      clusterManager.init(vertx);
      try {
        join(clusterManager).await();
        fail("Expected join to fail while ZooKeeper is down");
      } catch (Exception expected) {
      }
      assertFalse(clusterManager.isActive());

      server.start();
      join(clusterManager).await();
      assertTrue(clusterManager.isActive());
      assertWaitUntil(() -> clusterManager.getNodes().contains(clusterManager.getNodeId()));
      leave(clusterManager).await();
    }
  }

  private Future<Void> putAndGet(ZookeeperClusterManager clusterManager, String value) {
    Promise<AsyncMap<String, String>> promise = Promise.promise();
    clusterManager.getAsyncMap("map", promise);
    return promise.future()
      .compose(map -> map.put("key", value).compose(v -> map.get("key")))
      .map(result -> {
        assertEquals(value, result);
        return null;
      });
  }

  private static Future<Void> join(ZookeeperClusterManager clusterManager) {
    Promise<Void> promise = Promise.promise();
    clusterManager.join(promise);
    return promise.future();
  }

  private static Future<Void> leave(ZookeeperClusterManager clusterManager) {
    Promise<Void> promise = Promise.promise();
    clusterManager.leave(promise);
    return promise.future();
  }
}
