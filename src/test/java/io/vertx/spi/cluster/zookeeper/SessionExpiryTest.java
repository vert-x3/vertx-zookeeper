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

import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.test.core.VertxTestBase;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * A node must rejoin the cluster after its ZooKeeper session expired.
 */
public class SessionExpiryTest extends VertxTestBase {

  private final MockZKCluster zkCluster = new MockZKCluster();

  @Override
  protected ClusterManager getClusterManager() {
    return zkCluster.getClusterManager();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    zkCluster.stop();
  }

  @Test
  public void testRejoinAfterSessionExpiry() throws Exception {
    startNodes(2);
    vertices[1].eventBus().consumer("foo", msg -> msg.reply("pong")).completion().await();
    String nodeId = clusterManager(vertices[1]).getNodeId();

    expireSession(clusterManager(vertices[1]));

    assertWaitUntil(() -> clusterManager(vertices[0]).getNodes().contains(nodeId), 30_000);
    vertices[0].eventBus().request("foo", "ping").onComplete(onSuccess(reply -> {
      assertEquals("pong", reply.body());
      testComplete();
    }));
    await();
  }

  private static ZookeeperClusterManager clusterManager(Vertx vertx) {
    return (ZookeeperClusterManager) ((VertxInternal) vertx).clusterManager();
  }

  // Close the session on the server, like the server does when the session times out
  private void expireSession(ZookeeperClusterManager clusterManager) throws Exception {
    ZooKeeper zk = clusterManager.getCuratorFramework().getZookeeperClient().getZooKeeper();
    String connectString = zkCluster.getDefaultConfig().getString("zookeeperHosts");
    ZooKeeper other = new ZooKeeper(connectString, 10_000, event -> {}, zk.getSessionId(), zk.getSessionPasswd());
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (other.getState() != ZooKeeper.States.CONNECTED && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      assertEquals(ZooKeeper.States.CONNECTED, other.getState());
    } finally {
      other.close();
    }
  }
}
