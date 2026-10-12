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
import io.vertx.core.VertxException;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

/**
 * The node info of a node must be visible to other nodes as soon as it has been set.
 */
public class NodeInfoConsistencyTest extends VertxTestBase {

  private final MockZKCluster zkCluster = new MockZKCluster();
  private ZookeeperClusterManager node0;
  private ZookeeperClusterManager node1;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    node0 = join();
    node1 = join();
  }

  @Override
  protected void tearDown() throws Exception {
    for (ZookeeperClusterManager node : new ZookeeperClusterManager[]{node0, node1}) {
      if (node != null) {
        Promise<Void> promise = Promise.promise();
        node.leave(promise);
        promise.future().await();
      }
    }
    super.tearDown();
    zkCluster.stop();
  }

  private ZookeeperClusterManager join() {
    ZookeeperClusterManager node = (ZookeeperClusterManager) zkCluster.getClusterManager();
    node.init(vertx);
    Promise<Void> promise = Promise.promise();
    node.join(promise);
    promise.future().await();
    return node;
  }

  @Test
  public void testNodeInfoVisibleOnceSet() {
    for (int port = 1000; port < 1020; port++) {
      NodeInfo nodeInfo = new NodeInfo("localhost", port, new JsonObject().put("port", port));
      Promise<Void> set = Promise.promise();
      node1.setNodeInfo(nodeInfo, set);
      set.future().await();
      assertEquals(nodeInfo, getNodeInfo(node0, node1.getNodeId()).await());
    }
  }

  @Test
  public void testNodeInfoNotSet() {
    assertWaitUntil(() -> node0.getNodes().contains(node1.getNodeId()));
    try {
      getNodeInfo(node0, node1.getNodeId()).await();
      fail("Expected a failure");
    } catch (VertxException expected) {
    }
  }

  private static Future<NodeInfo> getNodeInfo(ZookeeperClusterManager node, String nodeId) {
    Promise<NodeInfo> promise = Promise.promise();
    node.getNodeInfo(nodeId, promise);
    return promise.future();
  }
}
