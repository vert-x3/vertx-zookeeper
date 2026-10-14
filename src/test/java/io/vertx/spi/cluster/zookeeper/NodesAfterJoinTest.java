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

import io.vertx.core.Promise;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Once joined, the nodes of the cluster must include the node, the HA manager relies on it.
 */
public class NodesAfterJoinTest extends VertxTestBase {

  private static final int JOINS = 10;

  private final MockZKCluster zkCluster = new MockZKCluster();
  private final List<ZookeeperClusterManager> joined = new ArrayList<>();

  @Override
  protected void tearDown() throws Exception {
    for (ZookeeperClusterManager clusterManager : joined) {
      Promise<Void> promise = Promise.promise();
      clusterManager.leave(promise);
      promise.future().await();
    }
    super.tearDown();
    zkCluster.stop();
  }

  @Test
  public void testNodesIncludeSelfOnceJoined() {
    for (int i = 0; i < JOINS; i++) {
      ZookeeperClusterManager clusterManager = new ZookeeperClusterManager(zkCluster.getDefaultConfig());
      clusterManager.init(vertx);
      Promise<Void> promise = Promise.promise();
      clusterManager.join(promise);
      promise.future().await();
      joined.add(clusterManager);
      List<String> nodes = clusterManager.getNodes();
      assertTrue("Join " + i + ": " + clusterManager.getNodeId() + " not in " + nodes, nodes.contains(clusterManager.getNodeId()));
    }
  }
}
