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
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.test.core.VertxTestBase;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Operations must fail when ZooKeeper reports an error.
 */
public class CallbackErrorTest extends VertxTestBase {

  private TestingServer server;
  private ZookeeperClusterManager clusterManager;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    server = new TestingServer(true);
    JsonObject config = new MockZKCluster().getDefaultConfig()
      .put("zookeeperHosts", server.getConnectString())
      .put("connectTimeout", 1000)
      .put("retry", new JsonObject().put("initialSleepTime", 100).put("maxTimes", 1));
    clusterManager = new ZookeeperClusterManager(config);
    clusterManager.init(vertx);
    Promise<Void> promise = Promise.promise();
    clusterManager.join(promise);
    promise.future().await();
  }

  @Override
  protected void tearDown() throws Exception {
    Promise<Void> promise = Promise.promise();
    clusterManager.leave(promise);
    promise.future().await();
    super.tearDown();
    server.close();
  }

  @Test
  public void testAddRegistrationFailure() throws Exception {
    // ZooKeeper does not allow children of ephemeral nodes
    clusterManager.getCuratorFramework().create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/__vertx.subs/foo");
    Promise<Void> promise = Promise.promise();
    clusterManager.addRegistration("foo", new RegistrationInfo(clusterManager.getNodeId(), 0, false), promise);
    assertFailure(promise.future(), KeeperException.NoChildrenForEphemeralsException.class);
  }

  @Test
  public void testSetNodeInfoFailure() throws Exception {
    server.stop();
    Promise<Void> promise = Promise.promise();
    clusterManager.setNodeInfo(new NodeInfo("localhost", 1234, null), promise);
    assertFailure(promise.future(), KeeperException.class);
  }

  private void assertFailure(Future<Void> future, Class<? extends Throwable> expected) throws Exception {
    try {
      future.await(30, TimeUnit.SECONDS);
      fail("Expected a failure");
    } catch (java.util.concurrent.TimeoutException e) {
      fail("The operation never completed");
    } catch (Exception e) {
      assertTrue("Unexpected failure " + e, expected.isInstance(e));
    }
  }
}
