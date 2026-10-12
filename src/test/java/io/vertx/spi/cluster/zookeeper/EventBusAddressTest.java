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
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

/**
 * Event bus addresses are not valid ZooKeeper node names in general.
 */
public class EventBusAddressTest extends VertxTestBase {

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
  public void testOverlappingAddresses() {
    startNodes(2);
    MessageConsumer<String> parent = vertices[0].eventBus().consumer("foo", msg -> msg.reply("foo"));
    MessageConsumer<String> child = vertices[0].eventBus().consumer("foo/bar", msg -> msg.reply("foo/bar"));
    Future.all(parent.completion(), child.completion())
      .compose(v -> vertices[1].eventBus().<String>request("foo", "ping"))
      .compose(reply -> {
        assertEquals("foo", reply.body());
        return vertices[1].eventBus().<String>request("foo/bar", "ping");
      })
      .onComplete(onSuccess(reply -> {
        assertEquals("foo/bar", reply.body());
        testComplete();
      }));
    await();
  }

  @Test
  public void testRegistrationUpdates() {
    testRegistrationUpdates("foo/bar");
  }

  @Test
  public void testRegistrationUpdatesWithSpecialCharacters() {
    testRegistrationUpdates("%2F..\u0001\uD83D\uDE00");
  }

  @Test
  public void testRegistrationUpdatesWithDotDot() {
    testRegistrationUpdates("..");
  }

  // Node 1 resolves the address while only node 0 has a consumer, it must then learn about the consumer added on node 2
  private void testRegistrationUpdates(String address) {
    startNodes(3);
    vertices[0].eventBus().<String>consumer(address, msg -> msg.reply("node0")).completion()
      .compose(v -> vertices[1].eventBus().<String>request(address, "ping"))
      .compose(reply -> {
        assertEquals("node0", reply.body());
        return vertices[2].eventBus().<String>consumer(address, msg -> msg.reply("node2")).completion();
      })
      .onComplete(onSuccess(v -> requestUntil(address, "node2")));
    await();
  }

  private void requestUntil(String address, String expected) {
    vertices[1].eventBus().<String>request(address, "ping").onComplete(onSuccess(reply -> {
      if (expected.equals(reply.body())) {
        testComplete();
      } else {
        vertices[1].setTimer(10, id -> requestUntil(address, expected));
      }
    }));
  }
}
