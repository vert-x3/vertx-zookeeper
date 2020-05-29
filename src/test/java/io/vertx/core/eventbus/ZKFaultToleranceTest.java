/*
 * Copyright (c) 2011-2020 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *      The Eclipse Public License is available at
 *      http://www.eclipse.org/legal/epl-v10.html
 *
 *      The Apache License v2.0 is available at
 *      http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.core.eventbus;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZKFaultToleranceTest extends FaultToleranceTest {

  private MockZKCluster zkClustered = new MockZKCluster();

  protected void startNodes(int numNodes, VertxOptions options) {
    CountDownLatch latch = new CountDownLatch(numNodes);
    vertices = new Vertx[numNodes];
    for (int i = 0; i < numNodes; i++) {
      int index = i;
      options.getEventBusOptions().setHost("localhost").setPort(0);
      clusteredVertx(options.setClusterManager(getClusterManager()), ar -> {
        try {
          if (ar.failed()) {
            ar.cause().printStackTrace();
          }
          assertTrue("Failed to start node", ar.succeeded());
          vertices[index] = ar.result();
        } finally {
          latch.countDown();
        }
      });
    }
    try {
      Thread.sleep(1500L);
    } catch (InterruptedException e) {
      //
    }
    try {
      assertTrue(latch.await(2, TimeUnit.MINUTES));
    } catch (InterruptedException e) {
      fail(e.getMessage());
    }
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    zkClustered.stop();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return zkClustered.getClusterManager();
  }
}
