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

package io.vertx.core.eventbus;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */

public class ZKClusteredEventbusTest extends ClusteredEventBusTest {

  private MockZKCluster zkClustered;

  public ZKClusteredEventbusTest() {
    try {
      zkClustered = new MockZKCluster();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected void startNodes(int numNodes, VertxOptions options) {
    CountDownLatch latch = new CountDownLatch(numNodes);
    vertices = new Vertx[numNodes];
    for (int i = 0; i < numNodes; i++) {
      int index = i;
      clusteredVertx(options.setClusterHost("localhost").setClusterPort(0)
        .setClusterManager(getClusterManager()), ar -> {
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

  public void after() throws Exception {
    super.after();
    zkClustered.stop();
  }

  public void await(long delay, TimeUnit timeUnit) {
    //fail fast if test blocking
    super.await(30, TimeUnit.SECONDS);
  }

  @Override
  protected ClusterManager getClusterManager() {
    return zkClustered.getClusterManager();
  }

}
