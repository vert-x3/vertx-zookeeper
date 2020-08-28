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

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;

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
