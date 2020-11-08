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

package io.vertx.ext.web.sstore;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class ZKClusteredSessionHandlerTest extends ClusteredSessionHandlerTest {

  private MockZKCluster zkClustered = new MockZKCluster();

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    zkClustered.stop();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return zkClustered.getClusterManager();
  }

  @Override
  @Test
  @Ignore("Not supported")
  public void testRetryTimeout() throws Exception {
    super.testRetryTimeout();
  }
}
