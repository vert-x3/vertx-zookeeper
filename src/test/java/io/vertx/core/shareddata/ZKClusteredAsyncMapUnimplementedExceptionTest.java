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

package io.vertx.core.shareddata;

import io.vertx.core.Vertx;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import io.vertx.spi.cluster.zookeeper.impl.ZKAsyncMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by huang.chengming
 */
public class ZKClusteredAsyncMapUnimplementedExceptionTest {

  private int doneFlag = 0;
  private Object doneResult = null;

  private void reset() {
    doneFlag = 0;
    doneResult = null;
  }

  @Test
  public void testZKUnimplementedException() throws Exception {

    TestingServer zkServer = mockZookeeperServer(false);
    CuratorFramework curator = mockCuratorFramework(zkServer.getConnectString());
    curator.start();

    Vertx vertx = Vertx.builder()
      .withClusterManager(new ZookeeperClusterManager(curator))
      .buildClustered()
      .toCompletionStage()
      .toCompletableFuture()
      .get();


    ZKAsyncMap<String, String> zkMap = new ZKAsyncMap<>(vertx, curator, "test-Map");

    // put value
    reset();
    zkMap.put("1", "1-value").onComplete(ar -> doneFlag = 1);
    await().atMost(5, SECONDS).until(() -> doneFlag == 1);


    // check value
    reset();
    zkMap.get("1")
      .onSuccess(ok -> doneResult = ok)
      .onFailure(err -> doneResult = err)
      .onComplete(ar -> doneFlag = 1);
    await().atMost(5, SECONDS).until(() -> doneFlag == 1);
    Assert.assertEquals("1-value", doneResult);


    // check TTL
    zkMap.put("2", "2-value", 1000)
      .onSuccess(ok -> doneResult = ok)
      .onFailure(err -> doneResult = err)
      .onComplete(ar -> doneFlag = 1);
    await().atMost(5, SECONDS).until(() -> doneFlag == 1);
    Assert.assertTrue(doneResult instanceof KeeperException);

    /*
     * When "extendedTypesEnabled" is not set, TTL nodes cannot be used.
     * TTL Nodes must be enabled via System property as they are disabled by default.
     * Create TTL Nodes without System property set the server will throw KeeperException.UnimplementedException.
     */
    Assert.assertTrue(doneResult instanceof KeeperException.UnimplementedException);

    curator.close();
    zkServer.close();
  }


  @Test
  public void testEnabledExtendedTypesFlag() throws Exception {

    TestingServer zkServer = mockZookeeperServer(true);
    CuratorFramework curator = mockCuratorFramework(zkServer.getConnectString());
    curator.start();

    Vertx vertx = Vertx.builder()
      .withClusterManager(new ZookeeperClusterManager(curator))
      .buildClustered()
      .toCompletionStage()
      .toCompletableFuture()
      .get();


    ZKAsyncMap<String, String> zkMap = new ZKAsyncMap<>(vertx, curator, "test2-Map");

    // check TTL
    reset();
    zkMap.put("3", "3-value", 1000)
      .onSuccess(ok -> doneResult = ok)
      .onFailure(err -> doneResult = err)
      .onComplete(ar -> doneFlag = 1);
    await().atMost(5, SECONDS).until(() -> doneFlag == 1);
    Assert.assertNull(doneResult);


    reset();
    zkMap.get("3")
      .onSuccess(ok -> doneResult = ok)
      .onFailure(err -> doneResult = err)
      .onComplete(ar -> doneFlag = 1);
    await().atMost(5, SECONDS).until(() -> doneFlag == 1);
    Assert.assertEquals("3-value", doneResult);

    curator.close();
    zkServer.close();

  }


  // --------------------

  private TestingServer mockZookeeperServer(boolean extendedTypesEnabled) throws Exception {
    Map<String, Object> prop = new HashMap<>();
    prop.put("extendedTypesEnabled", String.valueOf(extendedTypesEnabled));
    InstanceSpec spec = new InstanceSpec(null, -1, -1, -1, true, -1, 10000, 20, prop);
    return new TestingServer(spec, true);
  }

  private CuratorFramework mockCuratorFramework(String connectString) {
    return CuratorFrameworkFactory.builder()
      .connectString(connectString)
      .namespace("io.vertx")
      .sessionTimeoutMs(10 * 1000)
      .connectionTimeoutMs(10 * 1000)
      .retryPolicy(new RetryOneTime(1000))
      .build();
  }

}
