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

package io.vertx.core;

import io.vertx.core.json.JsonObject;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import io.vertx.test.core.AsyncTestBase;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by Stream.Liu
 */
public class ProgrammaticZKClusterManagerTest extends AsyncTestBase {

  private MockZKCluster zkCluster = new MockZKCluster();
  private RetryPolicy retryPolicy = new ExponentialBackoffRetry(2000, 5, 10000);

  private void testProgrammatic(ZookeeperClusterManager mgr, JsonObject config) throws Exception {
    mgr.setConfig(config);
    assertEquals(config, mgr.getConfig());
    VertxOptions options = new VertxOptions().setClusterManager(mgr);
    Vertx.clusteredVertx(options, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr.getCuratorFramework());
      res.result().close(res2 -> {
        assertTrue(res2.succeeded());
        testComplete();
      });
    });
    await();
  }

  @Test
  public void testProgrammaticSetConfig() throws Exception {
    JsonObject config = zkCluster.getDefaultConfig();
    ZookeeperClusterManager mgr = new ZookeeperClusterManager();
    mgr.setConfig(config);
    testProgrammatic(mgr, config);
  }

  @Test
  public void testProgrammaticSetWithConstructor() throws Exception {
    JsonObject config = zkCluster.getDefaultConfig();
    ZookeeperClusterManager mgr = new ZookeeperClusterManager(config);
    testProgrammatic(mgr, config);
  }

  @Test
  public void testCustomCuratorFramework() throws Exception {
    JsonObject config = zkCluster.getDefaultConfig();
    CuratorFramework curator = CuratorFrameworkFactory.builder()
      .connectString(config.getString("zookeeperHosts"))
      .namespace(config.getString("rootPath"))
      .retryPolicy(retryPolicy).build();
    curator.start();
    ZookeeperClusterManager mgr = new ZookeeperClusterManager(curator);
    testProgrammatic(mgr, config);
  }

  @Test
  public void testEventBusWhenUsingACustomCurator() throws Exception {
    JsonObject config = zkCluster.getDefaultConfig();
    CuratorFramework curator1 = CuratorFrameworkFactory.builder()
      .connectString(config.getString("zookeeperHosts"))
      .namespace(config.getString("rootPath"))
      .retryPolicy(retryPolicy).build();
    curator1.start();

    CuratorFramework curator2 = CuratorFrameworkFactory.builder()
      .connectString(config.getString("zookeeperHosts"))
      .namespace(config.getString("rootPath"))
      .retryPolicy(retryPolicy).build();
    curator2.start();

    ZookeeperClusterManager mgr1 = new ZookeeperClusterManager(curator1);
    ZookeeperClusterManager mgr2 = new ZookeeperClusterManager(curator2);
    VertxOptions options1 = new VertxOptions().setClusterManager(mgr1).setClusterHost("127.0.0.1");
    VertxOptions options2 = new VertxOptions().setClusterManager(mgr2).setClusterHost("127.0.0.1");

    AtomicReference<Vertx> vertx1 = new AtomicReference<>();
    AtomicReference<Vertx> vertx2 = new AtomicReference<>();

    Vertx.clusteredVertx(options1, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr1.getCuratorFramework());
      res.result().eventBus().consumer("news", message -> {
        assertNotNull(message);
        assertTrue(message.body().equals("hello"));
        testComplete();
      });
      vertx1.set(res.result());
    });

    assertWaitUntil(() -> vertx1.get() != null);

    Vertx.clusteredVertx(options2, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr2.getCuratorFramework());
      vertx2.set(res.result());
      res.result().eventBus().send("news", "hello");
    });

    await();

    vertx1.get().close(ar -> vertx1.set(null));
    vertx2.get().close(ar -> vertx2.set(null));

    assertTrue(curator1.getState() == CuratorFrameworkState.STARTED);
    assertTrue(curator2.getState() == CuratorFrameworkState.STARTED);

    assertWaitUntil(() -> vertx1.get() == null && vertx2.get() == null);

    curator1.close();
    curator2.close();

  }

  @Test
  public void testSharedDataUsingCustomCurator() throws Exception {
    JsonObject config = zkCluster.getDefaultConfig();
    CuratorFramework curator1 = CuratorFrameworkFactory.builder()
      .connectString(config.getString("zookeeperHosts"))
      .namespace(config.getString("rootPath"))
      .retryPolicy(retryPolicy).build();
    curator1.start();

    CuratorFramework curator2 = CuratorFrameworkFactory.builder()
      .connectString(config.getString("zookeeperHosts"))
      .namespace(config.getString("rootPath"))
      .retryPolicy(retryPolicy).build();
    curator2.start();

    ZookeeperClusterManager mgr1 = new ZookeeperClusterManager(curator1);
    ZookeeperClusterManager mgr2 = new ZookeeperClusterManager(curator2);
    VertxOptions options1 = new VertxOptions().setClusterManager(mgr1).setClusterHost("127.0.0.1");
    VertxOptions options2 = new VertxOptions().setClusterManager(mgr2).setClusterHost("127.0.0.1");

    AtomicReference<Vertx> vertx1 = new AtomicReference<>();
    AtomicReference<Vertx> vertx2 = new AtomicReference<>();

    Vertx.clusteredVertx(options1, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr1.getCuratorFramework());
      res.result().sharedData().getClusterWideMap("mymap1", ar -> {
        ar.result().put("news", "hello", v -> {
          vertx1.set(res.result());
        });
      });
    });

    assertWaitUntil(() -> vertx1.get() != null);

    Vertx.clusteredVertx(options2, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr2.getCuratorFramework());
      vertx2.set(res.result());
      res.result().sharedData().getClusterWideMap("mymap1", ar -> {
        ar.result().get("news", r -> {
          assertEquals("hello", r.result());
          testComplete();
        });
      });
    });

    await();

    vertx1.get().close(ar -> vertx1.set(null));
    vertx2.get().close(ar -> vertx2.set(null));

    assertWaitUntil(() -> vertx1.get() == null && vertx2.get() == null);

    // be sure stopping vertx did not cause or require our custom curator to close

    assertTrue(curator1.getState() == CuratorFrameworkState.STARTED);
    assertTrue(curator2.getState() == CuratorFrameworkState.STARTED);

    curator1.close();
    curator2.close();
  }

  @Test
  public void testThatExternalCuratorCanBeShutdown() {
    // This instance won't be used by vert.x
    JsonObject config = zkCluster.getDefaultConfig();
    CuratorFramework curator = CuratorFrameworkFactory.builder()
      .connectString(config.getString("zookeeperHosts"))
      .namespace(config.getString("rootPath"))
      .retryPolicy(retryPolicy).build();
    curator.start();
    String nodeID = UUID.randomUUID().toString();

    ZookeeperClusterManager mgr = new ZookeeperClusterManager(curator, nodeID);
    VertxOptions options = new VertxOptions().setClusterManager(mgr).setClusterHost("127.0.0.1");

    AtomicReference<Vertx> vertx1 = new AtomicReference<>();

    Vertx.clusteredVertx(options, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr.getCuratorFramework());
      res.result().sharedData().getClusterWideMap("mymap1", ar -> {
        ar.result().put("news", "hello", v -> {
          vertx1.set(res.result());
        });
      });
    });

    assertWaitUntil(() -> vertx1.get() != null);
    int size = mgr.getNodes().size();
    assertTrue(size > 0);
    assertTrue(mgr.getNodes().contains(nodeID));

    // Retrieve the value inserted by vert.x
    try {
      byte[] content = curator.getData().forPath("/asyncMap/mymap1/news");
      //There is header in bytes.
      String result = new String(Arrays.copyOfRange(content, 8, 13));
      assertEquals("hello", result);
    } catch (Exception e) {
      e.printStackTrace();
    }
    curator.close();

    assertWaitUntil(() -> mgr.getNodes().size() == size - 1);
    vertx1.get().close();
    vertx1.get().close(ar -> vertx1.set(null));

    assertWaitUntil(() -> vertx1.get() == null);
  }

  @Test
  public void testSharedDataUsingCustomCuratorFrameworks() throws Exception {
    JsonObject config = zkCluster.getDefaultConfig();
    CuratorFramework dataNode1 = CuratorFrameworkFactory.builder()
      .connectString(config.getString("zookeeperHosts"))
      .namespace(config.getString("rootPath"))
      .retryPolicy(retryPolicy).build();
    dataNode1.start();

    CuratorFramework dataNode2 = CuratorFrameworkFactory.builder()
      .connectString(config.getString("zookeeperHosts"))
      .namespace(config.getString("rootPath"))
      .retryPolicy(retryPolicy).build();
    dataNode2.start();

    CuratorFramework curator1 = CuratorFrameworkFactory.builder()
      .connectString(config.getString("zookeeperHosts"))
      .namespace(config.getString("rootPath"))
      .retryPolicy(retryPolicy).build();
    curator1.start();

    CuratorFramework curator2 = CuratorFrameworkFactory.builder()
      .connectString(config.getString("zookeeperHosts"))
      .namespace(config.getString("rootPath"))
      .retryPolicy(retryPolicy).build();
    curator2.start();

    ZookeeperClusterManager mgr1 = new ZookeeperClusterManager(curator1);
    ZookeeperClusterManager mgr2 = new ZookeeperClusterManager(curator2);
    VertxOptions options1 = new VertxOptions().setClusterManager(mgr1).setClusterHost("127.0.0.1");
    VertxOptions options2 = new VertxOptions().setClusterManager(mgr2).setClusterHost("127.0.0.1");

    AtomicReference<Vertx> vertx1 = new AtomicReference<>();
    AtomicReference<Vertx> vertx2 = new AtomicReference<>();

    Vertx.clusteredVertx(options1, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr1.getCuratorFramework());
      res.result().sharedData().getClusterWideMap("mymap1", ar -> {
        ar.result().put("news", "hello", v -> {
          vertx1.set(res.result());
        });
      });
    });

    assertWaitUntil(() -> vertx1.get() != null);

    Vertx.clusteredVertx(options2, res -> {
      assertTrue(res.succeeded());
      assertNotNull(mgr2.getCuratorFramework());
      vertx2.set(res.result());
      res.result().sharedData().getClusterWideMap("mymap1", ar -> {
        ar.result().get("news", r -> {
          assertEquals("hello", r.result());
          testComplete();
        });
      });
    });

    await();

    vertx1.get().close(ar -> vertx1.set(null));
    vertx2.get().close(ar -> vertx2.set(null));

    assertWaitUntil(() -> vertx1.get() == null && vertx2.get() == null);

    // be sure stopping vertx did not cause or require our custom hazelcast to shutdown

    assertTrue(curator1.getState() == CuratorFrameworkState.STARTED);
    assertTrue(curator2.getState() == CuratorFrameworkState.STARTED);

    curator1.close();
    curator2.close();

    assertTrue(dataNode1.getState() == CuratorFrameworkState.STARTED);
    assertTrue(dataNode2.getState() == CuratorFrameworkState.STARTED);

    dataNode1.close();
    dataNode2.close();
  }

}
