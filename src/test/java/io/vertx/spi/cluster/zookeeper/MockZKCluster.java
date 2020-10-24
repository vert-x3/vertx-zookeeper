package io.vertx.spi.cluster.zookeeper;

import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Created by Stream.Liu
 */
public class MockZKCluster {

  private final RetryPolicy retryPolicy = new ExponentialBackoffRetry(2000, 1, 8000);
  private TestingServer server;
  private final List<ZookeeperClusterManager> clusterManagers = new ArrayList<>();

  static {
    System.setProperty("zookeeper.extendedTypesEnabled", "true");
  }

  public MockZKCluster() {
    try {
      server = new TestingServer(new InstanceSpec(null, -1, -1, -1, true, -1, 10000, 120), true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public JsonObject getDefaultConfig() {
    JsonObject config = new JsonObject();
    config.put("zookeeperHosts", server.getConnectString());
    config.put("sessionTimeout", 10000);
    config.put("connectTimeout", 5000);
    config.put("rootPath", "io.vertx");
    config.put("retry", new JsonObject()
      .put("initialSleepTime", 500)
      .put("maxTimes", 2));
    return config;
  }

  public CuratorFramework curator;

  public void stop() {
    try {
      clusterManagers.clear();
      if (server == null) {
        server = new TestingServer(new InstanceSpec(null, -1, -1, -1, true, -1, 10000, 120), false);
      }
      server.restart();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public ClusterManager getClusterManager() {
    if (server == null) {
      try {
        server = new TestingServer(new InstanceSpec(null, -1, -1, -1, true, -1, 10000, 120), true);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    String connectString = server.getConnectString();
    curator = CuratorFrameworkFactory.builder()
        .namespace("io.vertx")
        .sessionTimeoutMs(3000)
        .connectionTimeoutMs(2000)
        .connectString(connectString)
        .retryPolicy(retryPolicy).build();
      curator.start();
      try {
        curator.blockUntilConnected(2, TimeUnit.SECONDS);
      } catch (Exception e) {
          e.printStackTrace();
      }
      String uuid = UUID.randomUUID().toString();
      ZookeeperClusterManager zookeeperClusterManager = new ZookeeperClusterManager(curator, uuid);
      clusterManagers.add(zookeeperClusterManager);
      return zookeeperClusterManager;
  }
}
