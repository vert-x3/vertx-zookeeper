package io.vertx.spi.cluster.zookeeper;

import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Stream.Liu
 */
public class MockZKCluster {

  private RetryPolicy retryPolicy = new ExponentialBackoffRetry(2000, 1, 8000);
  private TestingServer server;
  private Set<ZookeeperClusterManager> clusterManagers = new HashSet<>();

  public MockZKCluster() {
    try {
      server = new TestingServer(new InstanceSpec(null, -1, -1, -1, true, -1, -1, 120), true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public JsonObject getDefaultConfig() {
    JsonObject config = new JsonObject();
    config.put("zookeeperHosts", server.getConnectString());
    config.put("rootPath", "io.vertx");
    config.put("retry", new JsonObject()
      .put("initialSleepTime", 3000)
      .put("maxTimes", 3));
    return config;
  }

  public void stop() {
    try {
      clusterManagers.forEach(clusterManager -> clusterManager.getCuratorFramework().close());
      clusterManagers.clear();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public ClusterManager getClusterManager() {
    CuratorFramework curator = CuratorFrameworkFactory.builder()
      .namespace("io.vertx")
      .sessionTimeoutMs(10000)
      .connectionTimeoutMs(5000)
      .connectString(server.getConnectString())
      .retryPolicy(retryPolicy).build();
    curator.start();
    ZookeeperClusterManager zookeeperClusterManager = new ZookeeperClusterManager(retryPolicy, curator);
    clusterManagers.add(zookeeperClusterManager);
    return zookeeperClusterManager;
  }
}
