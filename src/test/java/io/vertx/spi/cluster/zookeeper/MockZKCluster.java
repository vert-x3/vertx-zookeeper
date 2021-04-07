package io.vertx.spi.cluster.zookeeper;

import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Stream.Liu
 */
public class MockZKCluster {
  private final InstanceSpec spec = new InstanceSpec(null, -1, -1, -1, true, -1, 10000, 120);
  private TestingServer server;
  private final List<ZookeeperClusterManager> clusterManagers = new ArrayList<>();

  static {
    System.setProperty("zookeeper.extendedTypesEnabled", "true");
  }

  public MockZKCluster() {
    try {
      server = new TestingServer(spec, true);
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
        server = new TestingServer(spec, false);
      }
      server.restart();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public ClusterManager getClusterManager() {
    if (server == null) {
      try {
        server = new TestingServer(spec, true);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    ZookeeperClusterManager zookeeperClusterManager = new ZookeeperClusterManager(getDefaultConfig());
    clusterManagers.add(zookeeperClusterManager);
    return zookeeperClusterManager;
  }
}
