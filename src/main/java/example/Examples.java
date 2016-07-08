package example;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.apache.curator.framework.CuratorFramework;

import java.util.Properties;

/**
 * Created by stream.
 */
public class Examples {

  public void example1() {
    ClusterManager mgr = new ZookeeperClusterManager();
    VertxOptions options = new VertxOptions().setClusterManager(mgr);
    Vertx.clusteredVertx(options, res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }

  public void example2() {
    Properties zkConfig = new Properties();
    zkConfig.setProperty("hosts.zookeeper", "127.0.0.1");
    zkConfig.setProperty("path.root", "io.vertx");
    zkConfig.setProperty("retry.initialSleepTime", "1000");
    zkConfig.setProperty("retry.intervalTimes", "3");

    ClusterManager mgr = new ZookeeperClusterManager(zkConfig);
    VertxOptions options = new VertxOptions().setClusterManager(mgr);

    Vertx.clusteredVertx(options, res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }

  public void example3(CuratorFramework curator) {
    ClusterManager mgr = new ZookeeperClusterManager(curator);
    VertxOptions options = new VertxOptions().setClusterManager(mgr);
    Vertx.clusteredVertx(options, res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }
}
