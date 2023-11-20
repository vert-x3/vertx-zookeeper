package example;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.apache.curator.framework.CuratorFramework;

/**
 * Created by stream.
 */
public class Examples {

  public void example1() {
    ClusterManager mgr = new ZookeeperClusterManager();
    Vertx.builder()
      .withClusterManager(mgr)
      .buildClustered().onComplete(res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }

  public void example2() {
    JsonObject zkConfig = new JsonObject();
    zkConfig.put("zookeeperHosts", "127.0.0.1");
    zkConfig.put("rootPath", "io.vertx");
    zkConfig.put("retry", new JsonObject()
        .put("initialSleepTime", 3000)
        .put("maxTimes", 3));


    ClusterManager mgr = new ZookeeperClusterManager(zkConfig);

    Vertx.builder()
      .withClusterManager(mgr)
      .buildClustered().onComplete(res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }

  public void example3(CuratorFramework curator) {
    ClusterManager mgr = new ZookeeperClusterManager(curator);
    Vertx.builder().withClusterManager(mgr).buildClustered().onComplete(res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }
}
