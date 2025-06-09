package io.vertx.spi.cluster.zookeeper;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.test.core.VertxTestBase;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by stream.
 */
public class ConsumerRoundRobinTest extends VertxTestBase {

  private static final String MESSAGE_ADDRESS = "consumerAddress";

  protected ClusterManager getClusterManager() {
    MockZKCluster zkCluster = new MockZKCluster();
    return zkCluster.getClusterManager();
  }


  private CompletableFuture<Void> addConsumer(int index) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    vertices[0].eventBus().
        consumer(MESSAGE_ADDRESS, message -> message.reply(index)).
        completion().onComplete(event -> {
          if (event.succeeded()) {
            future.complete(null);
          } else {
            future.completeExceptionally(event.cause());
          }
        });
    return future;
  }

  @Override
  protected VertxOptions getOptions() {
    VertxOptions options = super.getOptions();
    //防止调试的时候出现`BlockedThreadChecker`日志信息
    long         blockedThreadCheckInterval = 60 * 60 * 1000L;
    options.setBlockedThreadCheckInterval(blockedThreadCheckInterval);
    
    return options;
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    startNodes(1);
    CountDownLatch latch = new CountDownLatch(1);
    addConsumer(0).thenCompose(aVoid -> addConsumer(1)).thenCompose(aVoid -> addConsumer(2)).
        whenComplete((aVoid, throwable) -> {
          if (throwable != null) {
            fail(throwable);
          } else {
            latch.countDown();
          }
        });
    awaitLatch(latch);
  }

  @Test
  public void roundRobin() {
    AtomicInteger counter = new AtomicInteger(0);
    Set<Integer> results = new HashSet<>();
    Vertx vertx = vertices[0];
    vertx.setPeriodic(500, aLong -> vertx.eventBus().request(MESSAGE_ADDRESS, "Hi").onComplete(message -> {
      if (message.failed()) {
        fail(message.cause());
      } else {
        Integer result = (Integer) message.result().body();
        results.add(result);
        if (counter.incrementAndGet() == 3) {
          assertEquals(results.size(), counter.get());
          testComplete();
        }
      }
    }));
    await();
  }

}
