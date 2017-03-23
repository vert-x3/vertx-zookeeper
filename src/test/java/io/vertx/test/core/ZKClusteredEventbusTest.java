package io.vertx.test.core;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 *
 */

public class ZKClusteredEventbusTest extends ClusteredEventBusTest {

  private MockZKCluster zkClustered;
  //zookeeper sync node data need some time, some test have to send message delay
  private final static long DELAY_TIME = 1000L;
  public ZKClusteredEventbusTest() {
    try {
      zkClustered = new MockZKCluster();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  protected <T> void testPublish(T val, Consumer<T> consumer) {
    int numNodes = 3;
    startNodes(numNodes);
    AtomicInteger count = new AtomicInteger();
    class MyHandler implements Handler<Message<T>> {
      @Override
      public void handle(Message<T> msg) {
        if (consumer == null) {
          assertEquals(val, msg.body());
        } else {
          consumer.accept(msg.body());
        }
        if (count.incrementAndGet() == numNodes - 1) {
          testComplete();
        }
      }
    }
    AtomicInteger registerCount = new AtomicInteger(0);
    class MyRegisterHandler implements Handler<AsyncResult<Void>> {
      @Override
      public void handle(AsyncResult<Void> ar) {
        assertTrue(ar.succeeded());
        if (registerCount.incrementAndGet() == 2) {
          //there are some delay
          vertx.setTimer(DELAY_TIME, event -> vertices[0].eventBus().publish(ADDRESS1, val));
        }
      }
    }
    MessageConsumer reg = vertices[2].eventBus().<T>consumer(ADDRESS1).handler(new MyHandler());
    reg.completionHandler(new MyRegisterHandler());
    reg = vertices[1].eventBus().<T>consumer(ADDRESS1).handler(new MyHandler());
    reg.completionHandler(new MyRegisterHandler());
    await();
  }

  @Override
  protected <T, R> void testReply(T val, R received, Consumer<R> consumer, DeliveryOptions options) {
    if (vertices == null) {
      startNodes(2);
    }
    String str = TestUtils.randomUnicodeString(1000);
    MessageConsumer<?> reg = vertices[1].eventBus().consumer(ADDRESS1).handler(msg -> {
      assertEquals(str, msg.body());
      if (options == null) {
        msg.reply(val);
      } else {
        msg.reply(val, options);
      }
    });
    reg.completionHandler(ar -> {
      assertTrue(ar.succeeded());
      vertx.setTimer(DELAY_TIME, event -> {
        vertices[0].eventBus().send(ADDRESS1, str, onSuccess((Message<R> reply) -> {
          if (consumer == null) {
            assertEquals(received, reply.body());
            if (options != null && options.getHeaders() != null) {
              assertNotNull(reply.headers());
              assertEquals(options.getHeaders().size(), reply.headers().size());
              for (Map.Entry<String, String> entry: options.getHeaders().entries()) {
                assertEquals(reply.headers().get(entry.getKey()), entry.getValue());
              }
            }
          } else {
            consumer.accept(reply.body());
          }
          testComplete();
        }));
      });
    });

    await();
  }

  @Override
  protected <T, R> void testSend(T val, R received, Consumer<T> consumer, DeliveryOptions options) {
    if (vertices == null) {
      startNodes(2);
    }

    MessageConsumer<T> reg = vertices[1].eventBus().<T>consumer(ADDRESS1).handler((Message<T> msg) -> {
      if (consumer == null) {
        assertEquals(received, msg.body());
        if (options != null) {
          assertNotNull(msg.headers());
          int numHeaders = options.getHeaders() != null ? options.getHeaders().size() : 0;
          assertEquals(numHeaders, msg.headers().size());
          if (numHeaders != 0) {
            for (Map.Entry<String, String> entry : options.getHeaders().entries()) {
              assertEquals(msg.headers().get(entry.getKey()), entry.getValue());
            }
          }
        }
      } else {
        consumer.accept(msg.body());
      }
      testComplete();
    });
    reg.completionHandler(ar -> {
      assertTrue(ar.succeeded());
      //send message delay.
      vertx.setTimer(DELAY_TIME, event -> {
        if (options == null) {
          vertices[0].eventBus().send(ADDRESS1, val);
        } else {
          vertices[0].eventBus().send(ADDRESS1, val, options);
        }
      });
    });
    await();
  }

  public void testClusteredPong() throws Exception {
    startNodes(2, new VertxOptions().setClusterPingInterval(500).setClusterPingReplyInterval(500));
    AtomicBoolean sending = new AtomicBoolean();
    MessageConsumer<String> consumer = vertices[0].eventBus().<String>consumer("foobar").handler(msg -> {
      if (!sending.get()) {
        sending.set(true);
        vertx.setTimer(4000, id -> {
          vertices[1].eventBus().send("foobar", "whatever2");
        });
      } else {
        testComplete();
      }
    });
    consumer.completionHandler(ar -> {
      assertTrue(ar.succeeded());
      vertx.setTimer(DELAY_TIME, event -> vertices[1].eventBus().send("foobar", "whatever"));
    });
    await();
  }

  public void testDefaultCodecReplyExceptionSubclass() throws Exception {
    startNodes(2);
    MyReplyException myReplyException = new MyReplyException(23, "my exception");
    MyReplyExceptionMessageCodec codec = new MyReplyExceptionMessageCodec();
    vertices[0].eventBus().registerDefaultCodec(MyReplyException.class, codec);
    vertices[1].eventBus().registerDefaultCodec(MyReplyException.class, codec);
    MessageConsumer<ReplyException> reg = vertices[0].eventBus().<ReplyException>consumer(ADDRESS1, msg -> {
      assertTrue(msg.body() instanceof MyReplyException);
      testComplete();
    });
    reg.completionHandler(ar -> vertx.setTimer(DELAY_TIME, event -> vertices[1].eventBus().send(ADDRESS1, myReplyException)));

    await();
  }

  public void testRegisterRemote1() {
    startNodes(2);
    String str = TestUtils.randomUnicodeString(100);
    vertices[0].eventBus().<String>consumer(ADDRESS1).handler((Message<String> msg) -> {
      assertEquals(str, msg.body());
      testComplete();
    }).completionHandler(ar -> {
      assertTrue(ar.succeeded());
      vertx.setTimer(DELAY_TIME, event -> vertices[1].eventBus().send(ADDRESS1, str));
    });
    await();
  }

  public void testRegisterRemote2() {
    startNodes(2);
    String str = TestUtils.randomUnicodeString(100);
    vertices[0].eventBus().consumer(ADDRESS1, (Message<String> msg) -> {
      assertEquals(str, msg.body());
      testComplete();
    }).completionHandler(ar -> {
      assertTrue(ar.succeeded());
      vertx.setTimer(DELAY_TIME, event -> vertices[1].eventBus().send(ADDRESS1, str));
    });
    await();
  }


  public void after() throws Exception {
    super.after();
    zkClustered.stop();
  }

  public void await(long delay, TimeUnit timeUnit) {
    //fail fast if test blocking
    super.await(10, TimeUnit.SECONDS);
  }

  @Override
  protected ClusterManager getClusterManager() {
    return zkClustered.getClusterManager();
  }

}
