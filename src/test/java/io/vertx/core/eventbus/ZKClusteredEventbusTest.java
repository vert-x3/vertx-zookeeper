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

package io.vertx.core.eventbus;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;
import io.vertx.test.core.TestUtils;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 *
 */

public class ZKClusteredEventbusTest extends ClusteredEventBusTest {

  private final MockZKCluster zkClustered = new MockZKCluster();

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    zkClustered.stop();
  }

  public void await(long delay, TimeUnit timeUnit) {
    //fail fast if test blocking
    super.await(10, TimeUnit.SECONDS);
  }

  @Override
  protected <T, R> void testSend(T val, R received, Consumer<T> consumer, DeliveryOptions options) {
    if (vertices == null) {
      startNodes(2);
    }

    MessageConsumer<T> reg = vertices[1].eventBus().<T>consumer(ADDRESS1).handler((Message<T> msg) -> {
      if (consumer == null) {
        assertTrue(msg.isSend());
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
    reg.completion().onComplete(ar -> {
      assertTrue(ar.succeeded());
      vertices[1].setTimer(200L, along -> {
        if (options == null) {
          vertices[0].eventBus().send(ADDRESS1, val);
        } else {
          vertices[0].eventBus().send(ADDRESS1, val, options);
        }
      });
    });
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
    reg.completion().onComplete(ar -> {
      assertTrue(ar.succeeded());
      vertices[1].setTimer(200L, along -> {
        vertices[0].eventBus().<R>request(ADDRESS1, str).onComplete(onSuccess((Message<R> reply) -> {
          if (consumer == null) {
            assertTrue(reply.isSend());
            assertEquals(received, reply.body());
            if (options != null && options.getHeaders() != null) {
              assertNotNull(reply.headers());
              assertEquals(options.getHeaders().size(), reply.headers().size());
              for (Map.Entry<String, String> entry : options.getHeaders().entries()) {
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

  @Test
  public void testLocalHandlerClusteredPublish() throws Exception {
    startNodes(2);
    waitFor(2);
    vertices[1].eventBus().consumer(ADDRESS1, msg -> complete()).completion().onComplete(v1 -> {
      vertices[0].eventBus().localConsumer(ADDRESS1, msg -> complete()).completion().onComplete(v2 -> {
        vertices[1].setTimer(200L, aLong -> {
          vertices[0].eventBus().publish(ADDRESS1, "foo");
        });
      });
    });
    await();
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
          assertFalse(msg.isSend());
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
          vertices[0].setTimer(300L, h -> {
            vertices[0].eventBus().publish(ADDRESS1, val);
          });
        }
      }
    }
    MessageConsumer reg = vertices[2].eventBus().<T>consumer(ADDRESS1).handler(new MyHandler());
    reg.completion().onComplete(new MyRegisterHandler());
    reg = vertices[1].eventBus().<T>consumer(ADDRESS1).handler(new MyHandler());
    reg.completion().onComplete(new MyRegisterHandler());
    await();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return zkClustered.getClusterManager();
  }

}
