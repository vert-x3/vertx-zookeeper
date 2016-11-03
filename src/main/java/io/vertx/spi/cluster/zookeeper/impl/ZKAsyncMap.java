/*
 *  Copyright (c) 2011-2016 The original author or authors
 *  ------------------------------------------------------
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  and Apache License v2.0 which accompanies this distribution.
 *
 *       The Eclipse Public License is available at
 *       http://www.eclipse.org/legal/epl-v10.html
 *
 *       The Apache License v2.0 is available at
 *       http://www.opensource.org/licenses/apache2.0.php
 *
 *  You may elect to redistribute this code under either of these licenses.
 */
package io.vertx.spi.cluster.zookeeper.impl;

import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.spi.cluster.ClusterManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.data.Stat;

import java.time.Instant;
import java.util.Optional;

/**
 * Created by Stream.Liu
 */
public class ZKAsyncMap<K, V> extends ZKMap<K, V> implements AsyncMap<K, V> {

  private final PathChildrenCache curatorCache;
  private final ClusterManager clusterManager;

  private static final Logger logger = LoggerFactory.getLogger(ZKAsyncMap.class);

  private static final String TTL_KEY_HANDLER_ADDRESS = "__VERTX_ZK_TTL_HANDLER_ADDRESS";
  private static final String TTL_KEY_LOCK = "__VERTX_ZK_TTL_LOCK";
  private static final String TTL_KEY_BODY_KEY_PATH = "keyPath";
  private static final String TTL_KEY_BODY_TIMEOUT = "timeout";
  private static final String TTL_KEY_IS_CANCEL = "isCancel";
  private static final long TTL_KEY_GET_LOCK_TIMEOUT = 1500;
  private final LocalMap<String, Long> ttlTimer;

  public ZKAsyncMap(Vertx vertx, CuratorFramework curator, ClusterManager clusterManager, String mapName) {
    super(curator, vertx, ZK_PATH_ASYNC_MAP, mapName);
    this.clusterManager = clusterManager;
    curatorCache = new PathChildrenCache(curator, mapPath, true);
    ttlTimer = vertx.sharedData().getLocalMap("__VERTX_ZK_TTL_TIMER");
    listenTTLKeyEvent();
    try {
      curatorCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  /**
   * As Zookeeper do not support set TTL value to zkNode, we have to handle it by application self.
   * 1. Publish a specific message to all consumers that listen ttl action.
   * 2. All Consumer could receive this message in same time, so we have to make distribution lock as delete action can only
   * be execute one time.
   * 3. Check key is exist, and delete if exist, note: we just make a timer to delete if timeout, and we also save timer if we want to cancel
   * this action in later.
   * 4. Release lock.
   * <p>
   * Still if a put without ttl happens after the put with ttl but before the timeout expires, the most recent update will be removed.
   * For this case, we should add a field to indicate that if we should stop timer in the cluster.
   */
  private void listenTTLKeyEvent() {
    vertx.eventBus().consumer(TTL_KEY_HANDLER_ADDRESS, (Handler<Message<JsonObject>>) event -> {
        JsonObject body = event.body();
        String keyPath = body.getString(TTL_KEY_BODY_KEY_PATH);
        if (body.getBoolean(TTL_KEY_IS_CANCEL, false)) {
          long timerID = ttlTimer.remove(body.getString(keyPath));
          if (timerID > 0) vertx.cancelTimer(timerID);
        } else {
          long timerID = vertx.setTimer(body.getLong(TTL_KEY_BODY_TIMEOUT), aLong -> {
              clusterManager.getLockWithTimeout(TTL_KEY_LOCK, TTL_KEY_GET_LOCK_TIMEOUT, lockAsyncResult -> {
                if (lockAsyncResult.succeeded()) {
                  checkExists(keyPath)
                    .compose(checkResult -> checkResult ? delete(keyPath, null) : Future.succeededFuture())
                    .setHandler(deleteResult -> {
                      if (deleteResult.succeeded()) {
                        lockAsyncResult.result().release();
                        logger.debug(String.format("The key %s have arrived time, and have been deleted.", keyPath));
                      } else {
                        logger.error(String.format("Delete expire key %s failed.", keyPath), deleteResult.cause());
                      }
                    });
                } else {
                  logger.error("get TTL lock failed.", lockAsyncResult.cause());
                }
              });
            }
          );
          ttlTimer.put(keyPath, timerID);
        }
      }
    );
  }

  @Override
  public void get(K k, Handler<AsyncResult<V>> asyncResultHandler) {
    assertKeyIsNotNull(k)
      .compose(aVoid -> checkExists(k))
      .compose(checkResult -> {
        Future<V> future = Future.future();
        if (checkResult) {
          ChildData childData = curatorCache.getCurrentData(keyPath(k));
          if (childData != null && childData.getData() != null) {
            try {
              V value = asObject(childData.getData());
              future.complete(value);
            } catch (Exception e) {
              future.fail(e);
            }
          } else {
            future.complete();
          }
        } else {
          //ignore
          future.complete();
        }
        return future;
      })
      .setHandler(asyncResultHandler);
  }

  @Override
  public void put(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    put(k, v, Optional.empty(), completionHandler);
  }

  @Override
  public void put(K k, V v, long timeout, Handler<AsyncResult<Void>> completionHandler) {
    put(k, v, Optional.of(timeout), completionHandler);
  }

  private void put(K k, V v, Optional<Long> timeoutOptional, Handler<AsyncResult<Void>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> checkExists(k))
      .compose(checkResult -> checkResult ? setData(k, v) : create(k, v))
      .compose(aVoid -> {
        JsonObject body = new JsonObject().put(TTL_KEY_BODY_KEY_PATH, keyPath(k));
        if (timeoutOptional.isPresent()) body.put(TTL_KEY_BODY_TIMEOUT, timeoutOptional.get());
        else body.put(TTL_KEY_IS_CANCEL, true);
        //publish a ttl message to all nodes.
        vertx.eventBus().publish(TTL_KEY_HANDLER_ADDRESS, body);

        Future<Void> future = Future.future();
        future.complete();
        return future;
      })
      .setHandler(completionHandler);
  }

  @Override
  public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {
    putIfAbsent(k, v, Optional.empty(), completionHandler);
  }

  @Override
  public void putIfAbsent(K k, V v, long timeout, Handler<AsyncResult<V>> completionHandler) {
    putIfAbsent(k, v, Optional.of(timeout), completionHandler);
  }

  private void putIfAbsent(K k, V v, Optional<Long> timeoutOptional, Handler<AsyncResult<V>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> {
        Future<V> innerFuture = Future.future();
        vertx.executeBlocking(future -> {
          long startTime = Instant.now().toEpochMilli();
          int retries = 0;

          for (; ; ) {
            try {
              Stat stat = new Stat();
              String path = keyPath(k);
              V currentValue = getData(stat, path);
              if (compareAndSet(startTime, retries++, stat, path, currentValue, v)) {
                future.complete(currentValue);
                return;
              }
            } catch (Exception e) {
              future.fail(e);
              return;
            }
          }
        }, false, innerFuture.completer());
        return innerFuture;
      })
      .compose(value -> {
        JsonObject body = new JsonObject().put(TTL_KEY_BODY_KEY_PATH, keyPath(k));
        if (timeoutOptional.isPresent()) body.put(TTL_KEY_BODY_TIMEOUT, timeoutOptional.get());
        else body.put(TTL_KEY_IS_CANCEL, true);
        //publish a ttl message to all nodes.
        vertx.eventBus().publish(TTL_KEY_HANDLER_ADDRESS, body);

        return Future.succeededFuture(value);
      })
      .setHandler(completionHandler);
  }

  @Override
  public void remove(K k, Handler<AsyncResult<V>> asyncResultHandler) {
    assertKeyIsNotNull(k).compose(aVoid -> {
      Future<V> future = Future.future();
      get(k, future.completer());
      return future;
    }).compose(value -> {
      Future<V> future = Future.future();
      if (value != null) {
        return delete(k, value);
      } else {
        future.complete();
      }
      return future;
    }).setHandler(asyncResultHandler);
  }

  @Override
  public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> resultHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> {
        Future<V> future = Future.future();
        get(k, future.completer());
        return future;
      })
      .compose(value -> {
        Future<Boolean> future = Future.future();
        if (value.equals(v)) {
          delete(k, v).setHandler(deleteResult -> {
            if (deleteResult.succeeded()) future.complete(true);
            else future.fail(deleteResult.cause());
          });
        } else {
          future.complete(false);
        }
        return future;
      }).setHandler(resultHandler);
  }

  @Override
  public void replace(K k, V v, Handler<AsyncResult<V>> asyncResultHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> {
        Future<V> innerFuture = Future.future();
        vertx.executeBlocking(future -> {
          long startTime = Instant.now().toEpochMilli();
          int retries = 0;

          for (; ; ) {
            try {
              Stat stat = new Stat();
              String path = keyPath(k);
              V currentValue = getData(stat, path);
              //do not replace value if previous value is null
              if (currentValue == null) {
                future.complete(null);
                return;
              }
              if (compareAndSet(startTime, retries++, stat, path, currentValue, v)) {
                future.complete(currentValue);
                return;
              }
            } catch (Exception e) {
              future.fail(e);
              return;
            }
          }
        }, false, innerFuture.completer());
        return innerFuture;
      })
      .setHandler(asyncResultHandler);
  }

  @Override
  public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> resultHandler) {
    assertKeyIsNotNull(k)
      .compose(aVoid -> assertValueIsNotNull(oldValue))
      .compose(aVoid -> assertValueIsNotNull(newValue))
      .compose(aVoid -> {
        Future<Boolean> innerFuture = Future.future();
        vertx.executeBlocking(future -> {
          long startTime = Instant.now().toEpochMilli();
          int retries = 0;

          for (; ; ) {
            try {
              Stat stat = new Stat();
              String path = keyPath(k);
              V currentValue = getData(stat, path);
              if (!currentValue.equals(oldValue)) {
                future.complete(false);
                return;
              }
              if (compareAndSet(startTime, retries++, stat, path, oldValue, newValue)) {
                future.complete(true);
                return;
              }
            } catch (Exception e) {
              future.fail(e);
              return;
            }
          }
        }, false, innerFuture.completer());
        return innerFuture;
      })
      .setHandler(resultHandler);
  }

  @Override
  public void clear(Handler<AsyncResult<Void>> resultHandler) {
    //just remove parent node
    delete(mapPath, null).setHandler(result -> {
      if (result.succeeded()) {
        resultHandler.handle(Future.succeededFuture());
      } else {
        resultHandler.handle(Future.failedFuture(result.cause()));
      }
    });
  }

  @Override
  public void size(Handler<AsyncResult<Integer>> resultHandler) {
    try {
      curator.getChildren().inBackground((client, event) ->
        vertx.runOnContext(aVoid -> resultHandler.handle(Future.succeededFuture(event.getChildren().size()))))
        .forPath(mapPath);
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }

}