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

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.AsyncMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.vertx.spi.cluster.zookeeper.impl.AsyncMapTTLMonitor.*;

/**
 * Created by Stream.Liu
 */
public class ZKAsyncMap<K, V> extends ZKMap<K, V> implements AsyncMap<K, V> {

  private final PathChildrenCache curatorCache;
  private AsyncMapTTLMonitor<K, V> asyncMapTTLMonitor;

  public ZKAsyncMap(Vertx vertx, CuratorFramework curator, AsyncMapTTLMonitor<K,V> asyncMapTTLMonitor, String mapName) {
    super(curator, vertx, ZK_PATH_ASYNC_MAP, mapName);
    this.curatorCache = new PathChildrenCache(curator, mapPath, true);
    try {
      this.asyncMapTTLMonitor = asyncMapTTLMonitor;
      curatorCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public void get(K k, Handler<AsyncResult<V>> asyncResultHandler) {
    assertKeyIsNotNull(k)
      .compose(aVoid -> checkExists(k))
      .compose(checkResult -> {
        Promise<V> promise = Promise.promise();
        if (checkResult) {
          ChildData childData = curatorCache.getCurrentData(keyPath(k));
          if (childData != null && childData.getData() != null) {
            try {
              V value = asObject(childData.getData());
              promise.complete(value);
            } catch (Exception e) {
              promise.fail(e);
            }
          } else {
            promise.complete();
          }
        } else {
          //ignore
          promise.complete();
        }
        return promise.future();
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
        if (timeoutOptional.isPresent()) {
          asyncMapTTLMonitor.addAsyncMapWithPath(keyPath(k), this);
          body.put(TTL_KEY_BODY_TIMEOUT, timeoutOptional.get());
        } else body.put(TTL_KEY_IS_CANCEL, true);
        //publish a ttl message to all nodes.
        vertx.eventBus().publish(TTL_KEY_HANDLER_ADDRESS, body);

        return Future.<Void>succeededFuture();
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
        Promise<V> innerPromise = Promise.promise();
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
        }, false, innerPromise);
        return innerPromise.future();
      })
      .compose(value -> {
        JsonObject body = new JsonObject().put(TTL_KEY_BODY_KEY_PATH, keyPath(k));
        if (timeoutOptional.isPresent()) {
          asyncMapTTLMonitor.addAsyncMapWithPath(keyPath(k), this);
          body.put(TTL_KEY_BODY_TIMEOUT, timeoutOptional.get());
        } else body.put(TTL_KEY_IS_CANCEL, true);
        //publish a ttl message to all nodes.
        vertx.eventBus().publish(TTL_KEY_HANDLER_ADDRESS, body);
        return Future.succeededFuture(value);
      })
      .setHandler(completionHandler);
  }

  @Override
  public void remove(K k, Handler<AsyncResult<V>> asyncResultHandler) {
    assertKeyIsNotNull(k).compose(aVoid -> {
      Promise<V> promise = Promise.promise();
      get(k, promise);
      return promise.future();
    }).compose(value -> {
      Promise<V> promise = Promise.promise();
      if (value != null) {
        return delete(k, value);
      } else {
        promise.complete();
      }
      return promise.future();
    }).setHandler(asyncResultHandler);
  }

  @Override
  public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> resultHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> {
        Promise<V> promise = Promise.promise();
        get(k, promise);
        return promise.future();
      })
      .compose(value -> {
        Promise<Boolean> promise = Promise.promise();
        if (value.equals(v)) {
          delete(k, v).setHandler(deleteResult -> {
            if (deleteResult.succeeded()) promise.complete(true);
            else promise.fail(deleteResult.cause());
          });
        } else {
          promise.complete(false);
        }
        return promise.future();
      }).setHandler(resultHandler);
  }

  @Override
  public void replace(K k, V v, Handler<AsyncResult<V>> asyncResultHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> {
        Promise<V> innerPromise = Promise.promise();
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
        }, false, innerPromise);
        return innerPromise.future();
      })
      .setHandler(asyncResultHandler);
  }

  @Override
  public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> resultHandler) {
    assertKeyIsNotNull(k)
      .compose(aVoid -> assertValueIsNotNull(oldValue))
      .compose(aVoid -> assertValueIsNotNull(newValue))
      .compose(aVoid -> {
        Promise<Boolean> innerPromise = Promise.promise();
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
        }, false, innerPromise);
        return innerPromise.future();
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

  public void keys(Handler<AsyncResult<Set<K>>> resultHandler) {
    Context context = vertx.getOrCreateContext();
    try {
      curator.getChildren().inBackground((client, event) -> {
        Set<K> keys = new HashSet<>();
        for (String base64Key : event.getChildren()) {
          byte[] binaryKey = Base64.getUrlDecoder().decode(base64Key);
          K key;
          try {
            key = asObject(binaryKey);
          } catch (Exception e) {
            context.runOnContext(v -> resultHandler.handle(Future.failedFuture(e)));
            return;
          }
          keys.add(key);
        }
        context.runOnContext(v -> resultHandler.handle(Future.succeededFuture(keys)));
      }).forPath(mapPath);
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void values(Handler<AsyncResult<List<V>>> resultHandler) {
    Promise<Set<K>> keysPromise = Promise.promise();
    keys(keysPromise);
    keysPromise.future().compose(keys -> {
      List<Future> futures = new ArrayList<>(keys.size());
      for (K k : keys) {
        Promise valuePromise = Promise.promise();
        get(k, valuePromise);
        futures.add(valuePromise.future());
      }
      return CompositeFuture.all(futures).map(compositeFuture -> {
        List<V> values = new ArrayList<>(compositeFuture.size());
        for (int i = 0; i < compositeFuture.size(); i++) {
          values.add(compositeFuture.resultAt(i));
        }
        return values;
      });
    }).setHandler(resultHandler);
  }

  @Override
  public void entries(Handler<AsyncResult<Map<K, V>>> resultHandler) {
    Promise<Set<K>> keysPromise = Promise.promise();
    keys(keysPromise);
    keysPromise.future().map(ArrayList::new).compose(keys -> {
      List<Future> futures = new ArrayList<>(keys.size());
      for (K k : keys) {
        Promise valuePromise = Promise.promise();
        get(k, valuePromise);
        futures.add(valuePromise.future());
      }
      return CompositeFuture.all(futures).map(compositeFuture -> {
        Map<K, V> map = new HashMap<>();
        for (int i = 0; i < compositeFuture.size(); i++) {
          map.put(keys.get(i), compositeFuture.resultAt(i));
        }
        return map;
      });
    }).setHandler(resultHandler);
  }

  @Override
  String keyPath(K k) {
    try {
      return keyPathPrefix() + Base64.getUrlEncoder().encodeToString(asByte(k));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String keyPathPrefix() {
    return mapPath + "/";
  }
}
