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
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.shareddata.AsyncMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

/**
 * Created by Stream.Liu
 */
public class ZKAsyncMap<K, V> extends ZKMap<K, V> implements AsyncMap<K, V> {

  private final PathChildrenCache curatorCache;

  public ZKAsyncMap(Vertx vertx, CuratorFramework curator, String mapName) {
    super(curator, vertx, ZK_PATH_ASYNC_MAP, mapName);
    this.curatorCache = new PathChildrenCache(curator, mapPath, true);
    try {
      curatorCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public Future<V> get(K k) {
    return assertKeyIsNotNull(k)
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
      });
  }

  @Override
  public Future<Void> put(K k, V v) {
    return put(k, v, Optional.empty());
  }

  @Override
  public Future<Void> put(K k, V v, long ttl) {
    return put(k, v, Optional.of(ttl));
  }

  private Future<Void> put(K k, V v, Optional<Long> timeoutOptional) {
    return assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> checkExists(k))
      .compose(checkResult -> checkResult ? setData(k, v) : create(k, v, timeoutOptional))
      .compose(stat -> Future.succeededFuture());
  }

  @Override
  public Future<V> putIfAbsent(K k, V v) {
    return putIfAbsent(k, v, Optional.empty());
  }

  @Override
  public Future<V> putIfAbsent(K k, V v, long ttl) {
    return putIfAbsent(k, v, Optional.of(ttl));
  }

  private Future<V> putIfAbsent(K k, V v, Optional<Long> timeoutOptional) {
    return assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> get(k))
      .compose(value -> {
        if (value == null) {
          if (timeoutOptional.isPresent()) {
            put(k, v, timeoutOptional);
          } else {
            put(k, v);
          }
        }
        return Future.succeededFuture(value);
      });
  }

  @Override
  public Future<V> remove(K k) {
    return assertKeyIsNotNull(k).compose(aVoid -> {
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
    });
  }

  @Override
  public Future<Boolean> removeIfPresent(K k, V v) {
    return assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> {
        Promise<V> promise = Promise.promise();
        get(k, promise);
        return promise.future();
      })
      .compose(value -> {
        Promise<Boolean> promise = Promise.promise();
        if (v.equals(value)) {
          delete(k, v).onComplete(deleteResult -> {
            if (deleteResult.succeeded()) promise.complete(true);
            else promise.fail(deleteResult.cause());
          });
        } else {
          promise.complete(false);
        }
        return promise.future();
      });
  }

  @Override
  public Future<V> replace(K k, V v) {
    return assertKeyAndValueAreNotNull(k, v)
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
      });
  }

  @Override
  public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue) {
    return assertKeyIsNotNull(k)
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
      });
  }

  @Override
  public Future<Void> clear() {
    //just remove parent node
    return delete(mapPath, null).mapEmpty();
  }

  @Override
  public Future<Integer> size() {
    Promise<Integer> promise = ((VertxInternal)vertx).getOrCreateContext().promise();
    try {
      curator.getChildren().inBackground((client, event) ->
        promise.tryComplete(event.getChildren().size()))
        .forPath(mapPath);
    } catch (Exception e) {
      promise.tryFail(e);
    }
    return promise.future();
  }

  @Override
  public Future<Set<K>> keys() {
    Promise<Set<K>> promise = ((VertxInternal)vertx).getOrCreateContext().promise();
    try {
      curator.getChildren().inBackground((client, event) -> {
        Set<K> keys = new HashSet<>();
        for (String base64Key : event.getChildren()) {
          byte[] binaryKey = Base64.getUrlDecoder().decode(base64Key);
          K key;
          try {
            key = asObject(binaryKey);
          } catch (Exception e) {
            promise.tryFail(e);
            return;
          }
          keys.add(key);
        }
        promise.tryComplete(keys);
      }).forPath(mapPath);
    } catch (Exception e) {
      promise.tryFail(e);
    }
    return promise.future();
  }

  @Override
  public Future<List<V>> values() {
    Promise<Set<K>> keysPromise = ((VertxInternal)vertx).getOrCreateContext().promise();
    keys(keysPromise);
    return keysPromise.future().compose(keys -> {
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
    });
  }

  @Override
  public Future<Map<K, V>> entries() {
    Promise<Set<K>> keysPromise = ((VertxInternal)vertx).getOrCreateContext().promise();
    keys(keysPromise);
    return keysPromise.future().map(ArrayList::new).compose(keys -> {
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
    });
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
