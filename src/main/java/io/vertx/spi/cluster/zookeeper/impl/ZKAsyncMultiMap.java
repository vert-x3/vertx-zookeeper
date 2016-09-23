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
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Stream.Liu
 */
public class ZKAsyncMultiMap<K, V> extends ZKMap<K, V> implements AsyncMultiMap<K, V> {

  private TreeCache treeCache;
  private ConcurrentMap<String, ChoosableSet<V>> cache = new ConcurrentHashMap<>();

  public ZKAsyncMultiMap(Vertx vertx, CuratorFramework curator, String mapName) {
    super(curator, vertx, ZK_PATH_ASYNC_MULTI_MAP, mapName);
    treeCache = new TreeCache(curator, mapPath);
    treeCache.getListenable().addListener(new Listener());

    try {
      treeCache.start();
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    String path = valuePath(k, v);
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> checkExists(path))
      .compose(checkResult -> checkResult ? setData(path, v) : create(path, v))
      .setHandler(completionHandler);
  }

  @Override
  public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> asyncResultHandler) {
    assertKeyIsNotNull(k)
      .compose(aVoid -> {
        final String keyPath = keyPath(k);
        ChoosableSet<V> entries = cache.get(keyPath);
        Future<ChoosableIterable<V>> future = Future.future();
        if (entries != null && !entries.isEmpty()) {
          future.complete(entries);
        } else {
          Map<String, ChildData> maps = treeCache.getCurrentChildren(keyPath);
          ChoosableSet<V> newEntries = new ChoosableSet<>(maps != null ? maps.size() : 0);
          if (maps != null) {
            for (ChildData childData : maps.values()) {
              try {
                if (childData != null && childData.getData() != null && childData.getData().length > 0) {
                  newEntries.add(asObject(childData.getData()));
                }
              } catch (Exception ex) {
                future.fail(ex);
              }
            }
            cache.putIfAbsent(keyPath, newEntries);
          }
          future.complete(newEntries);
        }
        return future;
      })
      .setHandler(asyncResultHandler);
  }

  @Override
  public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> {
        String fullPath = valuePath(k, v);
        return remove(keyPath(k), v, fullPath);
      })
      .setHandler(completionHandler);
  }

  private Future<Boolean> remove(String keyPath, V v, String fullPath) {
    return checkExists(fullPath).compose(checkResult -> {
      Future<Boolean> future = Future.future();
      if (checkResult) {
        Optional.ofNullable(treeCache.getCurrentData(fullPath))
          .ifPresent(childData -> delete(fullPath, null).setHandler(deleteResult -> {
            //delete cache
            Optional.ofNullable(cache.get(keyPath)).ifPresent(vs -> {
              vs.remove(v);
              cache.put(keyPath, vs);
            });
            future.complete(true);
          }));
      } else {
        future.complete(false);
      }
      return future;
    });
  }

  @Override
  public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
    List<Future> futures = new ArrayList<>();
    Optional.ofNullable(treeCache.getCurrentChildren(mapPath)).ifPresent(childDataMap -> {
      childDataMap.keySet().forEach(partKeyPath -> {
        String keyPath = mapPath + "/" + partKeyPath;
        treeCache.getCurrentChildren(keyPath).keySet().forEach(valuePath -> {
          String fullPath = keyPath + "/" + valuePath;
          Optional.ofNullable(treeCache.getCurrentData(fullPath))
            .filter(childData -> Optional.of(childData.getData()).isPresent())
            .ifPresent(childData -> {
              try {
                V value = asObject(childData.getData());
                if (v.hashCode() == value.hashCode()) {
                  futures.add(remove(keyPath, v, fullPath));
                }
              } catch (Exception e) {
                futures.add(Future.failedFuture(e));
              }
            });
        });
      });
      //
      CompositeFuture.all(futures).compose(compositeFuture -> {
        Future<Void> future = Future.future();
        future.complete();
        return future;
      }).setHandler(completionHandler);
    });
  }

  private class Listener implements TreeCacheListener {
    private String cachePath(final String key) {
      return mapPath + "/" + key;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent treeCacheEvent) throws Exception {
      final ChildData childData = treeCacheEvent.getData();
      // We only care about events with childData: NODE_ADDED, NODE_REMOVED
      if (childData == null || mapPath.length() == childData.getPath().length()) {
        return;
      }
      // Strip off the map prefix and leave the multi-map key path: `<parent>/<child>`
      final String key[] = childData.getPath().substring(mapPath.length() + 1).split("/", 2);
      final ChoosableSet<V> entries = cache.computeIfAbsent(cachePath(key[0]), k -> new ChoosableSet<>(1));

      // When we only have 1 item in the key[], we're operating on the entire key (e.g. removing it)
      // rather than a child element under the key
      switch (treeCacheEvent.getType()) {
        case NODE_ADDED:
          if (key.length > 1) {
            entries.add(asObject(childData.getData()));
          }
          break;
        case NODE_REMOVED:
          if (key.length == 1) {
            cache.remove(cachePath(key[0]));
          } else {
            // When the child items are serialized into ZK, we use `toString()` to build the path
            // element. When removing, search for the item that has the expected string representation.
            entries.forEach(entry -> {
              if (entry.toString().equals(key[1])) entries.remove(entry);
            });
          }
          break;
      }
    }
  }

}