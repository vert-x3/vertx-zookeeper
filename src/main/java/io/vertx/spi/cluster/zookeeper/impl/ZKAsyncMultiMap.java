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
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type.*;

/**
 * Created by Stream.Liu
 */
public class ZKAsyncMultiMap<K, V> extends ZKMap<K, V> implements AsyncMultiMap<K, V> {

  private TreeCache treeCache;
  private CountDownLatch latch = new CountDownLatch(1);
  private ConcurrentMap<String, ChoosableSet<V>> cache = new ConcurrentHashMap<>();
  //we should have a snapshot cache which could make event bus information restore to the zk while node get reconnection event.
  //we come across this issue while internal network is unstable.
  private ConcurrentMap<String, ChoosableSet<V>> eventBusSnapshotCache = new ConcurrentHashMap<>();

  private static final Logger logger = LoggerFactory.getLogger(ZKAsyncMultiMap.class);

  public ZKAsyncMultiMap(Vertx vertx, CuratorFramework curator, String mapName) {
    super(curator, vertx, ZK_PATH_ASYNC_MULTI_MAP, mapName);
    treeCache = new TreeCache(curator, mapPath);
    treeCache.getListenable().addListener(new Listener());

    try {
      treeCache.start();
      latch.await(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    String path = valuePath(k, v);
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> checkExists(path))
      .compose(checkResult -> checkResult ? setData(path, v) : create(path, v, Optional.empty()))
      .compose(stat -> {
        //add to snapshot cache if path contains eventbus address
        if (path.contains(EVENTBUS_PATH)) {
          ChoosableSet<V> serverIDs = eventBusSnapshotCache.get(path);
          if (serverIDs == null) serverIDs = new ChoosableSet<>(1);
          serverIDs.add(v);
          eventBusSnapshotCache.put(path, serverIDs);
        }
        Promise<Void> future = Promise.promise();
        try {
          curator.sync().inBackground((syncClient, syncEvent) -> {
            if (syncEvent.getType() == CuratorEventType.SYNC) {
              curator.getData().inBackground((getClient, getEvent) -> {
                if (stat == null || stat.getMtime() <= getEvent.getStat().getMtime()) {
                  vertx.runOnContext(aVoid -> future.complete());
                } else {
                  vertx.runOnContext(aVoid -> future.fail("can not get correct zxid."));
                }
              }).forPath(path);
            }
          }).forPath(path);
        } catch (Exception ex) {
          vertx.runOnContext(aVoid -> future.fail(ex));
        }
        return future.future();
      })
      .onComplete(completionHandler);
  }

  @Override
  public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> asyncResultHandler) {
    Context ctx = vertx.getOrCreateContext();
    assertKeyIsNotNull(k)
      .compose(aVoid -> {
        final String keyPath = keyPath(k);
        ChoosableSet<V> entries = cache.get(keyPath);
        Promise<ChoosableIterable<V>> future = Promise.promise();
        if (entries != null && !entries.isEmpty()) {
          future.complete(entries);
        } else {
          //sync before get
          try {
            curator.sync().inBackground((clientSync, eventSync) -> {
              if (eventSync.getType() == CuratorEventType.SYNC) {
                Map<String, ChildData> maps = treeCache.getCurrentChildren(keyPath);
                ChoosableSet<V> newEntries = new ChoosableSet<>(maps != null ? maps.size() : 0);
                if (maps != null) {
                  for (ChildData childData : maps.values()) {
                    try {
                      if (childData != null && childData.getData() != null && childData.getData().length > 0) {
                        newEntries.add(asObject(childData.getData()));
                      }
                    } catch (Exception ex) {
                      ctx.runOnContext(v -> future.fail(ex));
                    }
                  }
                  cache.putIfAbsent(keyPath, newEntries);
                }
                ctx.runOnContext(v -> future.complete(newEntries));
              }
            }).forPath(keyPath);
          } catch (Exception ex) {
            ctx.runOnContext(v -> future.fail(ex));
          }
        }
        return future.future();
      })
      .onComplete(ar -> ctx.runOnContext(v -> asyncResultHandler.handle(ar)));
  }

  @Override
  public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
    assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> {
        String fullPath = valuePath(k, v);
        return remove(keyPath(k), v, fullPath);
      })
      .onComplete(completionHandler);
  }

  private Future<Boolean> remove(String keyPath, V v, String fullPath) {
    return checkExists(fullPath).compose(checkResult -> {
      Promise<Boolean> future = Promise.promise();
      if (checkResult) {
        Optional.ofNullable(treeCache.getCurrentData(fullPath))
          .ifPresent(childData -> delete(fullPath, null).onComplete(deleteResult -> {
            //delete snapshot cache if keyPath contains event bus address
            if (keyPath.contains(EVENTBUS_PATH)) {
              Optional.ofNullable(eventBusSnapshotCache.get(keyPath)).ifPresent(vs -> {
                vs.remove(v);
                eventBusSnapshotCache.put(keyPath, vs);
              });
            }
            future.complete(true);
          }));
      } else {
        future.complete(false);
      }
      return future.future();
    });
  }

  @Override
  public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
    removeAllMatching(value -> value.hashCode() == v.hashCode(), completionHandler);
  }

  @Override
  public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
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
                if (p.test(value)) {
                  futures.add(remove(keyPath, value, fullPath));
                }
              } catch (Exception e) {
                futures.add(Future.failedFuture(e));
              }
            });
        });
      });
      //
      CompositeFuture.all(futures).compose(compositeFuture -> {
        Promise<Void> future = Promise.promise();
        future.complete();
        return future.future();
      }).onComplete(completionHandler);
    });
  }

  private Future<Void> restoreSnapshotCache() {
    Promise<Void> futureResult = Promise.promise();
    List<Future> allFuture = eventBusSnapshotCache.entrySet().stream().map(entry -> {
      String path = entry.getKey().substring(mapPath.length() + 1).split("/", 2)[0];
      ChoosableSet<V> values = entry.getValue();
      List<Future> futures = values.getIds().stream().map(value -> {
        Promise<Void> future = Promise.promise();
        add((K) path, value, future);
        return future.future();
      }).collect(Collectors.toList());
      return futures;
    }).flatMap(Collection::stream).collect(Collectors.toList());

    CompositeFuture.all(allFuture).onComplete(event -> {
      if (event.failed()) {
        futureResult.fail(event.cause());
      } else {
        futureResult.complete();
      }
    });
    return futureResult.future();
  }

  private class Listener implements TreeCacheListener {
    private AtomicBoolean reconnected = new AtomicBoolean(false);

    private String cachePath(final String key) {
      return mapPath + "/" + key;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent treeCacheEvent) throws Exception {
      if (treeCacheEvent.getType() == INITIALIZED) {
        latch.countDown();
        return;
      }

      final ChildData childData = treeCacheEvent.getData();
      String[] key = null;
      ChoosableSet<V> entries = null;

      // We only care about events with childData: NODE_ADDED, NODE_REMOVED
      if (treeCacheEvent.getType() == NODE_ADDED || treeCacheEvent.getType() == NODE_REMOVED) {
        if (childData == null || mapPath.length() == childData.getPath().length()) {
          return;
        }
        // Strip off the map prefix and leave the multi-map key path: `<parent>/<child>`
        key = childData.getPath().substring(mapPath.length() + 1).split("/", 2);
        entries = cache.computeIfAbsent(cachePath(key[0]), k -> new ChoosableSet<>(1));
      }

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
            for (V entry : entries)
              if (entry.toString().equals(key[1])) entries.remove(entry);
          }
          //if reconnect status is true, we try to restore eventbus address information to the zookeeper cluster
          if (reconnected.get()) {
            reconnected.set(false);
            restoreSnapshotCache().onComplete(event -> {
              if (event.failed()) {
                logger.error("restore eventbus snapshot cache failed.", event.cause());
              } else {
                logger.info("restore eventbus snapshot cache success.");
              }
            });
          }
          break;
        case CONNECTION_SUSPENDED:
          logger.warn("connection to the zookeeper server have suspended.");
          break;
        case CONNECTION_RECONNECTED:
          reconnected.set(true);
          break;
        case CONNECTION_LOST:
          logger.error("connection to the zookeeper server have lost, all the temporary node will be remove.");
          break;
      }
    }
  }

}
