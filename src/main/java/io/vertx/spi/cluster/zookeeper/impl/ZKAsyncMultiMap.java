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
import java.util.concurrent.CompletableFuture;
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
    if (!keyIsNull(k, completionHandler) && !valueIsNull(v, completionHandler)) {
      String path = valuePath(k, v);
      checkExists(path, existEvent -> {
        if (existEvent.succeeded()) {
          if (existEvent.result()) {
            setData(path, v, completionHandler);
          } else {
            create(path, v, completionHandler);
          }
        } else {
          vertx.runOnContext(event -> completionHandler.handle(Future.failedFuture(existEvent.cause())));
        }
      });
    }
  }

  @Override
  public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> asyncResultHandler) {
    if (!keyIsNull(k, asyncResultHandler)) {
      final String keyPath = keyPath(k);
      ChoosableSet<V> entries = cache.get(keyPath);
      if (entries != null && !entries.isEmpty()) {
        asyncResultHandler.handle(Future.succeededFuture(entries));
      } else {
        vertx.runOnContext(event -> {
          Map<String, ChildData> maps = treeCache.getCurrentChildren(keyPath);
          ChoosableSet<V> newEntries = new ChoosableSet<>(maps != null ? maps.size() : 0);
          if (maps != null) {
            for (ChildData childData : maps.values()) {
              try {
                if (childData != null && childData.getData() != null && childData.getData().length > 0) {
                  newEntries.add(asObject(childData.getData()));
                }
              } catch (Exception ex) {
                asyncResultHandler.handle(Future.failedFuture(ex));
              }
            }
            cache.putIfAbsent(keyPath, newEntries);
          }
          asyncResultHandler.handle(Future.succeededFuture(newEntries));
        });
      }
    }
  }

  @Override
  public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
    if (!keyIsNull(k, completionHandler) && !valueIsNull(v, completionHandler)) {
      String fullPath = valuePath(k, v);
      remove(keyPath(k), v, fullPath, completionHandler);
    }
  }

  private void remove(String keyPath, V v, String fullPath, Handler<AsyncResult<Boolean>> completionHandler) {
    checkExists(fullPath, existEvent -> {
      if (existEvent.succeeded()) {
        if (existEvent.result()) {
          Optional.ofNullable(treeCache.getCurrentData(fullPath))
            .ifPresent(childData -> delete(fullPath, null, deleteEvent -> {
              //delete cache
              Optional.ofNullable(cache.get(keyPath)).ifPresent(vs -> {
                vs.remove(v);
                cache.put(keyPath, vs);
              });
              forwardAsyncResult(completionHandler, deleteEvent, true);
            }));
        } else {
          vertx.runOnContext(event -> completionHandler.handle(Future.succeededFuture(false)));
        }
      } else {
        vertx.runOnContext(event -> completionHandler.handle(Future.failedFuture(existEvent.cause())));
      }
    });
  }

  @Override
  public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
    List<CompletableFuture> futures = new ArrayList<>();
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
                  CompletableFuture future = new CompletableFuture();
                  remove(keyPath, v, fullPath, removeEvent -> {
                    if (removeEvent.succeeded()) future.complete(null);
                    else future.completeExceptionally(removeEvent.cause());
                  });
                  futures.add(future);
                }
              } catch (Exception e) {
                vertx.runOnContext(aVoid -> completionHandler.handle(Future.failedFuture(e)));
              }
            });
        });
      });
      //
      CompletableFuture
        .allOf(futures.toArray(new CompletableFuture[futures.size()]))
        .whenComplete((result, throwable) -> {
          if (throwable != null) {
            vertx.runOnContext(aVoid -> completionHandler.handle(Future.failedFuture(throwable)));
          } else {
            vertx.runOnContext(aVoid -> completionHandler.handle(Future.succeededFuture()));
          }
        });
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