package io.vertx.spi.cluster.impl.zookeeper;

import io.vertx.core.*;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Stream.Liu
 */
class ZKAsyncMultiMap<K, V> extends ZKMap<K, V> implements AsyncMultiMap<K, V> {
  private TreeCache treeCache;
  private ConcurrentMap<String, ChoosableSet<V>> cache = new ConcurrentHashMap<>();

  ZKAsyncMultiMap(Vertx vertx, CuratorFramework curator, String mapName) {
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
            create(valuePath(k, v), v, completionHandler);
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
      ChoosableSet<V> entries = cache.get(keyPath(k));
      if (entries != null && !entries.isEmpty()) {
        asyncResultHandler.handle(Future.succeededFuture(entries));
      } else {
        vertx.runOnContext(event -> {
          Map<String, ChildData> maps = treeCache.getCurrentChildren(keyPath(k));
          ChoosableSet<V> newEntries = new ChoosableSet<>(maps != null ? maps.size(): 0);
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
            cache.putIfAbsent(keyPath(k), newEntries);
          }
          asyncResultHandler.handle(Future.succeededFuture(newEntries));
        });
      }
    }
  }

  @Override
  public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
    if (!keyIsNull(k, completionHandler) && !valueIsNull(v, completionHandler)) {
      String valuePath = valuePath(k, v);
      remove(valuePath, completionHandler);
    }
  }

  private void remove(String valuePath, Handler<AsyncResult<Boolean>> completionHandler) {
    checkExists(valuePath, existEvent -> {
      if (existEvent.succeeded()) {
        if (existEvent.result()) {
          Optional.ofNullable(treeCache.getCurrentData(valuePath))
              .ifPresent(childData -> delete(valuePath, null, deleteEvent -> forwardAsyncResult(completionHandler, deleteEvent, true)));
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
    Collection<String> valuePaths = new ArrayList<>();
    Optional.ofNullable(treeCache.getCurrentChildren(mapPath)).ifPresent(childDataMap -> {
      childDataMap.keySet().forEach(keyPath ->
          treeCache.getCurrentChildren(mapPath + "/" + keyPath).keySet().forEach(valuePath ->
              Optional.ofNullable(treeCache.getCurrentData(mapPath + "/" + keyPath + "/" + valuePath))
                  .filter(childData -> Optional.of(childData.getData()).isPresent())
                  .ifPresent(childData -> {
                    try {
                      V value = asObject(childData.getData());
                      if (v.hashCode() == value.hashCode()) valuePaths.add(childData.getPath());
                    } catch (Exception e) {
                      vertx.runOnContext(aVoid -> completionHandler.handle(Future.failedFuture(e)));
                    }
                  })));
      //
      AtomicInteger size = new AtomicInteger(valuePaths.size());
      valuePaths.forEach(valuePath -> remove(valuePath, removeEvent -> {
        if (removeEvent.succeeded() && size.decrementAndGet() == 0) {
          vertx.runOnContext(aVoid -> completionHandler.handle(Future.succeededFuture()));
        } else {
          vertx.runOnContext(aVoid -> completionHandler.handle(Future.failedFuture(removeEvent.cause())));
        }
      }));
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
            for (final V entry: entries) {
              if (entry.toString().equals(key[1])) {
                entries.remove(entry);
              }
            }
          }
          break;
      }
    }
  }

}
