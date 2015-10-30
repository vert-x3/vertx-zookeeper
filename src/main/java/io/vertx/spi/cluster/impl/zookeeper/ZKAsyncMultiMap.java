package io.vertx.spi.cluster.impl.zookeeper;

import io.vertx.core.*;
import io.vertx.core.net.impl.ServerID;
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
import java.util.stream.Stream;

/**
 * Created by Stream.Liu
 */
class ZKAsyncMultiMap<K, V> extends ZKMap<K, V> implements AsyncMultiMap<K, V> {

  private TreeCache treeCache;
  private ConcurrentMap<String, ChoosableSet<V>> cache = new ConcurrentHashMap<>();
  private final static String VERTX_SUBS = "__vertx.subs";

  ZKAsyncMultiMap(Vertx vertx, CuratorFramework curator, String mapName) {
    super(curator, vertx, ZK_PATH_ASYNC_MULTI_MAP, mapName);
    treeCache = new TreeCache(curator, mapPath);
    //we only make a listener for the path of __vertx.subs
    if (mapName.equals(VERTX_SUBS)) treeCache.getListenable().addListener(new Listener());
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
          ChoosableSet<V> newEntries = new ChoosableSet<>(0);
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


  private Map.Entry<String, V> getServerID(ChildData childData) {
    String[] paths = childData.getPath().split("/");
    String[] hostAndPort = paths[paths.length - 1].split(":");
    ServerID serverID = new ServerID(Integer.valueOf(hostAndPort[1]), hostAndPort[0]);
    Map<String, V> result = new HashMap<>(1);
    String keyPath = Stream.of(paths).limit(paths.length - 1).reduce((previous, current) -> previous + "/" + current).get();
    result.put(keyPath, (V) serverID);
    return result.entrySet().iterator().next();
  }

  private class Listener implements TreeCacheListener {
    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent treeCacheEvent) throws Exception {
      ChildData childData;
      switch (treeCacheEvent.getType()) {
        case NODE_ADDED:
          childData = treeCacheEvent.getData();
          if (childData != null && childData.getData() != null && childData.getData().length > 0) {
            Map.Entry<String, V> entry = getServerID(childData);
            ChoosableSet<V> entries = Optional.ofNullable(cache.get(entry.getKey())).orElse(new ChoosableSet<>(1));
            entries.add(entry.getValue());
            cache.put(entry.getKey(), entries);
          }
          break;
        case NODE_REMOVED:
          childData = treeCacheEvent.getData();
          if (childData != null && childData.getPath() != null) {
            Map.Entry<String, V> entry = getServerID(childData);
            ChoosableSet<V> entries = Optional.ofNullable(cache.get(entry.getKey())).orElse(new ChoosableSet<>(0));
            entries.remove(entry.getValue());
            cache.put(entry.getKey(), entries);
          }
          break;
      }
    }
  }

}
