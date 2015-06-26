package io.vertx.spi.cluster.impl.zookeeper;

import io.vertx.core.*;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Stream.Liu
 */
class ZKAsyncMultiMap<K, V> extends ZKMap<K, V> implements AsyncMultiMap<K, V> {
  private TreeCache curatorCache;

  ZKAsyncMultiMap(Vertx vertx, CuratorFramework curator, String mapName) {
    super(curator, vertx, ZK_PATH_ASYNC_MULTI_MAP, mapName);

    // /io.vertx/asyncMultiMap/subs
    curatorCache = new TreeCache(curator, mapPath);
    try {
      curatorCache.start();
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
      vertx.runOnContext(event -> {
        Map<String, ChildData> maps = curatorCache.getCurrentChildren(keyPath(k));
        ChoosableSet<V> choosableSet = new ChoosableSet<>(0);
        if (maps != null) {
          for (ChildData childData : maps.values()) {
            try {
              if (childData != null && childData.getData() != null && childData.getData().length > 0)
                choosableSet.add(asObject(childData.getData()));
            } catch (Exception ex) {
              asyncResultHandler.handle(Future.failedFuture(ex));
            }
          }
        }
        asyncResultHandler.handle(Future.succeededFuture(choosableSet));
      });
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
          Optional.ofNullable(curatorCache.getCurrentData(valuePath))
              .ifPresent(childData ->
                  delete(valuePath, null, deleteEvent -> forwardAsyncResult(completionHandler, deleteEvent, true)));
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
    Optional.ofNullable(curatorCache.getCurrentChildren(mapPath)).ifPresent(childDataMap -> {
      childDataMap.keySet().forEach(keyPath ->
          curatorCache.getCurrentChildren(mapPath + "/" + keyPath).keySet().forEach(valuePath ->
              Optional.ofNullable(curatorCache.getCurrentData(mapPath + "/" + keyPath + "/" + valuePath))
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

}
