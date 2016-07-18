package io.vertx.spi.cluster.zookeeper.impl;

import io.vertx.core.*;
import io.vertx.core.shareddata.AsyncMap;
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

  public ZKAsyncMap(Vertx vertx, CuratorFramework curator, String mapName) {
    super(curator, vertx, ZK_PATH_ASYNC_MAP, mapName);
    curatorCache = new PathChildrenCache(curator, mapPath, true);
    try {
      curatorCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public void get(K k, Handler<AsyncResult<V>> asyncResultHandler) {
    if (!keyIsNull(k, asyncResultHandler)) {
      checkExists(k, checkResult -> {
        if (checkResult.succeeded()) {
          if (checkResult.result()) {
            ChildData childData = curatorCache.getCurrentData(keyPath(k));
            if (childData != null && childData.getData() != null) {
              try {
                V value = asObject(childData.getData());
                vertx.runOnContext(handler -> asyncResultHandler.handle(Future.succeededFuture(value)));
              } catch (Exception e) {
                vertx.runOnContext(handler -> asyncResultHandler.handle(Future.failedFuture(e)));
              }
            } else {
              vertx.runOnContext(handler -> asyncResultHandler.handle(Future.succeededFuture()));
            }
          } else {
            //ignore
            vertx.runOnContext(handler -> asyncResultHandler.handle(Future.succeededFuture()));
          }
        } else {
          vertx.runOnContext(handler -> asyncResultHandler.handle(Future.failedFuture(checkResult.cause())));
        }
      });
    }
  }

  @Override
  public void put(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    put(k, v, Optional.<TimeoutStream>empty(), completionHandler);
  }

  @Override
  public void put(K k, V v, long timeout, Handler<AsyncResult<Void>> completionHandler) {
    TimeoutStream timeoutStream = vertx.timerStream(timeout);
    timeoutStream.handler(aLong -> completionHandler.handle(Future.failedFuture("timeout on method put.")));
    put(k, v, Optional.of(timeoutStream), completionHandler);
  }

  private void put(K k, V v, Optional<TimeoutStream> timeoutStream, Handler<AsyncResult<Void>> completionHandler) {
    if (!keyIsNull(k, completionHandler) && !valueIsNull(v, completionHandler)) {
      checkExists(k, existEvent -> {
        if (existEvent.succeeded()) {
          if (existEvent.result()) {
            timeoutStream.ifPresent(TimeoutStream::cancel);
            setData(k, v, setDataEvent -> forwardAsyncResult(completionHandler, setDataEvent));
          } else {
            timeoutStream.ifPresent(TimeoutStream::cancel);
            create(k, v, completionHandler);
          }
        } else {
          vertx.runOnContext(aVoid -> {
            timeoutStream.ifPresent(TimeoutStream::cancel);
            completionHandler.handle(Future.failedFuture(existEvent.cause()));
          });
        }
      });
    }
  }

  @Override
  public void putIfAbsent(K k, V v, Handler<AsyncResult<V>> completionHandler) {
    putIfAbsent(k, v, Optional.<TimeoutStream>empty(), completionHandler);
  }

  @Override
  public void putIfAbsent(K k, V v, long timeout, Handler<AsyncResult<V>> completionHandler) {
    TimeoutStream timeoutStream = vertx.timerStream(timeout);
    timeoutStream.handler(aLong -> completionHandler.handle(Future.failedFuture("timeout on method putIfAbsent.")));
    putIfAbsent(k, v, Optional.of(timeoutStream), completionHandler);
  }

  private void putIfAbsent(K k, V v, Optional<TimeoutStream> timeoutStream, Handler<AsyncResult<V>> completionHandler) {
    if (!keyIsNull(k, completionHandler) && !valueIsNull(v, completionHandler)) {
      vertx.executeBlocking(future -> {
        long startTime = Instant.now().toEpochMilli();
        int retries = 0;

        for (; ; ) {
          try {
            Stat stat = new Stat();
            String path = keyPath(k);
            V currentValue = getData(stat, path);
            if (compareAndSet(startTime, retries++, stat, path, currentValue, v)) {
              timeoutStream.ifPresent(TimeoutStream::cancel);
              future.complete(currentValue);
              return;
            }
          } catch (Exception e) {
            timeoutStream.ifPresent(TimeoutStream::cancel);
            future.fail(e);
            return;
          }
        }
      }, completionHandler);
    }
  }

  @Override
  public void remove(K k, Handler<AsyncResult<V>> asyncResultHandler) {
    if (!keyIsNull(k, asyncResultHandler)) {
      get(k, getEvent -> {
        if (getEvent.succeeded()) {
          delete(k, getEvent.result(), asyncResultHandler);
        } else {
          vertx.runOnContext(event -> asyncResultHandler.handle(Future.failedFuture(getEvent.cause())));
        }
      });
    }
  }

  @Override
  public void removeIfPresent(K k, V v, Handler<AsyncResult<Boolean>> resultHandler) {
    if (!keyIsNull(k, resultHandler) && !valueIsNull(v, resultHandler)) {
      get(k, getEvent -> {
        if (getEvent.succeeded()) {
          if (v.equals(getEvent.result())) {
            delete(k, v, deleteEvent -> forwardAsyncResult(resultHandler, deleteEvent, true));
          } else {
            vertx.runOnContext(event -> resultHandler.handle(Future.succeededFuture(false)));
          }
        } else {
          vertx.runOnContext(event -> resultHandler.handle(Future.failedFuture(getEvent.cause())));
        }
      });
    }
  }

  @Override
  public void replace(K k, V v, Handler<AsyncResult<V>> asyncResultHandler) {
    if (!keyIsNull(k, asyncResultHandler) && !valueIsNull(v, asyncResultHandler)) {
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
      }, asyncResultHandler);
    }
  }

  @Override
  public void replaceIfPresent(K k, V oldValue, V newValue, Handler<AsyncResult<Boolean>> resultHandler) {
    if (!keyIsNull(k, resultHandler) && !valueIsNull(oldValue, resultHandler) && !valueIsNull(newValue, resultHandler)) {
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
      }, resultHandler);
    }
  }

  @Override
  public void clear(Handler<AsyncResult<Void>> resultHandler) {
    //just remove parent node
    delete(mapPath, null, deleteEvent -> forwardAsyncResult(resultHandler, deleteEvent, null));
  }

  @Override
  public void size(Handler<AsyncResult<Integer>> resultHandler) {
    try {
      curator.getChildren().inBackground((client, event) ->
        vertx.runOnContext(aVoid -> resultHandler.handle(Future.succeededFuture(event.getChildren().size()))))
        .forPath(mapPath);
    } catch (Exception e) {
      vertx.runOnContext(aVoid -> resultHandler.handle(Future.failedFuture(e)));
    }
  }

}