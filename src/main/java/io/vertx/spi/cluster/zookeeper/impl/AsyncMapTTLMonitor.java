package io.vertx.spi.cluster.zookeeper.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.spi.cluster.ClusterManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
 * <p>
 * Created by stream.
 */
public class AsyncMapTTLMonitor<K, V> {
  private final Vertx vertx;
  private final ClusterManager clusterManager;
  private final Map<String, ZKAsyncMap<K, V>> keyPathAndAsyncMap = new ConcurrentHashMap<>();

  static final String TTL_KEY_HANDLER_ADDRESS = "__VERTX_ZK_TTL_HANDLER_ADDRESS";
  static final String TTL_KEY_BODY_KEY_PATH = "keyPath";
  static final String TTL_KEY_BODY_TIMEOUT = "timeout";
  static final String TTL_KEY_IS_CANCEL = "isCancel";

  private static final String TTL_KEY_LOCK = "__VERTX_ZK_TTL_LOCK";
  private static final long TTL_KEY_GET_LOCK_TIMEOUT = 1500;
  private final LocalMap<String, Long> ttlTimer;
  private MessageConsumer<JsonObject> consumer;

  private volatile static AsyncMapTTLMonitor instance;

  private static final Logger logger = LoggerFactory.getLogger(AsyncMapTTLMonitor.class);

  @SuppressWarnings("unchecked")
  public static <K, V> AsyncMapTTLMonitor<K, V> getInstance(Vertx vertx, ClusterManager clusterManager) {
    if (instance == null) {
      synchronized (AsyncMapTTLMonitor.class) {
        if (instance == null) {
          instance = new AsyncMapTTLMonitor<K, V>(vertx, clusterManager);
        }
      }
    }
    return instance;
  }

  private AsyncMapTTLMonitor(Vertx vertx, ClusterManager clusterManager) {
    this.ttlTimer = vertx.sharedData().getLocalMap("__VERTX_ZK_TTL_TIMER");
    this.vertx = vertx;
    this.clusterManager = clusterManager;
    initConsumer();
  }

  private void initConsumer() {
    this.consumer = vertx.eventBus().consumer(TTL_KEY_HANDLER_ADDRESS, event -> {
        JsonObject body = event.body();
        String keyPath = body.getString(TTL_KEY_BODY_KEY_PATH);
        if (keyPathAndAsyncMap.get(keyPath) == null) return;
        if (body.getBoolean(TTL_KEY_IS_CANCEL, false)) {
          long timerID = ttlTimer.remove(body.getString(keyPath));
          if (timerID > 0) vertx.cancelTimer(timerID);
        } else {
          long timerID = vertx.setTimer(body.getLong(TTL_KEY_BODY_TIMEOUT), aLong -> {
              clusterManager.getLockWithTimeout(TTL_KEY_LOCK, TTL_KEY_GET_LOCK_TIMEOUT).onComplete(lockAsyncResult -> {
                ZKAsyncMap<K, V> zkAsyncMap = keyPathAndAsyncMap.get(keyPath);
                if (lockAsyncResult.succeeded()) {
                  zkAsyncMap.checkExists(keyPath)
                    .compose(checkResult -> checkResult ? zkAsyncMap.delete(keyPath, null) : Future.succeededFuture())
                    .onComplete(deleteResult -> {
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

  void addAsyncMapWithPath(String keyPath, ZKAsyncMap<K, V> asyncMap) {
    keyPathAndAsyncMap.putIfAbsent(keyPath, asyncMap);
  }

  public void stop() {
    consumer.unregister();
    keyPathAndAsyncMap.clear();
    instance = null;
  }

}
