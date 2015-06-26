package io.vertx.spi.cluster.impl.zookeeper;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.impl.ClusterSerializable;
import org.apache.curator.RetryLoop;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.time.Instant;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Created by Stream.Liu
 */
abstract class ZKMap<K, V> {

  protected final CuratorFramework curator;
  protected final Vertx vertx;
  protected final String mapPath;
  protected final String mapName;
  protected final EnsurePath ensurePath;

  protected static final String ZK_PATH_ASYNC_MAP = "asyncMap";
  protected static final String ZK_PATH_ASYNC_MULTI_MAP = "asyncMultiMap";
  protected static final String EVENTBUS_PATH = "/" + ZK_PATH_ASYNC_MULTI_MAP + "/subs/";
  protected static final String ZK_PATH_SYNC_MAP = "syncMap";

  private RetryPolicy retryPolicy = new ExponentialBackoffRetry(100, 5);

  protected ZKMap(CuratorFramework curator, Vertx vertx, String mapType, String mapName) {
    this.curator = curator;
    this.vertx = vertx;
    this.mapName = mapName;
    this.mapPath = "/" + mapType + "/" + mapName;
    ensurePath = curator.newNamespaceAwareEnsurePath(mapPath);
  }

  protected String keyPath(K k) {
    Objects.requireNonNull(k, "key should not be null.");
    return mapPath + "/" + k.toString();
  }

  protected String valuePath(K k, Object v) {
    Objects.requireNonNull(v, "value should not be null.");
    return keyPath(k) + "/" + v.toString();
  }

  protected <T> boolean keyIsNull(Object key, Handler<AsyncResult<T>> handler) {
    boolean result = key == null;
    if (result) handler.handle(Future.failedFuture("key can not be null."));
    return result;
  }

  protected <T> boolean valueIsNull(Object value, Handler<AsyncResult<T>> handler) {
    boolean result = value == null;
    if (result) handler.handle(Future.failedFuture("value can not be null."));
    return result;
  }

  protected byte[] asByte(Object object) throws IOException {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(byteOut);
    if (object instanceof ClusterSerializable) {
      ClusterSerializable clusterSerializable = (ClusterSerializable) object;
      dataOutput.writeBoolean(true);
      dataOutput.writeUTF(object.getClass().getName());
      Buffer buffer = Buffer.buffer();
      clusterSerializable.writeToBuffer(buffer);
      byte[] bytes = buffer.getBytes();
      dataOutput.writeInt(bytes.length);
      dataOutput.write(bytes);
    } else {
      dataOutput.writeBoolean(false);
      ByteArrayOutputStream javaByteOut = new ByteArrayOutputStream();
      ObjectOutput objectOutput = new ObjectOutputStream(javaByteOut);
      objectOutput.writeObject(object);
      dataOutput.write(javaByteOut.toByteArray());
    }
    return byteOut.toByteArray();
  }

  protected <T> T asObject(byte[] bytes) throws Exception {
    ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
    DataInputStream in = new DataInputStream(byteIn);
    boolean isClusterSerializable = in.readBoolean();
    if (isClusterSerializable) {
      String className = in.readUTF();
      Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
      int length = in.readInt();
      byte[] body = new byte[length];
      in.readFully(body);
      try {
        ClusterSerializable clusterSerializable = (ClusterSerializable) clazz.newInstance();
        clusterSerializable.readFromBuffer(0, Buffer.buffer(body));
        return (T) clusterSerializable;
      } catch (Exception e) {
        throw new IllegalStateException("Failed to load class " + e.getMessage(), e);
      }
    } else {
      byte[] body = new byte[in.available()];
      in.readFully(body);
      ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(body));
      return (T) objectIn.readObject();
    }
  }

  protected <T, E> void forwardAsyncResult(Handler<AsyncResult<T>> completeHandler, AsyncResult<E> asyncResult) {
    if (asyncResult.succeeded()) {
      E result = asyncResult.result();
      if (result == null || result instanceof Void) {
        vertx.runOnContext(event -> completeHandler.handle(Future.succeededFuture()));
      } else {
        vertx.runOnContext(event -> completeHandler.handle(Future.succeededFuture((T) result)));
      }
    } else {
      vertx.runOnContext(aVoid -> completeHandler.handle(Future.failedFuture(asyncResult.cause())));
    }
  }

  protected <T, E> void forwardAsyncResult(Handler<AsyncResult<T>> completeHandler, AsyncResult<E> asyncResult, T result) {
    if (asyncResult.succeeded()) {
      vertx.runOnContext(event -> completeHandler.handle(Future.succeededFuture(result)));
    } else {
      vertx.runOnContext(aVoid -> completeHandler.handle(Future.failedFuture(asyncResult.cause())));
    }
  }

  /**
   * get data with Stat
   *
   * @param stat new Stat
   * @param path node path
   * @param <T>  result
   * @return T
   * @throws Exception
   */
  protected <T> T getData(Stat stat, String path) throws Exception {
    T result = null;
    try {
      ensurePath.ensure(curator.getZookeeperClient());
      result = asObject(curator.getData().storingStatIn(stat).forPath(path));
    } catch (KeeperException.NoNodeException e) {
      try {
        curator.create().creatingParentsIfNeeded().forPath(path, asByte(null));
      } catch (KeeperException.NodeExistsException ex) {
        // Do nothing useful.
      }
    }
    return result;
  }

  /**
   * CAS Operation.
   *
   * @param startTime start time for backOff
   * @param retries   retry times backOff
   * @param path      node path
   * @param expect    the value expect in path.
   * @param update    the value should be update to the path.
   * @return boolean
   * @throws Exception
   */
  protected boolean compareAndSet(long startTime, int retries, Stat stat, String path, V expect, V update) throws Exception {
    V currentValue = getData(stat, path);
    if (currentValue == expect || currentValue.equals(expect)) {
      try {
        curator.setData().withVersion(stat.getVersion()).forPath(path, asByte(update));
      } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
        // If the version has changed, block on the retry policy if necessary. If no more retries are remaining,
        // fail the operation.
        if (!retryPolicy.allowRetry(retries, Instant.EPOCH.toEpochMilli() - startTime, RetryLoop.getDefaultRetrySleeper())) {
          throw new VertxException("failed to acquire optimistic lock");
        }
      }
      return true;
    } else {
      return false;
    }
  }

  protected void checkExists(K k, AsyncResultHandler<Boolean> handler) {
    checkExists(keyPath(k), handler);
  }

  protected void checkExists(String path, AsyncResultHandler<Boolean> handler) {
    try {
      curator.checkExists().inBackground((client, event) -> {
        if (event.getType() == CuratorEventType.EXISTS) {
          if (event.getStat() == null) {
            vertx.runOnContext(aVoid -> handler.handle(Future.succeededFuture(false)));
          } else {
            vertx.runOnContext(aVoid -> handler.handle(Future.succeededFuture(true)));
          }
        }
      }).forPath(path);
    } catch (Exception ex) {
      vertx.runOnContext(aVoid -> handler.handle(Future.failedFuture(ex)));
    }
  }

  protected void create(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    create(keyPath(k), v, completionHandler);
  }

  protected void create(String path, V v, Handler<AsyncResult<Void>> completionHandler) {
    try {
      //there are two type of node - ephemeral and persistent.
      //if path is 'asyncMultiMap/subs/' which save the data of eventbus address and serverID we could using ephemeral,
      //since the lifecycle of this path as long as this verticle.
      CreateMode nodeMode = path.contains(EVENTBUS_PATH) ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;
      curator.create().creatingParentsIfNeeded().withMode(nodeMode).inBackground((cl, el) -> {
        if (el.getType() == CuratorEventType.CREATE) {
          vertx.runOnContext(event -> completionHandler.handle(Future.succeededFuture()));
        }
      }).forPath(path, asByte(v));
    } catch (Exception ex) {
      vertx.runOnContext(event -> completionHandler.handle(Future.failedFuture(ex)));
    }
  }

  protected void setData(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
    setData(keyPath(k), v, completionHandler);
  }

  protected void setData(String path, V v, Handler<AsyncResult<Void>> completionHandler) {
    try {
      curator.setData().inBackground((client, event) -> {
        if (event.getType() == CuratorEventType.SET_DATA) {
          vertx.runOnContext(e -> completionHandler.handle(Future.succeededFuture()));
        }
      }).forPath(path, asByte(v));
    } catch (Exception ex) {
      vertx.runOnContext(event -> completionHandler.handle(Future.failedFuture(ex)));
    }
  }

  protected void delete(K k, V v, Handler<AsyncResult<V>> asyncResultHandler) {
    delete(keyPath(k), v, asyncResultHandler);
  }

  protected void delete(String path, V v, Handler<AsyncResult<V>> asyncResultHandler) {
    try {
      curator.delete().deletingChildrenIfNeeded().inBackground((client, event) -> {
        if (event.getType() == CuratorEventType.DELETE) {
          curator.getChildren().inBackground((childClient, childEvent) -> {
            //clean parent node if doesn't have child node.
            if (childEvent.getChildren().size() == 0) {
              String[] paths = path.split("/");
              String parentNodePath = Stream.of(paths).limit(paths.length - 1).reduce((previous, current) -> previous + "/" + current).get();
              curator.delete().inBackground((deleteClient, deleteEvent) -> {
                if (deleteEvent.getType() == CuratorEventType.DELETE)
                  vertx.runOnContext(ea -> asyncResultHandler.handle(Future.succeededFuture(v)));
              }).forPath(parentNodePath);
            } else {
              vertx.runOnContext(ea -> asyncResultHandler.handle(Future.succeededFuture(v)));
            }
          }).forPath(path);
        }
      }).forPath(path);
    } catch (Exception ex) {
      vertx.runOnContext(aVoid -> asyncResultHandler.handle(Future.failedFuture(ex)));
    }
  }
}
