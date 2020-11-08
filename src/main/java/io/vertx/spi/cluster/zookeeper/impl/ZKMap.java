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

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.impl.ClusterSerializable;
import org.apache.curator.RetryLoop;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.lang.reflect.Constructor;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Created by Stream.Liu
 */
abstract class ZKMap<K, V> {

  final CuratorFramework curator;
  protected final Vertx vertx;
  final String mapPath;
  protected final String mapName;

  static final String ZK_PATH_ASYNC_MAP = "asyncMap";
  static final String ZK_PATH_ASYNC_MULTI_MAP = "asyncMultiMap";
  static final String EVENTBUS_PATH = "/" + ZK_PATH_ASYNC_MULTI_MAP + "/__vertx.subs/";
  static final String ZK_PATH_SYNC_MAP = "syncMap";

  private RetryPolicy retryPolicy = new ExponentialBackoffRetry(100, 5);

  ZKMap(CuratorFramework curator, Vertx vertx, String mapType, String mapName) {
    this.curator = curator;
    this.vertx = vertx;
    this.mapName = mapName;
    this.mapPath = "/" + mapType + "/" + mapName;
  }

  String keyPath(K k) {
    return mapPath + "/" + k.toString();
  }

  String valuePath(K k, Object v) {
    return keyPath(k) + "/" + v.toString();
  }

  Future<Void> assertKeyIsNotNull(Object key) {
    boolean result = key == null;
    if (result) return Future.failedFuture("key can not be null.");
    else return Future.succeededFuture();
  }

  Future<Void> assertValueIsNotNull(Object value) {
    boolean result = value == null;
    if (result) return Future.failedFuture("value can not be null.");
    else return Future.succeededFuture();
  }

  Future<Void> assertKeyAndValueAreNotNull(Object key, Object value) {
    return assertKeyIsNotNull(key).compose(aVoid -> assertValueIsNotNull(value));
  }

  byte[] asByte(Object object) throws IOException {
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

  <T> T asObject(byte[] bytes) throws Exception {
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
        ClusterSerializable clusterSerializable;
        //check clazz if have a public Constructor method.
        if (clazz.getConstructors().length == 0) {
          Constructor<T> constructor = (Constructor<T>) clazz.getDeclaredConstructor();
          constructor.setAccessible(true);
          clusterSerializable = (ClusterSerializable) constructor.newInstance();
        } else {
          clusterSerializable = (ClusterSerializable) clazz.newInstance();
        }
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

  /**
   * get data with Stat
   *
   * @param stat new Stat
   * @param path node path
   * @param <T>  result
   * @return T
   * @throws Exception
   */
  <T> T getData(Stat stat, String path) throws Exception {
    T result = null;
    if (null != curator.checkExists().forPath(path)) {
      result = asObject(curator.getData().storingStatIn(stat).forPath(path));
    } else {
      curator.create().creatingParentsIfNeeded().forPath(path, asByte(null));
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
  boolean compareAndSet(long startTime, int retries, Stat stat, String path, V expect, V update) throws Exception {
    V currentValue = getData(stat, path);
    if (currentValue == expect || currentValue.equals(expect)) {
      try {
        curator.setData().withVersion(stat.getVersion()).forPath(path, asByte(update));
      } catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
        // If the version has changed, block on the retry policy if necessary. If no more retries are remaining,
        // fail the operation.
        if (!retryPolicy.allowRetry(retries, Instant.now().toEpochMilli() - startTime, RetryLoop.getDefaultRetrySleeper())) {
          throw new VertxException("failed to acquire optimistic lock");
        }
      }
      return true;
    } else {
      return false;
    }
  }

  Future<Boolean> checkExists(K k) {
    return checkExists(keyPath(k));
  }

  Future<Boolean> checkExists(String path) {
    Promise<Boolean> future = Promise.promise();
    try {
      curator.sync().inBackground((clientSync, eventSync) -> {
        try {
          if (eventSync.getType() == CuratorEventType.SYNC) {
            curator.checkExists().inBackground((clientCheck, eventCheck) -> {
              if (eventCheck.getType() == CuratorEventType.EXISTS) {
                if (eventCheck.getStat() == null) {
                  vertx.runOnContext(aVoid -> future.complete(false));
                } else {
                  vertx.runOnContext(aVoid -> future.complete(true));
                }
              }
            }).forPath(path);
          }
        } catch (Exception ex) {
          vertx.runOnContext(aVoid -> future.fail(ex));
        }
      }).forPath(path);
    } catch (Exception ex) {
      vertx.runOnContext(aVoid -> future.fail(ex));
    }
    return future.future();
  }

  Future<Stat> create(K k, V v, Optional<Long> timeToLive) {
    return create(keyPath(k), v, timeToLive);
  }

  Future<Stat> create(String path, V v, Optional<Long> timeToLive) {
    Promise<Stat> future = Promise.promise();
    try {
      //there are two type of node - ephemeral and persistent.
      //if path is 'asyncMultiMap/subs/' which save the data of eventbus address and serverID we could using ephemeral,
      //since the lifecycle of this path as long as this verticle.
      CreateMode nodeMode = path.contains(EVENTBUS_PATH) ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;
      //as zk 3.5.x provide ttl node mode, we should consider it.
      nodeMode = timeToLive.isPresent() ? CreateMode.PERSISTENT_WITH_TTL : nodeMode;
      ProtectACLCreateModeStatPathAndBytesable<String> pathAndBytesable = timeToLive.isPresent()
        ? curator.create().withTtl(timeToLive.get()).creatingParentsIfNeeded()
        : curator.create().creatingParentsIfNeeded();
      pathAndBytesable.withMode(nodeMode).inBackground((cl, el) -> {
        if (el.getType() == CuratorEventType.CREATE) {
          vertx.runOnContext(event -> future.complete(el.getStat()));
        }
      }).forPath(path, asByte(v));
    } catch (Exception ex) {
      vertx.runOnContext(event -> future.fail(ex));
    }
    return future.future();
  }

  Future<Stat> setData(K k, V v) {
    return setData(keyPath(k), v);
  }

  Future<Stat> setData(String path, V v) {
    Promise<Stat> future = Promise.promise();
    try {
      curator.setData().inBackground((client, event) -> {
        if (event.getType() == CuratorEventType.SET_DATA) {
          vertx.runOnContext(e -> future.complete(event.getStat()));
        }
      }).forPath(path, asByte(v));
    } catch (Exception ex) {
      vertx.runOnContext(event -> future.fail(ex));
    }
    return future.future();
  }

  Future<V> delete(K k, V v) {
    return delete(keyPath(k), v);
  }

  Future<V> delete(String path, V v) {
    Promise<V> future = Promise.promise();
    try {
      curator.delete().deletingChildrenIfNeeded().inBackground((client, event) -> {
        if (event.getType() == CuratorEventType.DELETE) {
          //clean parent node if doesn't have child node.
          String[] paths = path.split("/");
          String parentNodePath = Stream.of(paths).limit(paths.length - 1).reduce((previous, current) -> previous + "/" + current).get();
          curator.getChildren().inBackground((childClient, childEvent) -> {
            if (childEvent.getChildren().size() == 0) {
              curator.delete().inBackground((deleteClient, deleteEvent) -> {
                if (deleteEvent.getType() == CuratorEventType.DELETE)
                  vertx.runOnContext(ea -> future.complete(v));
              }).forPath(parentNodePath);
            } else {
              vertx.runOnContext(ea -> future.complete(v));
            }
          }).forPath(parentNodePath);
        }
      }).forPath(path);
    } catch (Exception ex) {
      vertx.runOnContext(aVoid -> future.fail(ex));
    }
    return future.future();
  }
}
