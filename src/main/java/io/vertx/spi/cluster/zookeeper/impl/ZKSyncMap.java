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

import io.vertx.core.VertxException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by Stream.Liu
 */
public class ZKSyncMap<K, V> extends ZKMap<K, V> implements Map<K, V> {

  public ZKSyncMap(CuratorFramework curator, String mapName) {
    super(curator, null, ZK_PATH_SYNC_MAP, mapName);
  }

  @Override
  public int size() {
    try {
      return curator.getChildren().forPath(mapPath).size();
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public boolean isEmpty() {
    try {
      syncKeyPath(mapPath);
      return curator.getChildren().forPath(mapPath).isEmpty();
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public boolean containsKey(Object key) {
    try {
      String keyPath = keyPath((K) key);
      syncKeyPath(keyPath);
      return curator.checkExists().forPath(keyPath) != null;
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public boolean containsValue(Object value) {
    try {
      syncKeyPath(mapPath);
      return curator.getChildren().forPath(mapPath).stream().anyMatch(k -> {
        try {
          byte[] bytes = curator.getData().forPath(keyPath((K) k));
          KeyValue<K, V> keyValue = asObject(bytes);
          return keyValue.getValue().equals(value);
        } catch (Exception ex) {
          throw new VertxException(ex);
        }
      });
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public V get(Object key) {
    try {
      String keyPath = keyPath((K) key);
      syncKeyPath(keyPath);
      if (null == curator.checkExists().forPath(keyPath)) {
        return null;
      } else {
        KeyValue<K, V> keyValue = asObject(curator.getData().forPath(keyPath));
        return keyValue.getValue();
      }
    } catch (Exception e) {
      if (!(e instanceof KeeperException.NodeExistsException)) {
        throw new VertxException(e);
      }
    }
    return null;
  }

  @Override
  public V put(K key, V value) {
    try {
      String keyPath = keyPath(key);
      KeyValue<K, V> keyValue = new KeyValue<>(key, value);
      byte[] valueBytes = asByte(keyValue);
      if (get(key) != null) {
        curator.setData().forPath(keyPath, valueBytes);
      } else {
        curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(keyPath, valueBytes);
      }
      return value;
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public V remove(Object key) {
    try {
      V result = get(key);
      if (result != null) curator.delete().deletingChildrenIfNeeded().forPath(keyPath((K) key));
      return result;
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    m.entrySet().stream().forEach(entry -> put(entry.getKey(), entry.getValue()));
  }

  @Override
  public void clear() {
    try {
      curator.delete().deletingChildrenIfNeeded().forPath(mapPath);
      curator.create().creatingParentsIfNeeded().forPath(mapPath);
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public Set<K> keySet() {
    try {
      syncKeyPath(mapPath);
      return curator.getChildren().forPath(mapPath).stream().map(k -> {
        try {
          KeyValue<K, V> keyValue = asObject(curator.getData().forPath(keyPath((K) k)));
          return keyValue.getKey();
        } catch (Exception ex) {
          throw new VertxException(ex);
        }
      }).collect(Collectors.toSet());
    } catch (Exception ex) {
      throw new VertxException(ex);
    }
  }

  @Override
  public Collection<V> values() {
    try {
      syncKeyPath(mapPath);
      return curator.getChildren().forPath(mapPath).stream()
        .map(k -> {
            try {
              KeyValue<K, V> keyValue = asObject(curator.getData().forPath(keyPath((K) k)));
              return keyValue.getValue();
            } catch (Exception ex) {
              throw new VertxException(ex);
            }
          }
        ).collect(Collectors.toSet());
    } catch (Exception ex) {
      throw new VertxException(ex);
    }
  }

  private void syncKeyPath(String path) {
    //sync always run in background, so we have to using latch to wait callback.
    CountDownLatch latch = new CountDownLatch(1);
    try {
      curator.sync().inBackground((client, event) -> {
        if (event.getPath().equals(path) && event.getType() == CuratorEventType.SYNC) {
          latch.countDown();
        }
      }).forPath(path);
      latch.await(3L, TimeUnit.SECONDS);
    } catch (Exception e) {
      if (!(e instanceof KeeperException.NodeExistsException)) {
        throw new VertxException(e);
      }
    }
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return keySet().stream().map(k -> {
      V v = get(k);
      return new HashMap.SimpleImmutableEntry<>(k, v);
    }).collect(Collectors.toSet());
  }

  static class KeyValue<K, V> implements Serializable {
    private static final long serialVersionUID = 6529685098267757690L;
    private K key;
    private V value;

    public KeyValue(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }
  }

}
