package io.vertx.spi.cluster.impl.zookeeper;

import com.google.common.collect.Maps;
import io.vertx.core.VertxException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by Stream.Liu
 */
class ZKSyncMap<K, V> extends ZKMap<K, V> implements Map<K, V> {

  ZKSyncMap(CuratorFramework curator, String mapName) {
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
      return curator.getChildren().forPath(mapPath).isEmpty();
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public boolean containsKey(Object key) {
    try {
      return curator.checkExists().forPath(keyPath((K) key)) != null;
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  @Override
  public boolean containsValue(Object value) {
    try {
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
        curator.create().creatingParentsIfNeeded().forPath(keyPath, valueBytes);
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

  @Override
  public Set<Entry<K, V>> entrySet() {
    return keySet().stream().map(k -> {
      V v = get(k);
      return Maps.immutableEntry(k, v);
    }).collect(Collectors.toSet());
  }

  private static class KeyValue<K, V> implements Serializable {
    private K key;
    private V value;

    private KeyValue(K key, V value) {
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
