/*
 *  Copyright (c) 2011-2020 The original author or authors
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

package io.vertx.spi.cluster.zookeeper;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.*;
import io.vertx.spi.cluster.zookeeper.impl.*;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A cluster manager that uses Zookeeper
 *
 * @author Stream.Liu
 */
public class ZookeeperClusterManager implements ClusterManager, PathChildrenCacheListener {

  private static final Logger log = LoggerFactory.getLogger(ZookeeperClusterManager.class);

  private VertxInternal vertx;

  private NodeListener nodeListener;
  private RegistrationListener registrationListener;
  private PathChildrenCache clusterNodes;
  private volatile boolean active;
  private volatile boolean joined;

  private String nodeId;
  private NodeInfo nodeInfo;
  private CuratorFramework curator;
  private boolean customCuratorCluster;
  private RetryPolicy retryPolicy;
  private SubsMapHelper subsMapHelper;
  private final Map<String, NodeInfo> localNodeInfo = new ConcurrentHashMap<>();
  private final Map<String, ZKLock> locks = new ConcurrentHashMap<>();
  private final Map<String, AsyncMap<?, ?>> asyncMapCache = new ConcurrentHashMap<>();

  private JsonObject conf = new JsonObject();

  private static final String ZK_PATH_LOCKS = "/locks/";
  private static final String ZK_PATH_CLUSTER_NODE = "/cluster/nodes/";
  private static final String ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH = "/cluster/nodes";

  private ExecutorService lockReleaseExec;

  private Function<String, String> resolveNodeId = path -> {
    String[] pathArr = path.split("\\/");
    return pathArr[pathArr.length - 1];
  };

  public ZookeeperClusterManager() {
    conf = ConfigUtil.loadConfig(null);
  }

  public ZookeeperClusterManager(CuratorFramework curator) {
    this(curator, UUID.randomUUID().toString());
  }

  public ZookeeperClusterManager(String resourceLocation) {
    conf = ConfigUtil.loadConfig(resourceLocation);
  }

  public ZookeeperClusterManager(CuratorFramework curator, String nodeId) {
    Objects.requireNonNull(curator, "The Curator instance cannot be null.");
    Objects.requireNonNull(nodeId, "The nodeId cannot be null.");
    this.curator = curator;
    this.nodeId = nodeId;
    this.customCuratorCluster = true;
  }

  public ZookeeperClusterManager(JsonObject config) {
    this.conf = config;
  }

  public void setConfig(JsonObject conf) {
    this.conf = conf;
  }

  public JsonObject getConfig() {
    return conf;
  }

  public CuratorFramework getCuratorFramework() {
    return this.curator;
  }

  @Override
  public void init(Vertx vertx) {
    this.vertx = (VertxInternal) vertx;
  }

  @Override
  public <K, V> void getAsyncMap(String name, Promise<AsyncMap<K, V>> promise) {
    vertx.<AsyncMap<K, V>>executeBlocking(() -> {
      @SuppressWarnings("unchecked")
      AsyncMap<K, V> zkAsyncMap = (AsyncMap<K, V>) asyncMapCache.computeIfAbsent(name, key -> new ZKAsyncMap<>(vertx, curator, name));
      return zkAsyncMap;
    }).onComplete(promise);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return new ZKSyncMap<>(curator, name);
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Promise<Lock> promise) {
    vertx.<Lock>executeBlocking(() -> {
      ZKLock lock = locks.get(name);
      if (lock == null) {
        InterProcessSemaphoreMutex mutexLock = new InterProcessSemaphoreMutex(curator, ZK_PATH_LOCKS + name);
        lock = new ZKLock(mutexLock, lockReleaseExec);
      }
      try {
        if (lock.getLock().acquire(timeout, TimeUnit.MILLISECONDS)) {
          locks.putIfAbsent(name, lock);
          return lock;
        } else {
          throw new VertxException("Timed out waiting to get lock " + name);
        }
      } catch (Exception e) {
        throw new VertxException("get lock exception", e);
      }
    }, false).onComplete(promise);
  }

  @Override
  public void getCounter(String name, Promise<Counter> promise) {
    vertx.<Counter>executeBlocking(() -> {
      try {
        Objects.requireNonNull(name);
        return new ZKCounter(vertx, curator, name, retryPolicy);
      } catch (Exception e) {
        throw new VertxException(e, true);
      }
    }).onComplete(promise);
  }

  @Override
  public String getNodeId() {
    return nodeId;
  }

  @Override
  public List<String> getNodes() {
    return clusterNodes.getCurrentData().stream()
      .map(childData -> resolveNodeId.apply(childData.getPath()))
      .collect(Collectors.toList());
  }

  @Override
  public void registrationListener(RegistrationListener registrationListener) {
    this.registrationListener = registrationListener;
  }

  @Override
  public void nodeListener(NodeListener listener) {
    this.nodeListener = listener;
  }

  @Override
  public void setNodeInfo(NodeInfo nodeInfo, Promise<Void> promise) {
    synchronized (this) {
      this.nodeInfo = nodeInfo;
    }
    try {
      Buffer buffer = Buffer.buffer();
      nodeInfo.writeToBuffer(buffer);
      curator.create().orSetData().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).inBackground((c, e) -> {
        if (e.getType() == CuratorEventType.SET_DATA || e.getType() == CuratorEventType.CREATE) {
          vertx.runOnContext(Avoid -> {
            localNodeInfo.put(nodeId, nodeInfo);
            promise.complete();
          });
        }
      }).withUnhandledErrorListener(log::error).forPath(ZK_PATH_CLUSTER_NODE + nodeId, buffer.getBytes());
    } catch (Exception e) {
      log.error("create node failed.", e);
    }
  }

  @Override
  public synchronized NodeInfo getNodeInfo() {
    return nodeInfo;
  }

  @Override
  public void getNodeInfo(String nodeId, Promise<NodeInfo> promise) {
    vertx.<NodeInfo>executeBlocking(() -> {
      return Optional.ofNullable(clusterNodes.getCurrentData(ZK_PATH_CLUSTER_NODE + nodeId))
        .map(childData -> {
            Buffer buffer = Buffer.buffer(childData.getData());
            NodeInfo nodeInfo = new NodeInfo();
            nodeInfo.readFromBuffer(0, buffer);
            return nodeInfo;
          }).orElseThrow(() -> new VertxException("Not a member of the cluster", true));
    }, false).onComplete(promise);
  }

  private void addLocalNodeId() throws VertxException {
    clusterNodes = new PathChildrenCache(curator, ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH, true);
    clusterNodes.getListenable().addListener(this);
    try {
      clusterNodes.start(PathChildrenCache.StartMode.NORMAL);
      //Join to the cluster
      createThisNode();
      joined = true;
      subsMapHelper = new SubsMapHelper(curator, vertx, registrationListener, nodeId);
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  private void createThisNode() throws Exception {
    try {
      curator.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(ZK_PATH_CLUSTER_NODE + nodeId, nodeId.getBytes());
    } catch (KeeperException.NodeExistsException e) {
      //idempotent
      log.info("node:" + nodeId + " have created successful.");
    }
  }

  @Override
  public void join(Promise<Void> promise) {
    vertx.<Void>executeBlocking(() -> {
      if (!active) {
        active = true;
        lockReleaseExec = Executors.newCachedThreadPool(r -> new Thread(r, "vertx-zookeeper-service-release-lock-thread"));

        //The curator instance has been passed using the constructor.
        if (customCuratorCluster) {
          addLocalNodeId();
          return null;
        }

        if (curator == null) {
          retryPolicy = RetryPolicyHelper.createRetryPolicy(conf.getJsonObject("retry", new JsonObject()));

          // Read the zookeeper hosts from a system variable
          String hosts = System.getProperty("vertx.zookeeper.hosts");
          if (hosts == null) {
            hosts = conf.getString("zookeeperHosts", "127.0.0.1");
          }
          log.info("Zookeeper hosts set to " + hosts);

          curator = CuratorFrameworkFactory.builder()
            .connectString(hosts)
            .namespace(conf.getString("rootPath", "io.vertx"))
            .sessionTimeoutMs(conf.getInteger("sessionTimeout", 20000))
            .connectionTimeoutMs(conf.getInteger("connectTimeout", 3000))
            .retryPolicy(retryPolicy).build();
        }
        curator.start();
        while (curator.getState() != CuratorFrameworkState.STARTED) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            if (curator.getState() != CuratorFrameworkState.STARTED) {
              throw new VertxException("zookeeper client being interrupted while starting.", true);
            }
          }
        }
        nodeId = UUID.randomUUID().toString();
        addLocalNodeId();
        return null;
      } else {
        return null;
      }
    }).onComplete(promise);
  }

  @Override
  public void leave(Promise<Void> promise) {
    vertx.<Void>executeBlocking(() -> {
      synchronized (ZookeeperClusterManager.this) {
        if (active) {
          active = false;
          joined = false;
          lockReleaseExec.shutdown();
          try {
            clusterNodes.close();
            subsMapHelper.close();
            curator.close();
          } catch (Exception e) {
            log.warn("zookeeper close exception.", e);
          }
        }
        return null;
      }
    }).onComplete(promise);
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public void addRegistration(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    subsMapHelper.put(address, registrationInfo, promise);
  }

  @Override
  public void removeRegistration(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    subsMapHelper.remove(address, registrationInfo, promise);
  }

  @Override
  public void getRegistrations(String address, Promise<List<RegistrationInfo>> promise) {
    vertx.executeBlocking(() -> subsMapHelper.get(address), false).onComplete(promise);
  }

  @Override
  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
    if (!active) return;
    switch (event.getType()) {
      case CHILD_ADDED:
        try {
          if (nodeListener != null && client.getState() != CuratorFrameworkState.STOPPED) {
            nodeListener.nodeAdded(resolveNodeId.apply(event.getData().getPath()));
          }
        } catch (Throwable t) {
          log.error("Failed to handle memberAdded", t);
        }
        break;
      case CHILD_REMOVED:
        try {
          if (nodeListener != null && client.getState() != CuratorFrameworkState.STOPPED) {
            nodeListener.nodeLeft(resolveNodeId.apply(event.getData().getPath()));
          }
        } catch (Throwable t) {
          log.warn("Failed to handle memberRemoved", t);
        }
        break;
      case CHILD_UPDATED:
        //log.warn("Weird event that update cluster node. path:" + event.getData().getPath());
        break;
      case CONNECTION_RECONNECTED:
        if (joined) {
          createThisNode();
          List<Future<Void>> futures = new ArrayList<>();
          for(Map.Entry<String, NodeInfo> entry : localNodeInfo.entrySet()) {
            Promise<Void> promise = Promise.promise();
            setNodeInfo(entry.getValue(), promise);
            futures.add(promise.future());
          }
          Future.all(futures).onComplete(ar -> {
            if (ar.failed()) {
              log.error("recover node info failed.", ar.cause());
            }
          });
        }
        break;
      case CONNECTION_SUSPENDED:
        //just release locks on this node.
        locks.values().forEach(ZKLock::release);
        break;
      case CONNECTION_LOST:
        //release locks and clean locks
        joined = false;
        locks.values().forEach(ZKLock::release);
        locks.clear();
        break;
    }
  }
}
