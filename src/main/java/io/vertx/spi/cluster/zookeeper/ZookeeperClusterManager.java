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

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
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
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.*;
import java.util.*;
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
  private NodeSelector nodeSelector;

  private NodeListener nodeListener;
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

  private static final String DEFAULT_CONFIG_FILE = "default-zookeeper.json";
  private static final String CONFIG_FILE = "zookeeper.json";
  private static final String ZK_SYS_CONFIG_KEY = "vertx.zookeeper.config";
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
    String resourceLocation = System.getProperty(ZK_SYS_CONFIG_KEY, CONFIG_FILE);
    loadProperties(resourceLocation);
  }

  public ZookeeperClusterManager(CuratorFramework curator) {
    this(curator, UUID.randomUUID().toString());
  }

  public ZookeeperClusterManager(String resourceLocation) {
    loadProperties(resourceLocation);
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

  private void loadProperties(String resourceLocation) {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(getConfigStream(resourceLocation))));
      String line;
      StringBuilder sb = new StringBuilder();
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      conf = new JsonObject(sb.toString());
      log.info("Loaded zookeeper.json file from resourceLocation=" + resourceLocation);
    } catch (FileNotFoundException e) {
      log.error("Could not find zookeeper config file", e);
    } catch (IOException e) {
      log.error("Failed to load zookeeper config", e);
    }
  }

  private InputStream getConfigStream(String resourceLocation) throws FileNotFoundException {
    ClassLoader ctxClsLoader = Thread.currentThread().getContextClassLoader();
    InputStream is           = null;
    if (resourceLocation.startsWith("classpath:")) {
      resourceLocation = resourceLocation.substring(10);
      if (ctxClsLoader != null) {
        is = ctxClsLoader.getResourceAsStream(resourceLocation);
      }
      if (is == null && !resourceLocation.equals(CONFIG_FILE)) {
        is = new FileInputStream(resourceLocation);
      } else if (is == null && resourceLocation.equals(CONFIG_FILE)) {
        is = getClass().getClassLoader().getResourceAsStream(resourceLocation);
        if (is == null) {
          is = getClass().getClassLoader().getResourceAsStream(DEFAULT_CONFIG_FILE);
        }
      }
    } else {
      try {
        is = new FileInputStream(resourceLocation);
      } catch (FileNotFoundException fileEx) {
        if (ctxClsLoader != null) {
          is = ctxClsLoader.getResourceAsStream(resourceLocation);
        }
        if (is == null && resourceLocation.equals(CONFIG_FILE)) {
          is = getClass().getClassLoader().getResourceAsStream(resourceLocation);
          if (is == null) {
            is = getClass().getClassLoader().getResourceAsStream(DEFAULT_CONFIG_FILE);
          }
        }
      }

    }
    return is;
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
  public void init(Vertx vertx, NodeSelector nodeSelector) {
    this.vertx = (VertxInternal) vertx;
    this.nodeSelector = nodeSelector;
  }

  @Override
  public <K, V> void getAsyncMap(String name, Promise<AsyncMap<K, V>> promise) {
    vertx.executeBlocking(prom -> {
      @SuppressWarnings("unchecked")
      AsyncMap<K, V> zkAsyncMap = (AsyncMap<K, V>) asyncMapCache.computeIfAbsent(name, key -> new ZKAsyncMap<>(vertx, curator, name));
      prom.complete(zkAsyncMap);
    }, promise);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return new ZKSyncMap<>(curator, name);
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Promise<Lock> promise) {
    vertx.executeBlocking(prom -> {
      ZKLock lock = locks.get(name);
      if (lock == null) {
        InterProcessSemaphoreMutex mutexLock = new InterProcessSemaphoreMutex(curator, ZK_PATH_LOCKS + name);
        lock = new ZKLock(mutexLock, lockReleaseExec);
      }
      try {
        if (lock.getLock().acquire(timeout, TimeUnit.MILLISECONDS)) {
          locks.putIfAbsent(name, lock);
          prom.complete(lock);
        } else {
          throw new VertxException("Timed out waiting to get lock " + name);
        }
      } catch (Exception e) {
        throw new VertxException("get lock exception", e);
      }
    }, false, promise);
  }

  @Override
  public void getCounter(String name, Promise<Counter> promise) {
    vertx.executeBlocking(future -> {
      try {
        Objects.requireNonNull(name);
        future.complete(new ZKCounter(vertx, curator, name, retryPolicy));
      } catch (Exception e) {
        future.fail(new VertxException(e));
      }
    }, promise);
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
      curator.create().orSetData().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).inBackground((c, e) -> {
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
    vertx.executeBlocking(prom -> {
      prom.complete(Optional.ofNullable(clusterNodes.getCurrentData(ZK_PATH_CLUSTER_NODE + nodeId))
        .map(childData -> {
          Buffer buffer = Buffer.buffer(childData.getData());
          NodeInfo nodeInfo = new NodeInfo();
          nodeInfo.readFromBuffer(0, buffer);
          return nodeInfo;
        }).orElseThrow(() -> new VertxException("Not a member of the cluster")));
    }, false, promise);
  }

  private void addLocalNodeId() throws VertxException {
    clusterNodes = new PathChildrenCache(curator, ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH, true);
    clusterNodes.getListenable().addListener(this);
    try {
      clusterNodes.start(PathChildrenCache.StartMode.NORMAL);
      //Join to the cluster
      createThisNode();
      joined = true;
      subsMapHelper = new SubsMapHelper(curator, vertx, nodeSelector, nodeId);
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  private void createThisNode() throws Exception {
    try {
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(ZK_PATH_CLUSTER_NODE + nodeId, nodeId.getBytes());
    } catch (KeeperException.NodeExistsException e) {
      //idempotent
      log.info("node:" + nodeId + " have created successful.");
    }
  }

  @Override
  public void join(Promise<Void> promise) {
    vertx.executeBlocking(prom -> {
      if (!active) {
        active = true;
        lockReleaseExec = Executors.newCachedThreadPool(r -> new Thread(r, "vertx-zookeeper-service-release-lock-thread"));

        //The curator instance has been passed using the constructor.
        if (customCuratorCluster) {
          try {
            addLocalNodeId();
            prom.complete();
          } catch (VertxException e) {
            prom.fail(e);
          }
          return;
        }

        if (curator == null) {
          retryPolicy = new ExponentialBackoffRetry(
            conf.getJsonObject("retry", new JsonObject()).getInteger("initialSleepTime", 1000),
            conf.getJsonObject("retry", new JsonObject()).getInteger("maxTimes", 5),
            conf.getJsonObject("retry", new JsonObject()).getInteger("intervalTimes", 10000));

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
              prom.fail("zookeeper client being interrupted while starting.");
            }
          }
        }
        nodeId = UUID.randomUUID().toString();
        try {
          addLocalNodeId();
          prom.complete();
        } catch (Exception e) {
          prom.fail(e);
        }
      } else {
        prom.complete();
      }
    }, promise);
  }

  @Override
  public void leave(Promise<Void> promise) {
    vertx.executeBlocking(prom -> {
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
          } finally {
            prom.complete();
          }
        } else prom.complete();
      }
    }, promise);
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
    vertx.executeBlocking(prom -> {
      prom.complete(subsMapHelper.get(address));
    }, false, promise);
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
          List<Future> futures = new ArrayList<>();
          for(Map.Entry<String, NodeInfo> entry : localNodeInfo.entrySet()) {
            Promise promise = Promise.promise();
            setNodeInfo(entry.getValue(), promise);
            futures.add(promise.future());
          }
          CompositeFuture.all(futures).onComplete(ar -> {
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
