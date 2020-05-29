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

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.*;
import io.vertx.spi.cluster.zookeeper.impl.ZKAsyncMap;
import io.vertx.spi.cluster.zookeeper.impl.ZKCounter;
import io.vertx.spi.cluster.zookeeper.impl.ZKLock;
import io.vertx.spi.cluster.zookeeper.impl.ZKSyncMap;
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

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
  private CuratorFramework curator;
  private boolean customCuratorCluster;
  private RetryPolicy retryPolicy;
  private Map<String, ZKLock> locks = new ConcurrentHashMap<>();
  private Map<String, AsyncMap<?, ?>> asyncMapCache = new ConcurrentHashMap<>();

  private static final String DEFAULT_CONFIG_FILE = "default-zookeeper.json";
  private static final String CONFIG_FILE = "zookeeper.json";
  private static final String ZK_SYS_CONFIG_KEY = "vertx.zookeeper.config";
  private JsonObject conf = new JsonObject();

  private static final String ZK_PATH_LOCKS = "/locks/";
  private static final String ZK_PATH_CLUSTER_NODE = "/cluster/nodes/";
  private static final String ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH = "/cluster/nodes";
  private static final String VERTX_HA_NODE = "__vertx.haInfo";

  private ExecutorService lockReleaseExec;

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

  //just for unit testing
  ZookeeperClusterManager(RetryPolicy retryPolicy, CuratorFramework curator) {
    Objects.requireNonNull(retryPolicy, "The retry policy cannot be null.");
    Objects.requireNonNull(curator, "The Curator instance cannot be null.");
    this.retryPolicy = retryPolicy;
    this.curator = curator;
    this.nodeId = UUID.randomUUID().toString();
    this.customCuratorCluster = true;
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
    InputStream is = null;
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
    return clusterNodes.getCurrentData().stream().map(e -> new String(e.getData())).collect(Collectors.toList());
  }

  @Override
  public void nodeListener(NodeListener listener) {
    this.nodeListener = listener;
  }

  @Override
  public void setNodeInfo(NodeInfo nodeInfo, Promise<Void> promise) {
    // FIXME
  }

  @Override
  public NodeInfo getNodeInfo() {
    // FIXME
    return null;
  }

  @Override
  public void getNodeInfo(String nodeId, Promise<NodeInfo> promise) {
    // FIXME
  }

  private void addLocalNodeId() throws VertxException {
    clusterNodes = new PathChildrenCache(curator, ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH, true);
    clusterNodes.getListenable().addListener(this);
    try {
      clusterNodes.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
      //Join to the cluster
      createThisNode();
      joined = true;
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }

  private void createThisNode() throws Exception {
    //clean ha node would be happened multi times with multi vertx node in startup, so we have a lock to avoid conflict.
    Promise<Lock> promise = Promise.promise();
    getLockWithTimeout("__cluster_init_lock", 3000L, promise);
    promise.future().onComplete(lockAsyncResult -> {
      if (lockAsyncResult.succeeded()) {
        try {
          //we have to clear `__vertx.haInfo` node if cluster is empty, as __haInfo is PERSISTENT mode, so we can not delete last
          //child of this path.
          if (clusterNodes.getCurrentData().size() == 0 && curator.checkExists().forPath("/syncMap") != null)
            if (curator.checkExists().forPath("/syncMap/" + VERTX_HA_NODE) != null) {
              getSyncMap(VERTX_HA_NODE).clear();
            }
          if (curator.checkExists().forPath("/syncMap/" + VERTX_HA_NODE) != null) {
            getSyncMap(VERTX_HA_NODE).clear();
          }
        } catch (Exception ex) {
          log.error("check zk node failed.", ex);
        } finally {
          lockAsyncResult.result().release();
        }
      } else {
        log.error("get cluster init lock failed.", lockAsyncResult.cause());
      }
    });
    curator.create().withMode(CreateMode.EPHEMERAL).forPath(ZK_PATH_CLUSTER_NODE + nodeId, nodeId.getBytes());
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
      }
    }, promise);
  }

  @Override
  public void leave(Promise<Void> promise) {
    vertx.executeBlocking(prom -> {
      synchronized (ZookeeperClusterManager.this) {
        if (active) {
          active = false;
          lockReleaseExec.shutdown();
          try {
            curator.delete().deletingChildrenIfNeeded().inBackground((client, event) -> {
              if (event.getType() == CuratorEventType.DELETE) {
                if (customCuratorCluster) {
                  prom.complete();
                } else {
                  if (curator.getState() == CuratorFrameworkState.STARTED) {
                    curator.close();
                    prom.complete();
                  }
                }
              }
            }).forPath(ZK_PATH_CLUSTER_NODE + nodeId);
          } catch (Exception e) {
            log.error(e);
            prom.fail(e);
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
    // FIXME
  }

  @Override
  public void removeRegistration(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    // FIXME
  }

  @Override
  public void getRegistrations(String address, Promise<List<RegistrationInfo>> promise) {
    // FIXME
  }

  @Override
  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
    if (!active) return;
    switch (event.getType()) {
      case CHILD_ADDED:
        try {
          if (nodeListener != null) {
            nodeListener.nodeAdded(new String(event.getData().getData()));
          }
        } catch (Throwable t) {
          log.error("Failed to handle memberAdded", t);
        }
        break;
      case CHILD_REMOVED:
        try {
          if (nodeListener != null) {
            nodeListener.nodeLeft(new String(event.getData().getData()));
          }
        } catch (Throwable t) {
          log.error("Failed to handle memberRemoved", t);
        }
        break;
      case CHILD_UPDATED:
        log.warn("Weird event that update cluster node. path:" + event.getData().getPath());
        break;
      case CONNECTION_RECONNECTED:
        if (joined) {
          createThisNode();
        }
        break;
      case CONNECTION_SUSPENDED:
        //just release locks on this node.
        locks.values().forEach(ZKLock::release);
        break;
      case CONNECTION_LOST:
        //release locks and clean locks
        locks.values().forEach(ZKLock::release);
        locks.clear();
        break;
    }
  }
}
