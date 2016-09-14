package io.vertx.spi.cluster.zookeeper;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.zookeeper.impl.ZKAsyncMap;
import io.vertx.spi.cluster.zookeeper.impl.ZKAsyncMultiMap;
import io.vertx.spi.cluster.zookeeper.impl.ZKSyncMap;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A cluster manager that uses Zookeeper
 *
 * @author Stream.Liu
 */
public class ZookeeperClusterManager implements ClusterManager, PathChildrenCacheListener, ConnectionStateListener {

  private static final Logger log = LoggerFactory.getLogger(ZookeeperClusterManager.class);
  private Vertx vertx;

  private NodeListener nodeListener;
  private PathChildrenCache clusterNodes;
  private volatile boolean active;

  private String nodeID;
  private CuratorFramework curator;
  private boolean customCuratorCluster;
  private RetryPolicy retryPolicy;
  private Map<String, ZKLock> locks = new ConcurrentHashMap<>();

  private static final String DEFAULT_CONFIG_FILE = "default-zookeeper.json";
  private static final String CONFIG_FILE = "zookeeper.json";
  private static final String ZK_SYS_CONFIG_KEY = "vertx.zookeeper.config";
  private JsonObject conf = new JsonObject();

  private static final String ZK_PATH_LOCKS = "/locks/";
  private static final String ZK_PATH_COUNTERS = "/counters/";
  private static final String ZK_PATH_CLUSTER_NODE = "/cluster/nodes/";
  private static final String ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH = "/cluster/nodes";

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

  public ZookeeperClusterManager(CuratorFramework curator, String nodeID) {
    Objects.requireNonNull(curator, "The Curator instance cannot be null.");
    Objects.requireNonNull(nodeID, "The nodeID cannot be null.");
    this.curator = curator;
    this.nodeID = nodeID;
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
    this.nodeID = UUID.randomUUID().toString();
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
  public void setVertx(Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Every eventbus handler has an ID. SubsMap (subscriber map) is a MultiMap which
   * maps handler-IDs with server-IDs and thus allows the eventbus to determine where
   * to send messages.
   *
   * @param name A unique name by which the the MultiMap can be identified within the cluster.
   *             See the cluster config file (e.g. io.vertx.spi.cluster.impl.zookeeper.zookeeper.properties in case of ZookeeperClusterManager) for
   *             additional MultiMap config parameters.
   * @return subscription map
   */
  @Override
  public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> handler) {
    vertx.runOnContext(event -> handler.handle(Future.succeededFuture(new ZKAsyncMultiMap<>(vertx, curator, name))));
  }

  @Override
  public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> handler) {
    vertx.runOnContext(event -> handler.handle(Future.succeededFuture(new ZKAsyncMap<>(vertx, curator, name))));
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return new ZKSyncMap<>(curator, name);
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
    vertx.executeBlocking(future -> {
      ZKLock lock = locks.get(name);
      if (lock == null) {
        InterProcessSemaphoreMutex mutexLock = new InterProcessSemaphoreMutex(curator, ZK_PATH_LOCKS + name);
        lock = new ZKLock(mutexLock);
      }
      try {
        if (lock.getLock().acquire(timeout, TimeUnit.MILLISECONDS)) {
          locks.putIfAbsent(name, lock);
          future.complete(lock);
        } else {
          future.fail(new VertxException("Timed out waiting to get lock " + name));
        }
      } catch (Exception e) {
        future.fail(new VertxException("get lock exception", e));
      }
    }, resultHandler);
  }

  @Override
  public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
    vertx.executeBlocking(future -> {
      try {
        Objects.requireNonNull(name);
        future.complete(new ZKCounter(name, retryPolicy));
      } catch (Exception e) {
        future.fail(new VertxException(e));
      }
    }, resultHandler);
  }

  @Override
  public String getNodeID() {
    return nodeID;
  }

  @Override
  public List<String> getNodes() {
    return clusterNodes.getCurrentData().stream().map(e -> new String(e.getData())).collect(Collectors.toList());
  }

  @Override
  public void nodeListener(NodeListener listener) {
    this.nodeListener = listener;
  }

  private void addLocalNodeID() throws VertxException {
    clusterNodes = new PathChildrenCache(curator, ZK_PATH_CLUSTER_NODE_WITHOUT_SLASH, true);
    clusterNodes.getListenable().addListener(this);
    try {
      clusterNodes.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
      //Join to the cluster
      curator.create().withMode(CreateMode.EPHEMERAL).forPath(ZK_PATH_CLUSTER_NODE + nodeID, nodeID.getBytes());
    } catch (Exception e) {
      throw new VertxException(e);
    }
  }


  @Override
  public synchronized void join(Handler<AsyncResult<Void>> resultHandler) {
    vertx.executeBlocking(future -> {
      if (!active) {
        active = true;

        //The curator instance has been passed using the constructor.
        if (customCuratorCluster) {
          try {
            addLocalNodeID();
          } catch (VertxException e) {
            future.fail(e);
          }
          future.complete();
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
        nodeID = UUID.randomUUID().toString();
        try {
          addLocalNodeID();
        } catch (Exception e) {
          future.fail(e);
        }
        future.complete();
      }
    }, resultHandler);
  }

  @Override
  public void leave(Handler<AsyncResult<Void>> resultHandler) {
    vertx.executeBlocking(future -> {
      synchronized (ZookeeperClusterManager.this) {
        if (active) {
          active = false;
          try {
            curator.delete().deletingChildrenIfNeeded().inBackground((client, event) -> {
              if (event.getType() == CuratorEventType.DELETE) {
                clusterNodes.getListenable().removeListener(ZookeeperClusterManager.this);
                if (!customCuratorCluster && curator.getState() == CuratorFrameworkState.STARTED) {
                  curator.close();
                }
              }
            }).forPath(ZK_PATH_CLUSTER_NODE + nodeID);
          } catch (Exception e) {
            log.error(e);
          }
        }
      }
      future.complete();
    }, resultHandler);
  }

  @Override
  public boolean isActive() {
    return active;
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
    }
  }

  /**
   * some state make effect to the lock, we have to handle it.
   *
   * @param client   curator
   * @param newState the state of connection to the io.vertx.spi.cluster.impl.zookeeper.zookeeper.
   */
  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    switch (newState) {
      case LOST:
        //release locks and clean locks
        locks.values().forEach(ZKLock::release);
        locks.clear();
        break;
      case SUSPENDED:
        //just release locks on this node.
        locks.values().forEach(ZKLock::release);
        break;
      case RECONNECTED:
        break;
    }
  }

  /**
   * Counter implement
   */
  private class ZKCounter implements Counter {

    private DistributedAtomicLong atomicLong;
    private String counterPath;

    public ZKCounter(String nodeName, RetryPolicy retryPolicy) throws Exception {
      this.counterPath = ZK_PATH_COUNTERS + nodeName;
      this.atomicLong = new DistributedAtomicLong(curator, counterPath, retryPolicy);
    }

    @Override
    public void get(Handler<AsyncResult<Long>> resultHandler) {
      Objects.requireNonNull(resultHandler);
      vertx.executeBlocking(future -> {
        try {
          future.complete(atomicLong.get().preValue());
        } catch (Exception e) {
          future.fail(new VertxException(e));
        }
      }, resultHandler);
    }

    @Override
    public void incrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
      Objects.requireNonNull(resultHandler);
      increment(true, resultHandler);
    }

    @Override
    public void getAndIncrement(Handler<AsyncResult<Long>> resultHandler) {
      increment(false, resultHandler);
    }

    private void increment(boolean post, Handler<AsyncResult<Long>> resultHandler) {
      Objects.requireNonNull(resultHandler);
      vertx.executeBlocking(future -> {
        try {
          long returnValue = 0;
          if (atomicLong.get().succeeded()) returnValue = atomicLong.get().preValue();
          if (atomicLong.increment().succeeded()) {
            future.complete(post ? atomicLong.get().postValue() : returnValue);
          } else {
            future.fail(new VertxException("increment value failed."));
          }
        } catch (Exception e) {
          future.fail(new VertxException(e));
        }
      }, resultHandler);
    }

    @Override
    public void decrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
      Objects.requireNonNull(resultHandler);
      vertx.executeBlocking(future -> {
        try {
          if (atomicLong.decrement().succeeded()) {
            future.complete(atomicLong.get().postValue());
          } else {
            future.fail(new VertxException("decrement value failed."));
          }
        } catch (Exception e) {
          future.fail(new VertxException(e));
        }
      }, resultHandler);
    }

    @Override
    public void addAndGet(long value, Handler<AsyncResult<Long>> resultHandler) {
      add(value, true, resultHandler);
    }

    @Override
    public void getAndAdd(long value, Handler<AsyncResult<Long>> resultHandler) {
      add(value, false, resultHandler);
    }

    private void add(long value, boolean post, Handler<AsyncResult<Long>> resultHandler) {
      Objects.requireNonNull(resultHandler);
      vertx.executeBlocking(future -> {
        try {
          long returnValue = 0;
          if (atomicLong.get().succeeded()) returnValue = atomicLong.get().preValue();
          if (atomicLong.add(value).succeeded()) {
            future.complete(post ? atomicLong.get().postValue() : returnValue);
          } else {
            future.fail(new VertxException("add value failed."));
          }
        } catch (Exception e) {
          future.fail(new VertxException(e));
        }
      }, resultHandler);
    }

    @Override
    public void compareAndSet(long expected, long value, Handler<AsyncResult<Boolean>> resultHandler) {
      Objects.requireNonNull(resultHandler);
      vertx.executeBlocking(future -> {
        try {
          if (atomicLong.get().succeeded() && atomicLong.get().preValue() == 0) this.atomicLong.initialize(0L);
          future.complete(atomicLong.compareAndSet(expected, value).succeeded());
        } catch (Exception e) {
          future.fail(new VertxException(e));
        }
      }, resultHandler);
    }
  }


  /**
   * Lock implement
   */
  private class ZKLock implements Lock {
    private InterProcessSemaphoreMutex lock;

    private ZKLock(InterProcessSemaphoreMutex lock) {
      this.lock = lock;
    }

    public InterProcessSemaphoreMutex getLock() {
      return lock;
    }

    @Override
    public void release() {
      try {
        lock.release();
      } catch (Exception e) {
        log.error(e);
      }
    }
  }

}