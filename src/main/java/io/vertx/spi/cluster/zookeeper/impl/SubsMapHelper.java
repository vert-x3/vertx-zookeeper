package io.vertx.spi.cluster.zookeeper.impl;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeSelector;
import io.vertx.core.spi.cluster.RegistrationInfo;
import io.vertx.core.spi.cluster.RegistrationUpdateEvent;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.zookeeper.CreateMode;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SubsMapHelper implements TreeCacheListener {

  private final CuratorFramework curator;
  private final TreeCache treeCache;
  private final VertxInternal vertx;
  private final NodeSelector nodeSelector;
  private final String nodeId;
  private final ConcurrentMap<String, Set<RegistrationInfo>> ownSubs = new ConcurrentHashMap<>();

  private static final String VERTX_SUBS_NAME = "/__vertx.subs";
  private static final Logger log = LoggerFactory.getLogger(SubsMapHelper.class);

  private final Function<String, String> keyPath = address -> VERTX_SUBS_NAME + "/" + address;
  private final Function<RegistrationInfo, String> valuePath = registrationInfo -> registrationInfo.nodeId() + "-" + registrationInfo.seq();
  private final BiFunction<String, RegistrationInfo, String> fullPath = (address, registrationInfo) -> keyPath.apply(address) + "/" + valuePath.apply(registrationInfo);

  public SubsMapHelper(CuratorFramework curator, VertxInternal vertx, NodeSelector nodeSelector, String nodeId) {
    this.curator = curator;
    this.vertx = vertx;
    this.treeCache = new TreeCache(curator, VERTX_SUBS_NAME);
    try {
      this.treeCache.start();
    } catch (Exception e) {
      throw new VertxException(e);
    }
    this.nodeSelector = nodeSelector;
    this.nodeId = nodeId;
  }

  public void put(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    try {
      Buffer buffer = Buffer.buffer();
      registrationInfo.writeToBuffer(buffer);
      curator.create().orSetData().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).inBackground((c, e) -> {
        if (e.getType() == CuratorEventType.CREATE || e.getType() == CuratorEventType.SET_DATA) {
          vertx.runOnContext(Avoid -> {
            ownSubs.compute(address, (add, curr) -> {
              Set<RegistrationInfo> res = curr != null ? curr : new CopyOnWriteArraySet<>();
              res.add(registrationInfo);
              return res;
            });
            promise.complete();
          });
        }
      }).withUnhandledErrorListener(log::error).forPath(fullPath.apply(address, registrationInfo), buffer.getBytes());
    } catch (Exception e) {
      log.error(String.format("create subs address %s failed.", address), e);
    }
  }

  public List<RegistrationInfo> get(String address) {
    return Optional.ofNullable(treeCache.getCurrentChildren(keyPath.apply(address))).map(data -> {
      return data.values().stream().map(childData -> {
        RegistrationInfo registrationInfo = new RegistrationInfo();
        Buffer buffer = Buffer.buffer(childData.getData());
        registrationInfo.readFromBuffer(0, buffer);
        return registrationInfo;
      }).collect(Collectors.toList());
    }).orElse(new ArrayList<>());
  }

  public void remove(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
    try {
      curator.delete().inBackground((c, e) -> {
        if (e.getType() == CuratorEventType.DELETE) {
          vertx.runOnContext(aVoid -> {
            ownSubs.computeIfPresent(address, (add, curr) -> {
              curr.remove(registrationInfo);
              return curr.isEmpty() ? null : curr;
            });
            promise.complete();
          });
        }
      }).forPath(fullPath.apply(address, registrationInfo));
    } catch (Exception e) {
      log.error(String.format("remove subs address %s failed.", address), e);
      promise.fail(e);
    }
  }

  @Override
  public void childEvent(CuratorFramework client, TreeCacheEvent event) {
    switch (event.getType()) {
      case NODE_ADDED:
      case NODE_UPDATED:
      case NODE_REMOVED:
        String addr = event.getData().getPath().split("\\/", 3)[1];
        vertx.<List<RegistrationInfo>>executeBlocking(prom -> {
          prom.complete(get(addr));
        }, false, ar -> {
          if (ar.succeeded()) {
            nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(addr, ar.result()));
          } else {
            log.trace("A failure occured while retrieving the updated registrations", ar.cause());
            nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(addr, Collections.emptyList()));
          }
        });
        break;
      case CONNECTION_SUSPENDED:
        log.warn(String.format("vertx node %s which connected to zookeeper have been suspended.", nodeId));
        break;
      case CONNECTION_LOST:
        log.warn(String.format("vertx node %s which connected to zookeeper has lost", nodeId));
        break;
      case CONNECTION_RECONNECTED:
        log.info(String.format("vertx node %s have reconnected to zookeeper", nodeId));
        vertx.runOnContext(aVoid -> {
          List<Future> futures = new ArrayList<>();
          for (Map.Entry<String, Set<RegistrationInfo>> entry : ownSubs.entrySet()) {
            for (RegistrationInfo registrationInfo : entry.getValue()) {
              Promise<Void> promise = Promise.promise();
              put(entry.getKey(), registrationInfo, promise);
              futures.add(promise.future());
            }
          }
          CompositeFuture.all(futures).onComplete(ar -> {
            if (ar.failed()) {
              log.error("recover node subs information failed.", ar.cause());
            } else {
              log.info("recover node subs success.");
            }
          });
        });
        break;
    }
  }

}
