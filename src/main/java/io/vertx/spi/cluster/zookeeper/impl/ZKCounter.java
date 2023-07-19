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

package io.vertx.spi.cluster.zookeeper.impl;

import io.vertx.core.Future;
import io.vertx.core.VertxException;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.shareddata.Counter;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;

public class ZKCounter implements Counter {

  private static final String ZK_PATH_COUNTERS = "/counters/";

  private final VertxInternal vertx;
  private final DistributedAtomicLong atomicLong;

  public ZKCounter(VertxInternal vertx, CuratorFramework curator, String nodeName, RetryPolicy retryPolicy) {
    this.vertx = vertx;
    String counterPath = ZK_PATH_COUNTERS + nodeName;
    this.atomicLong = new DistributedAtomicLong(curator, counterPath, retryPolicy);
  }

  @Override
  public Future<Long> get() {
    return vertx.executeBlocking(() -> {
      try {
        return atomicLong.get().preValue();
      } catch (Exception e) {
        throw new VertxException(e, true);
      }
    });
  }

  @Override
  public Future<Long> incrementAndGet() {
    return increment(true);
  }

  @Override
  public Future<Long> getAndIncrement() {
    return increment(false);
  }

  private Future<Long> increment(boolean post) {
    return vertx.executeBlocking(() -> {
      try {
        long returnValue = 0;
        if (atomicLong.get().succeeded()) returnValue = atomicLong.get().preValue();
        if (atomicLong.increment().succeeded()) {
          return post ? atomicLong.get().postValue() : returnValue;
        } else {
          throw new VertxException("increment value failed.", true);
        }
      } catch (Exception e) {
        throw new VertxException(e, true);
      }
    });
  }

  @Override
  public Future<Long> decrementAndGet() {
    return vertx.executeBlocking(() -> {
      try {
        if (atomicLong.decrement().succeeded()) {
          return atomicLong.get().postValue();
        } else {
          throw new VertxException("decrement value failed.", true);
        }
      } catch (Exception e) {
        throw new VertxException(e, true);
      }
    });
  }

  @Override
  public Future<Long> addAndGet(long value) {
    return add(value, true);
  }

  @Override
  public Future<Long> getAndAdd(long value) {
    return add(value, false);
  }

  private Future<Long> add(long value, boolean post) {
    return vertx.executeBlocking(() -> {
      try {
        long returnValue = 0;
        if (atomicLong.get().succeeded()) returnValue = atomicLong.get().preValue();
        if (atomicLong.add(value).succeeded()) {
          return post ? atomicLong.get().postValue() : returnValue;
        } else {
          throw new VertxException("add value failed.", true);
        }
      } catch (Exception e) {
        throw new VertxException(e, true);
      }
    });
  }

  @Override
  public Future<Boolean> compareAndSet(long expected, long value) {
    return vertx.executeBlocking(() -> {
      try {
        if (atomicLong.get().succeeded() && atomicLong.get().preValue() == 0) this.atomicLong.initialize(0L);
        return atomicLong.compareAndSet(expected, value).succeeded();
      } catch (Exception e) {
        throw new VertxException(e, true);
      }
    });
  }
}
