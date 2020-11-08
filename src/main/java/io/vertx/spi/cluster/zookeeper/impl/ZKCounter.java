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

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.VertxException;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.shareddata.Counter;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;

import java.util.Objects;

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
    return vertx.executeBlocking(future -> {
      try {
        future.complete(atomicLong.get().preValue());
      } catch (Exception e) {
        future.fail(new VertxException(e));
      }
    });
  }

  @Override
  public void get(Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    get().onComplete(resultHandler);
  }

  @Override
  public Future<Long> incrementAndGet() {
    return increment(true);
  }

  @Override
  public void incrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    incrementAndGet().onComplete(resultHandler);
  }

  @Override
  public Future<Long> getAndIncrement() {
    return increment(false);
  }

  @Override
  public void getAndIncrement(Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    getAndIncrement().onComplete(resultHandler);
  }

  private Future<Long> increment(boolean post) {
    return vertx.executeBlocking(future -> {
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
    });
  }

  @Override
  public Future<Long> decrementAndGet() {
    return vertx.executeBlocking(future -> {
      try {
        if (atomicLong.decrement().succeeded()) {
          future.complete(atomicLong.get().postValue());
        } else {
          future.fail(new VertxException("decrement value failed."));
        }
      } catch (Exception e) {
        future.fail(new VertxException(e));
      }
    });
  }

  @Override
  public void decrementAndGet(Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    decrementAndGet().onComplete(resultHandler);
  }

  @Override
  public Future<Long> addAndGet(long value) {
    return add(value, true);
  }

  @Override
  public void addAndGet(long value, Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    addAndGet(value).onComplete(resultHandler);
  }

  @Override
  public Future<Long> getAndAdd(long value) {
    return add(value, false);
  }

  @Override
  public void getAndAdd(long value, Handler<AsyncResult<Long>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    getAndAdd(value).onComplete(resultHandler);
  }

  private Future<Long> add(long value, boolean post) {
    return vertx.executeBlocking(future -> {
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
    });
  }

  @Override
  public Future<Boolean> compareAndSet(long expected, long value) {
    return vertx.executeBlocking(future -> {
      try {
        if (atomicLong.get().succeeded() && atomicLong.get().preValue() == 0) this.atomicLong.initialize(0L);
        future.complete(atomicLong.compareAndSet(expected, value).succeeded());
      } catch (Exception e) {
        future.fail(new VertxException(e));
      }
    });
  }

  @Override
  public void compareAndSet(long expected, long value, Handler<AsyncResult<Boolean>> resultHandler) {
    Objects.requireNonNull(resultHandler);
    compareAndSet(expected, value).onComplete(resultHandler);
  }
}
