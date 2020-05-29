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

import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.shareddata.Lock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;

import java.util.concurrent.ExecutorService;

/**
 * Lock implementation.
 */
public class ZKLock implements Lock {

  private static final Logger log = LoggerFactory.getLogger(ZKLock.class);

  private final InterProcessSemaphoreMutex lock;
  private final ExecutorService lockReleaseExec;

  public ZKLock(InterProcessSemaphoreMutex lock, ExecutorService lockReleaseExec) {
    this.lock = lock;
    this.lockReleaseExec = lockReleaseExec;
  }

  public InterProcessSemaphoreMutex getLock() {
    return lock;
  }

  @Override
  public void release() {
    lockReleaseExec.execute(() -> {
      try {
        lock.release();
      } catch (Exception e) {
        log.error(e);
      }
    });
  }
}
