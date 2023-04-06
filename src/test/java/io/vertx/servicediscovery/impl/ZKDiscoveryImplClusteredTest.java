/*
 * Copyright (c) 2011-2020 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *      The Eclipse Public License is available at
 *      http://www.eclipse.org/legal/epl-v10.html
 *
 *      The Apache License v2.0 is available at
 *      http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.vertx.servicediscovery.impl;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;
import org.junit.After;
import org.junit.Before;

import static com.jayway.awaitility.Awaitility.await;

public class ZKDiscoveryImplClusteredTest extends DiscoveryImplTestBase {

  private MockZKCluster zkClustered = new MockZKCluster();

  @Before
  public void setUp() {
    VertxOptions options = new VertxOptions().setClusterManager(zkClustered.getClusterManager());
    options.getEventBusOptions().setHost("localhost").setPort(0);
    Vertx.clusteredVertx(options).onComplete(ar -> {
      vertx = ar.result();
    });
    await().until(() -> vertx != null);
    discovery = new DiscoveryImpl(vertx, new ServiceDiscoveryOptions());
  }

  @After
  public void tearDown() {
    super.tearDown();
    zkClustered.stop();
  }
}
