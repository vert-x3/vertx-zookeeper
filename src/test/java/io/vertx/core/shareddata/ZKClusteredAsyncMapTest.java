/*
 * Copyright 2018 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.vertx.core.shareddata;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.zookeeper.MockZKCluster;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Objects;

/**
 *
 */
public class ZKClusteredAsyncMapTest extends io.vertx.tests.shareddata.ClusteredAsyncMapTest {

  private MockZKCluster zkClustered = new MockZKCluster();

  public void after() throws Exception {
    super.after();
    zkClustered.stop();
  }

  @Test
  @Override
  public void testMapReplaceIfPresentTtl() {
    getVertx().sharedData().<String, String>getAsyncMap("foo").onComplete(onSuccess(map -> {
        map.replaceIfPresent("key", "old", "new", 100)
          .onSuccess(b -> fail("operation should not be implemented"))
          .onFailure(t -> {
            assertTrue("operation not implemented", t instanceof UnsupportedOperationException);
            complete();
          });
      }));
    await();
  }

  @Test
  @Override
  public void testMapReplaceIfPresentTtlWhenNotPresent() {
    getVertx().sharedData().<String, String>getAsyncMap("foo").onComplete(onSuccess(map -> {
        map.replaceIfPresent("key", "old", "new", 100)
          .onSuccess(b -> fail("operation should not be implemented"))
          .onFailure(t -> {
            assertTrue("operation not implemented", t instanceof UnsupportedOperationException);
            complete();
          });
      }));
    await();
  }

  @Test
  @Override
  public void testMapReplaceTtl() {
    getVertx().sharedData().<String, String>getAsyncMap("foo").onComplete(onSuccess(map -> {
        map.replace("key", "new", 100)
          .onSuccess(b -> fail("operation should not be implemented"))
          .onFailure(t -> {
            assertTrue("operation not implemented", t instanceof UnsupportedOperationException);
            complete();
          });
      }));
    await();
  }

  @Test
  @Override
  public void testMapReplaceTtlWithPreviousValue() {
    getVertx().sharedData().<String, String>getAsyncMap("foo").onComplete(onSuccess(map -> {
        map.replace("key", "new", 100)
          .onSuccess(b -> fail("operation should not be implemented"))
          .onFailure(t -> {
            assertTrue("operation not implemented", t instanceof UnsupportedOperationException);
            complete();
          });
      }));
    await();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return zkClustered.getClusterManager();
  }

  @Test
  public void testStoreAndGetBuffer() {
    getVertx().sharedData().<String, Buffer>getAsyncMap("foo").onComplete(onSuccess(map -> {
      map.put("test", Buffer.buffer().appendString("Hello")).onComplete(onSuccess(putResult -> map.get("test").onComplete(onSuccess(myBuffer -> {
        assertEquals("Hello", myBuffer.toString());
        testComplete();
      }))));
    }));
    await();
  }

  @Ignore
  @Override
  public void testMapPutThenPutTtl() {
    // This test fails upstream, the test is doing:

    // 1. get the async map: foo
    // 2. store the value "molo" under the key "pipo"
    // 3. store the value "mili" under the key "pipo" with TTL 15
    // 4. get the async map: foo
    // N. every 15, check if key "pipo" is NULL <-- THIS NEVER HAPPENS
  }
}
