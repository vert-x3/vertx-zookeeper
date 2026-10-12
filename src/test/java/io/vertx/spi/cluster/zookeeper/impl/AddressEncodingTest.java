/*
 *  Copyright (c) 2011-2026 The original author or authors
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

import org.apache.zookeeper.common.PathUtils;
import org.junit.Test;

import static io.vertx.spi.cluster.zookeeper.impl.SubsMapHelper.decodeAddress;
import static io.vertx.spi.cluster.zookeeper.impl.SubsMapHelper.encodeAddress;
import static org.junit.Assert.assertEquals;

public class AddressEncodingTest {

  @Test
  public void testPlainAddressIsUnchanged() {
    for (String address : new String[]{"foo", "foo.bar", "__vertx.reply.123", "foo-bar_baz*", "..."}) {
      assertEquals(address, encodeAddress(address));
      assertEquals(address, decodeAddress(address));
    }
  }

  @Test
  public void testEncoding() {
    assertEquals("foo%2Fbar", encodeAddress("foo/bar"));
    assertEquals("100%25", encodeAddress("100%"));
    assertEquals("%2E", encodeAddress("."));
    assertEquals("%2E%2E", encodeAddress(".."));
    assertEquals("%F0%9F%98%80", encodeAddress("\uD83D\uDE00"));
  }

  @Test
  public void testRoundTrip() {
    for (String address : new String[]{"foo/bar", "/", "//", "100%", "%2F", ".", "..", "a\u0000b", "\u0001\u001F\u007F\u009F",
      "\uE000", "\uFFF0\uFFFF", "\uD83D\uDE00/x", "héllo/wörld", "a b+c", "a%20b", "a:b"}) {
      String encoded = encodeAddress(address);
      PathUtils.validatePath("/__vertx.subs/" + encoded);
      assertEquals(address, decodeAddress(encoded));
    }
  }
}
