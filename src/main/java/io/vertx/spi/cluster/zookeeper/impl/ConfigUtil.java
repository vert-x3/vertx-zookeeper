/*
 *  Copyright (c) 2011-2023 The original author or authors
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

import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ConfigUtil {

  private static final Logger log = LoggerFactory.getLogger(ConfigUtil.class);

  private static final String DEFAULT_CONFIG_FILE = "default-zookeeper.json";
  private static final String CONFIG_FILE = "zookeeper.json";
  private static final String ZK_SYS_CONFIG_KEY = "vertx.zookeeper.config";

  public static JsonObject loadConfig(String resourceLocation) {
    JsonObject conf = null;
    try (
      InputStream is = getConfigStream(resourceLocation != null ? resourceLocation : System.getProperty(ZK_SYS_CONFIG_KEY));
      BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(is)))
    ) {
      String line;
      StringBuilder sb = new StringBuilder();
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      conf = new JsonObject(sb.toString());
    } catch (IOException ex) {
      log.error("Failed to read config", ex);
    }
    return conf;
  }

  private static InputStream getConfigStream(String resourceLocation) {
    InputStream is = getConfigStreamFor(resourceLocation);
    if (is == null) {
      is = getConfigStreamFromClasspath(CONFIG_FILE, DEFAULT_CONFIG_FILE);
    }
    return is;
  }

  private static InputStream getConfigStreamFor(String resourceLocation) {
    InputStream is = null;
    if (resourceLocation != null) {
      if (resourceLocation.startsWith("classpath:")) {
        return getConfigStreamFromClasspath(resourceLocation.substring("classpath:".length()), CONFIG_FILE);
      }
      File cfgFile = new File(resourceLocation);
      if (cfgFile.exists()) {
        try {
          is = new FileInputStream(cfgFile);
        } catch (FileNotFoundException ex) {
          log.warn(String.format("Failed to open file '%s' defined in '%s'. Continuing classpath search for %s", resourceLocation, ZK_SYS_CONFIG_KEY, CONFIG_FILE));
        }
      }
    }
    return is;
  }

  private static InputStream getConfigStreamFromClasspath(String configFile, String defaultConfig) {
    InputStream is = null;
    ClassLoader ctxClsLoader = Thread.currentThread().getContextClassLoader();
    if (ctxClsLoader != null) {
      is = ctxClsLoader.getResourceAsStream(configFile);
    }
    if (is == null) {
      is = ConfigUtil.class.getClassLoader().getResourceAsStream(configFile);
      if (is == null) {
        is = ConfigUtil.class.getClassLoader().getResourceAsStream(defaultConfig);
      }
    }
    return is;
  }

  private ConfigUtil() {
    // Utility class
  }
}
