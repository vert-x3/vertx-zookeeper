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
import java.util.Random;

import io.vertx.core.Promise;
import io.vertx.core.VertxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cluster manager that uses Zookeeper with retry logic in case of failure to attempt to join the cluster.
 *
 * @author Anvesh Mora
 */
public class DefaultZookeeperClusterManager extends ZookeeperClusterManager {
    private static final Logger logger = LoggerFactory.getLogger(DefaultZookeeperClusterManager.class);
    private static final int    MAX_RETRIES   = 5;
    private static final int    MIN_WAIT_TIME = 1_000;
    private              int    retryCount    = 0;
    
    public void join(Promise<Void> promise) {
        logger.info("Attempting to join the Zookeeper cluster...");
        super.join(promise);

        promise.future().onSuccess(v -> {
        logger.info("Successfully joined the Zookeeper cluster.");
        retryCount = 0; // Reset retry count on success
        }).onFailure(cause -> {
        logger.warn("Initial attempt to join Zookeeper cluster failed: {}", cause.getMessage(), cause);

        if (retryCount < MAX_RETRIES) {
            retryWithExponentialBackoffAndJitter(promise);
        } else {
            logger.error("Exceeded maximum retries ({}) to connect to Zookeeper. Throwing exception.", MAX_RETRIES);
            throw new VertxException("Failed to connect to Zookeeper after " + MAX_RETRIES + " retries", cause);
        }
        });
    }
    
    private void retryWithExponentialBackoffAndJitter(Promise<Void> promise) {
        int    waitTime = MIN_WAIT_TIME;
        Random random   = new Random();
        while (retryCount < MAX_RETRIES) {
        try {
            logger.warn("Retry attempt {} to join Zookeeper cluster...", retryCount);
            super.join(promise);
            promise.future().onSuccess(v -> {
            logger.info("Successfully joined the Zookeeper cluster on retry attempt {}.", retryCount);
            retryCount = 0; // Reset retry count upon success
            }).onFailure(cause -> {
            logger.warn("Retry attempt {} failed: {}. Will retry up to {} times.", retryCount, cause.getMessage(), MAX_RETRIES, cause);
            });
            break;
        } catch (Exception ex) {
            logger.error("Exception during retry attempt {}: {}", retryCount, ex.getMessage(), ex);
            try {
            // Add jitter by introducing a random factor (up to 1000ms extra)
            waitTime = waitTime + random.nextInt(1_000);
            logger.warn("Waiting {}ms before next retry...", waitTime);
            Thread.sleep(waitTime);
            waitTime *= 2; // Double the wait time for the next iteration
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Thread interrupted during retry sleep. Aborting retries.", e);
                throw new VertxException("Internal thread error when trying to join Zookeeper cluster", e);
            }
        } finally{
            retryCount++;
        }
        }
    }
}