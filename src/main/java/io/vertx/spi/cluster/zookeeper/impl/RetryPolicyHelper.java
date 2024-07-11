package io.vertx.spi.cluster.zookeeper.impl;

import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.*;

public class RetryPolicyHelper {
  private static final String DEFAULT_RETRY_POLICY = "exponential_backoff";
  private static final Logger log = LoggerFactory.getLogger(RetryPolicyHelper.class);


  /**
   * creates a {@link RetryPolicy} based on a JsonObject configuration.
   * It falls back to {@link ExponentialBackoffRetry} if no valid policy is specified.
   *
   * @param conf the configuration object
   * @return RetryPolicy instance
   */
  public static RetryPolicy createRetryPolicy(JsonObject conf){
    String policy = conf.getString("policy", DEFAULT_RETRY_POLICY);
    int initialSleepTime = conf.getInteger("initialSleepTime", 1000);
    int maxTimes = conf.getInteger("maxTimes", 5);
    int intervalTimes = conf.getInteger("intervalTimes",10000);

    switch (policy){
      case "bounded_exponential_backoff": return new BoundedExponentialBackoffRetry(initialSleepTime,maxTimes,intervalTimes);
      case "one_time": return new RetryOneTime(intervalTimes);
      case "n_times": return new RetryNTimes(maxTimes, intervalTimes);
      case "forever": return new RetryForever(intervalTimes);
      case "until_elapsed": return new RetryUntilElapsed(maxTimes,intervalTimes);
      case DEFAULT_RETRY_POLICY: return new ExponentialBackoffRetry(initialSleepTime, maxTimes, intervalTimes);
      default: {
        log.warn(String.format("%s is not a valid policy, falling back to %s",policy, DEFAULT_RETRY_POLICY));
        return new ExponentialBackoffRetry(initialSleepTime, maxTimes, intervalTimes);
      }
    }
  }

  private RetryPolicyHelper() {
    //Utility class
  }
}
