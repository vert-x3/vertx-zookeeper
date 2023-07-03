package io.vertx.spi.cluster.zookeeper;

import io.vertx.core.json.JsonObject;
import io.vertx.spi.cluster.zookeeper.impl.RetryPolicyHelper;
import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryOneTime;
import org.junit.Assert;
import org.junit.Test;

public class RetryPolicyTest {

  @Test
  public void createDefaultRetryPolicy(){
    JsonObject config = new JsonObject();
    RetryPolicy policy = RetryPolicyHelper.createRetryPolicy(config);
    Assert.assertTrue(policy instanceof ExponentialBackoffRetry);
  }

  @Test
  public void createOneTimeRetryPolicy(){
    JsonObject config = new JsonObject().put("policy","one_time");
    RetryPolicy policy = RetryPolicyHelper.createRetryPolicy(config);
    Assert.assertTrue(policy instanceof RetryOneTime);
  }

}
