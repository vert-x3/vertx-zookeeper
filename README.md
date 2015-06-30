# Zookeeper Vert.x Cluster Manager
Using zookeeper as vert.x cluster manager, implements interfaces of vert.x cluster totally.
So you can using it to instead of vertx-hazelcast if you wanna.  
  
In Vert.x a cluster manager is used for various functions including:
- Discovery and group membership of Vert.x nodes in a cluster
- Maintaining cluster wide topic subscriber lists (so we know which nodes are interested in which event bus addresses)
- Distributed Map support
- Distributed Locks
- Distributed Counters  

Cluster managers `do not` handle the event bus inter-node transport, this is done directly by Vert.x with TCP connections.

## How to work
We using [curator](http://curator.apache.org/) framework rather than zookeeper client directly, so  
you would find we also dependency library that curator's such as `guava`, `slf4j` and of course `zookeeper`.  

Since ZK using tree dictionary to store data, we can take root path as namespace default root path is `io.vertx` which in default-zookeeper.properties.  
and there are another 5 sub path to record other information for functions in vert.x cluster manager, all you can change the path is `root path`.  

you can find all the vert.x node information in path of `/io.vertx/cluster/nodes/`,
`/io.vertx/asyncMap/$name/` record all the `AsyncMap` you created with `io.vertx.core.shareddata.AsyncMap` interface.
`/io.vertx/asyncMultiMap/$name/` record all the `AsyncMultiMap` you created with `io.vertx.core.spi.cluster.AsyncMultiMap` interface.
`/io.vertx/locks/` record distributed Locks information.  
`/io.vertx/counters/` record distributed Count information.  

### EventBus in Zookeeper
we could also find how many `EventBus` register with an eventbus address in ZK path`io.vertx/asyncMultiMap/subs/$busAddress/`, so we have view that could 
look up all eventbus with it address through zk cli or WebUI.

#### Using this cluster manager
If you are using Vert.x from the command line, the jar corresponding to this cluster manager 
(it will be named `vertx-zookeeper-${version}-shaded.jar` should be in the lib directory of the Vert.x installation.
you can also put all the jar (with out shaded) into `$VERTX_HOME/lib` by yourself with following step.

- execution `mvn package -Dmaven.test.skip=true` and copy `target/vertx-zookeeper-$version/lib/*.jar` or just only `target/`vertx-zookeeper-${version}-shaded.jar` into $VERTX_HOME/lib
- change the value of `-Dvertx.clusterManagerFactory=` to `io.vertx.spi.cluster.impl.zookeeper.ZookeeperClusterManager` in`$VERTX_HOME/bin/vertx`
- make sure you have running zookeeper server.
- put zookeeper.properties into $VERTX_HOME/conf, you can find default-zookeeper.properties as [example](https://github.com/stream1984/vertx-zookeeper/blob/master/src/main/resources/default-zookeeper.properties)
- then run your verticle with cmd `vertx -cluster`

If you want clustering with this cluster manager in your Vert.x Maven or Gradle project then just add a dependency to 
the artifact: io.vertx:vertx-zookeeper:${version}:shaded in your project.  

You can also specify the cluster manager programmatically if you are embedding Vert.x by specifying it on the options when you are creating your Vert.x instance, 
for example:
```java
Properties zkConfig = new Properties();
zkConfig.setProperty("hosts.zookeeper", "127.0.0.1");
zkConfig.setProperty("path.root", "io.vertx");
zkConfig.setProperty("retry.initialSleepTime", "1000");
zkConfig.setProperty("retry.intervalTimes", "3");

ClusterManager mgr = new ZookeeperClusterManager(zkConfig);
VertxOptions options = new VertxOptions().setClusterManager(mgr);

Vertx.clusteredVertx(options, res -> {
  if (res.succeeded()) {
    Vertx vertx = res.result();
  } else {
    // failed!
  }
});
```

#### Configuring this cluster manager
```properties
#Set the list of servers to connect to zookeeper servers
hosts.zookeeper=127.0.0.1

#session timeout (ms) with zookeeper server
timeout.session = 20000

#connect timeout (ms) to zookeeper server
timeout.connect = 3000`

#As ZooKeeper is a shared space, users of a given cluster should stay within a pre-defined namespace
path.root=io.vertx

#initial amount of time (ms) to wait between retries while lost connect to zookeeper
retry.initialSleepTime=100

#max time in ms to sleep on each retry while lost connect to zookeeper
retry.intervalTimes=10000

#max number of times to retry after lost connect to zookeeper
retry.maxTimes=5
```

* `path.root` is very useful, you can isolate your vertx cluster by change the value of `path.root`, such as dev env, uat env etc...
