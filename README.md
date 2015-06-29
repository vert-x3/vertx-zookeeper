# Zookeeper Vert.x Cluster Manager
Using zookeeper as vert.x cluster manager.

### How to work
you have to package by yourself since we have not yet mirgrate project to vert-x3 organisation.

- execution `-Dmaven.test.skip=true` and copy `vertx-zookeeper-$version/lib/*.jar` into $VERTX_HOME/lib
- make sure you have running zookeeper server.
- put zookeeper.properties into $VERTX_HOME/conf, you can find default-zookeeper.properties as [example](https://github.com/stream1984/vertx-zookeeper/blob/master/src/main/resources/default-zookeeper.properties)
- then run your verticle with cmd `vertx -cluster`

#### zookeeper.properties
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
