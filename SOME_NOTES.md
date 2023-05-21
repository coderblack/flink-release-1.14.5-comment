```shell
# 项目整体编译
mvn clean install -DskipTests -Dfast


# 项目内运行时，akka加载问题
mvn package -pl flink-rpc/flink-rpc-akka,flink-rpc/flink-rpc-akka-loader
```
