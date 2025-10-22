start-dfs.sh
cd hbase-2.0.5
bin/start-hbase.sh
jps

cd apache-zookeeper-3.7.2-bin
bin/zkServer.sh start

bin/hbase-daemon.sh start master
bin/hbase-daemon.sh start regionserver

bin/hbase shell
create 'users', 'info'

start-dfs.sh
start-yarn.sh
bin/beeline -n db_user -u jdbc:hive2://localhost:10000






