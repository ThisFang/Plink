#!/bin/sh

cd /
./usr/local/opt/flink-1.6.0/bin/stop-cluster.sh

# cd /tmp
# rm -rf blobStore-*
# rm -rf flink-*
# rm -rf executionGraphStore-*
# rm -rf jaas-*
# rm -rf jffi*

cd /
./usr/local/opt/flink-1.6.0/bin/start-cluster.sh
