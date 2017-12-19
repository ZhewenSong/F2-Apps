#!/bin/bash
# Deploy tez after modifying the example
cd /home/ubuntu/F2/f2_tez/tez-examples/
# mvn install -DskipTests
mvn -DskipTests -Dtar -Pdist -Dmaven.javadoc.skip=true package
cp /home/ubuntu/F2/f2_tez/tez-examples/target/tez-examples-0.7.1.jar /home/ubuntu/software/f2_tez/
cp /home/ubuntu/F2/f2_tez/tez-examples/target/tez-examples-0.7.1.jar /home/ubuntu/software/f2_tez-minimal/
cd /home/ubuntu/software/f2_tez/
tar -czvf tez-0.7.1.tar.gz *
hadoop dfs -rmr /apps/tez-0.7.1.tar.gz
hadoop dfs -copyFromLocal tez-0.7.1.tar.gz /apps/


