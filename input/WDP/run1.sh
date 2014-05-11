hadoop dfs -rmr /user/hduser/input/wdp/sample1.txt
hadoop dfs -rmr /user/hduser/output/wdp/sample2

hadoop dfs -copyFromLocal '/home/user/Desktop/giraph/WDP/sample1.txt' /user/hduser/input/wdp/sample1.txt

hadoop jar $GIRAPH_HOME/giraph-examples/target/giraph-examples-1.1.0-SNAPSHOT-for-hadoop-1.2.1-jar-with-dependencies.jar org.apache.giraph.GiraphRunner  -Dgiraph.graphPartitionerFactoryClass=org.apache.giraph.partition.SimpleLongRangePartitionerFactory -Dgiraph.vertexKeySpaceSize=15   org.apache.giraph.examples.SimpleWDPComputation -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat -vip /user/hduser/input/wdp/sample1.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/sample2  -w 2

