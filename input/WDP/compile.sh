cp /home/user/Desktop/giraph/WDP/SimpleWDPComputation.java $GIRAPH_HOME/giraph-examples/src/main/java/org/apache/giraph/examples/

cd $GIRAPH_HOME/giraph-examples
mvn package -DskipTests

hadoop dfs -rmr /user/hduser/input/wdp/sample_graph.txt
hadoop dfs -rmr /user/hduser/output/wdp

hadoop dfs -copyFromLocal '/home/user/Desktop/giraph/WDP/sample_graph.txt' /user/hduser/input/wdp/sample_graph.txt



hadoop jar $GIRAPH_HOME/giraph-examples/target/giraph-examples-1.1.0-SNAPSHOT-for-hadoop-1.2.1-jar-with-dependencies.jar org.apache.giraph.GiraphRunner org.apache.giraph.examples.SimpleWDPComputation -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat -vip /user/hduser/input/wdp/sample_graph.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp -w 1
