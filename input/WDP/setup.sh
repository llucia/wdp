: '
hadoop dfs -rmr /user/hduser/output/wdp/SimpleWDPComputation/sample1

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner -Dgiraph.graphPartitionerFactoryClass=org.apache.giraph.partition.SimpleLongRangePartitionerFactory -Dgiraph.vertexKeySpaceSize=15 org.mysamples.SimpleWDPComputation -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat -vip /user/hduser/input/wdp/sample1.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/SimpleWDPComputation/sample1  -w 2

hadoop dfs -rmr /user/hduser/output/wdp/SimpleMutateWDPComputation/sample1

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.SimpleMutateWDPComputation -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat -vip /user/hduser/input/wdp/sample1.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/SimpleMutateWDPComputation/sample1  -w 2


hadoop dfs -rmr /user/hduser/output/wdp/DynamicWDPComputation/sample1

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.DynamicWDPComputation -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat -vip /user/hduser/input/wdp/DynamicWDPComputation/sample1.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/DynamicWDPComputation/sample1  -w 1


hadoop dfs -rmr /user/hduser/output/wdp/DynamicWDPComputation/sample1

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.TestingMutation -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat -vip /user/hduser/input/wdp/DynamicWDPComputation/sample1.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/DynamicWDPComputation/sample1  -w 1


hadoop dfs -rmr /user/hduser/output/wdp/TestingMutation/sample1

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.TestingMutation -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat -vip /user/hduser/input/wdp/sample3.txt -vof org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexOutputFormat -op /user/hduser/output/wdp/TestingMutation/sample1  -w 1

hadoop dfs -rmr /user/hduser/output/wdp/DynamicWDPComputation/sample3

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.DynamicWDPComputation -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat -vip /user/hduser/input/wdp/sample3.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/DynamicWDPComputation/sample3  -w 1

hadoop dfs -rmr /user/hduser/output/wdp/HelloWorldComputation/sample3

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.HelloWorldComputation -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat -vip /user/hduser/input/wdp/sample3.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/HelloWorldComputation/sample3  -w 1

hadoop dfs -rmr /user/hduser/output/wdp/WDPFormatTestComputation/sample1

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.WDPFormatTestComputation -vif org.mysamples.io.formats.JsonWDPVertexIdDoubleFloatWDPMessageVertexInputFormat -vip /user/hduser/input/wdp/sample4.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/WDPFormatTestComputation/sample1  -w 1


hadoop dfs -rmr /user/hduser/output/wdp/WDPMessageFormatTestComputation/sample1

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.WDPMessageFormatTestComputation -vif org.mysamples.io.formats.JsonWDPVertexIdDoubleFloatWDPMessageVertexInputFormat -vip /user/hduser/input/wdp/sample4.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/WDPMessageFormatTestComputation/sample1  -w 1


hadoop dfs -copyFromLocal /home/user/Desktop/giraph/WDP/sample5.txt /user/hduser/input/wdp/sample5.txt



hadoop dfs -rmr /user/hduser/output/wdp/core/DynamicWDPComputation/sample2

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.wdp.DynamicWDPComputation -vif org.mysamples.io.formats.JsonWDPVertexIdDoubleFloatWDPMessageVertexInputFormat -vip /user/hduser/input/wdp/sample1.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/core/DynamicWDPComputation/sample2  -w 1

hadoop dfs -rmr /user/hduser/output/wdp/core/DynamicWDPComputation/sample5

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.wdp.DynamicWDPComputation -vif org.mysamples.io.formats.JsonWDPVertexIdDoubleFloatWDPMessageVertexInputFormat -vip /user/hduser/input/wdp/sample5.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/core/DynamicWDPComputation/sample5  -w 1

hadoop dfs -rmr /user/hduser/output/wdp/TestAddRequestComputation/test

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.TestAddRequestComputation -vif org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat -vip /user/hduser/input/wdp/test.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/TestAddRequestComputation/test  -w 1

hadoop dfs -rmr /user/hduser/output/wdp/core/DynamicWDPComputation/sample6

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.wdp.DynamicWDPComputation -vif org.mysamples.io.formats.JsonWDPVertexIdDoubleFloatWDPMessageVertexInputFormat -vip /user/hduser/input/wdp/sample5.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/core/DynamicWDPComputation/sample6  -w 1

hadoop dfs -rmr /user/hduser/output/wdp/core/DynamicWDPComputation/problem2

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.wdp.DynamicWDPComputation -vif org.mysamples.io.formats.JsonWDPVertexIdDoubleFloatWDPMessageVertexInputFormat -vip /user/hduser/input/wdp/problem2.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/core/DynamicWDPComputation/problem2  -w 1
'

hadoop dfs -rmr /user/hduser/output/wdp/core/DynamicWDPComputation/problem4

hadoop jar /home/user/IdeaProjects/GiraphJobs/out/artifacts/GiraphJobs_jar/GiraphJobs.jar  org.apache.giraph.GiraphRunner org.mysamples.wdp.DynamicWDPComputation -vif org.mysamples.io.formats.JsonWDPVertexIdDoubleFloatWDPMessageVertexInputFormat -vip /user/hduser/input/wdp/problem4.txt -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat -op /user/hduser/output/wdp/core/DynamicWDPComputation/problem4  -w 1


