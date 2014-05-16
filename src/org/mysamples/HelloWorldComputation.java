package org.mysamples;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * Created by user on 4/26/14.
 */

/**
 * Computes the revenue for the winner determination problem (WDP) in Combinatorial Auctions
 */
@Algorithm(
        name = "Simple Mutate WDP for Combinatorial Auctions",
        description = "Computes the revenue for all vertices in the graph previously loaded returning only the leaf vertices"
)
public class HelloWorldComputation extends BasicComputation<
        LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(HelloWorldComputation.class);

    public void log_debug(String message){
        if (LOG.isDebugEnabled()) {
            LOG.debug(message);
        }
    }
    public void log_info(String message){
        if (LOG.isInfoEnabled()) {
            LOG.info(message);
        }
    }

    /** Max aggregator name */
    private static String MAX_AGG = "max";


    @Override
    public void compute(
            Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {
        double vertexValue = vertex.getValue().get();
        aggregate(MAX_AGG, new DoubleWritable(vertexValue));

        log_info("Compute function in VertexId: "+ vertex.getId().get()+" VertexValue: "+vertexValue);
        if (getSuperstep() == 0) {
            sendMessageToAllEdges(vertex,  new DoubleWritable(vertexValue));
        }
      log_info("liannet aggregator "+getAggregatedValue(MAX_AGG));
        System.out.print("liannet aggregator "+getAggregatedValue(MAX_AGG));

        vertex.voteToHalt();
    }


    /**
     * Master compute associated with {@link AggregatorMasterCompute}.
     * It registers required aggregators.
     */
    public static class AggregatorMasterCompute extends
            DefaultMasterCompute {
        @Override
        public void initialize() throws InstantiationException,
                IllegalAccessException {
            registerPersistentAggregator(MAX_AGG, DoubleMaxAggregator.class);
        }
    }
}

