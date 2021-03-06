package org.mysamples;

/**
 * Created by user on 4/27/14.
 */
import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.mysamples.io.types.WDPVertexId;
import org.mysamples.io.types.WDPVertexIdWritable;

import java.io.IOException;


/**
 * Computes the revenue for the winner determination problem (WDP) in Combinatorial Auctions
 */
@Algorithm(
        name = "Simple Mutate WDP for Combinatorial Auctions",
        description = "Computes the revenue for all vertices in the graph previously loaded returning only the leaf vertices"
)
public class WDPFormatTestComputation extends BasicComputation<
        WDPVertexIdWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(WDPFormatTestComputation.class);

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

    @Override
    public void compute(
            Vertex<WDPVertexIdWritable, DoubleWritable, FloatWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {
        double vertexValue = vertex.getValue().get();

        log_info("Compute function in VertexId: "+ vertex.getId().get()+" VertexValue: "+vertexValue);
        if (getSuperstep() == 0) {
            sendMessageToAllEdges(vertex,  new DoubleWritable(vertexValue));
        }
        vertex.voteToHalt();
    }
}


