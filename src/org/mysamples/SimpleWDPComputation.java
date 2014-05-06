package org.mysamples;


import org.apache.giraph.Algorithm;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Computes the revenue for the winner determination problem (WDP) in Combinatorial Auctions
 */
@Algorithm(
        name = "Simple WDP for Combinatorial Auctions",
        description = "Computes the revenue for all vertexes in the graph previously loaded"
)
public class SimpleWDPComputation extends BasicComputation<
        LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(SimpleWDPComputation.class);

    public void log_debug(String message){
        if (LOG.isDebugEnabled()) {
            LOG.debug(message);
        }
    }

    @Override
    public void compute(
            Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {

        double revenue = 0;
        if (getSuperstep() == 0) {
            //send initial value
            revenue= vertex.getValue().get();
        }
        else
        {
            //send received value Validate messages.size()<=1
            for (DoubleWritable message : messages) {
                revenue = message.get();
            }

            //set vertex value
            double newValue=vertex.getValue().get()+revenue;
            vertex.setValue(new DoubleWritable(newValue));
            log_debug("Vertex " + vertex.getId() + " with value = " + vertex.getValue() +
                        " got newValue = " + newValue);
        }

        //send message
        sendMessageToAllEdges(vertex, new DoubleWritable(revenue));
        log_debug("Vertex " + vertex.getId() + " sending message to all edges Revenue = " + revenue);
        vertex.voteToHalt();
    }
}
