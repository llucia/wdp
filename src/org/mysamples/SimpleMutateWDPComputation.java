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
        name = "Simple Mutate WDP for Combinatorial Auctions",
        description = "Computes the revenue for all vertices in the graph previously loaded returning only the leaf vertices"
)
public class SimpleMutateWDPComputation extends BasicComputation<
        LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(SimpleMutateWDPComputation.class);

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
        boolean isVertexHalted=true;
        if (getSuperstep() == 0) {
            revenue= vertex.getValue().get();
            isVertexHalted=false;
        }
        else
        {
            //send received value Validate messages.size()<=1
            for (DoubleWritable message : messages) {
                revenue = message.get();
                isVertexHalted=false;
            }
            //set vertex value if vertex is a roof
            if(vertex.getNumEdges()==0 && !isVertexHalted)
            {
                double newValue=vertex.getValue().get()+revenue;
                vertex.setValue(new DoubleWritable(newValue));
                log_debug("Vertex " + vertex.getId() + " with value = " + vertex.getValue() +
                            " got newValue = " + newValue);
                vertex.voteToHalt();
            }
        }
        //send message or remove vertex
        if (isVertexHalted )
        {
            removeVertexRequest(vertex.getId());
            log_debug("Remove Vertex Request: " + vertex.getId()   );
        }
        else
        {
            //send message
            sendMessageToAllEdges(vertex, new DoubleWritable(revenue));
            log_debug("Vertex " + vertex.getId() + " sending message to all edges Revenue = " + revenue);
        }
    }
}
