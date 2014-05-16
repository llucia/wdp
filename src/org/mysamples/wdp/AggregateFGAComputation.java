package org.mysamples.wdp;

/**
 * Created by user on 5/14/14.
 */

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.log4j.Logger;
import org.mysamples.io.types.WDPVertexIdWritable;

import java.io.IOException;

/**
 * Computes the revenue for the winner determination problem (WDP) in Combinatorial Auctions
 */
@Algorithm(
        name = "Simple WDP for Combinatorial Auctions",
        description = "Computes the revenue for all vertexes in the graph previously loaded"
)
public class AggregateFGAComputation extends BasicComputation<
        WDPVertexIdWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(AggregateFGAComputation.class);

    public void log_info(String message){
        if (LOG.isInfoEnabled()) {
            LOG.info(message);
        }
    }
    public void log_info(Vertex<WDPVertexIdWritable, DoubleWritable, FloatWritable>  vertex) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Superstep: " + getSuperstep() + " TotalNumVertices: " + getTotalNumVertices() + " TotalNumEdges: " + getTotalNumEdges());
            LOG.info("Processing Vertex " + vertex.getId() + " Value= " + vertex.getValue() + " NumEdges: " + vertex.getNumEdges()+ " to: ");
            for (Edge<WDPVertexIdWritable, FloatWritable> edge : vertex.getEdges()) {
                LOG.info(edge.getTargetVertexId()+", ");
            }

        }
    }

    @Override
    public void compute(
            Vertex<WDPVertexIdWritable, DoubleWritable, FloatWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {

        log_info(vertex);
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
            aggregate(MAX_AGG, new DoubleWritable(newValue));
            log_info("Vertex " + vertex.getId() + " with value = " + vertex.getValue() +
                    " got newValue = " + newValue);
        }

        //send message
        sendMessageToAllEdges(vertex, new DoubleWritable(revenue));
        log_info("Vertex " + vertex.getId() + " sending message to all edges Revenue = " + revenue);
        vertex.voteToHalt();
        System.out.print(Math.max(((DoubleWritable)getAggregatedValue(MAX_AGG)).get(),vertex.getValue().get())+"; ");

    }

    /** Max aggregator name */
    private static String MAX_AGG = "max";

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
