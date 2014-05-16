package org.mysamples.wdp;

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
        name = "Simple Mutate WDP for Combinatorial Auctions",
        description = "Computes the revenue for all vertices in the graph previously loaded returning only the leaf vertices"
)
public class AggregateMFGAComputation extends BasicComputation<
        WDPVertexIdWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(AggregateMFGAComputation.class);

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
                aggregate(MAX_AGG, new DoubleWritable(newValue));
                log_info("Vertex " + vertex.getId() + " with value = " + vertex.getValue() +
                        " got newValue = " + newValue);
                vertex.voteToHalt();
            }
        }
        //send message or remove vertex
        if (isVertexHalted )
        {
            removeVertexRequest(vertex.getId());
            log_info("Remove Vertex Request: " + vertex.getId()   );
        }
        else
        {
            //send message
            sendMessageToAllEdges(vertex, new DoubleWritable(revenue));
            log_info("Vertex " + vertex.getId() + " sending message to all edges Revenue = " + revenue);
        }
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
