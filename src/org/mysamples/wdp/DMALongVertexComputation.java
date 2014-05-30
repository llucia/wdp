package org.mysamples.wdp;

/**
 * Created by user on 4/27/14.
 */

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;
import org.mysamples.io.types.WDPMessage;
import org.mysamples.io.types.WDPMessageWritable;

import java.io.IOException;

/**
 * Computes the revenue for the winner determination problem (WDP) in Combinatorial Auctions
 */
@Algorithm(
        name = "Dynamic WDP for Combinatorial Auctions",
        description = "Computes the revenue for all vertices in the graph creating new vertices dynamically"
)
public class DMALongVertexComputation extends BasicComputation<
        LongWritable, WDPMessageWritable, NullWritable, WDPMessageWritable> {

    /**
     * Class logger
     */
    private static final Logger LOG =
            Logger.getLogger(DMALongVertexComputation.class);

    public void log_info(Vertex<LongWritable, WDPMessageWritable, NullWritable>  vertex) {
        if (LOG.isInfoEnabled()) {
            System.out.println("Superstep: " + getSuperstep() + " TotalNumVertices: " + getTotalNumVertices() + " TotalNumEdges: " + getTotalNumEdges());
            System.out.println("Processing Vertex " + vertex.getId() + " Value= " + vertex.getValue() + " NumEdges: " + vertex.getNumEdges() + " to: ");
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                System.out.println(edge.getTargetVertexId() + ", ");
            }

        }
    }
    public void log_info(String msg) {
        if (LOG.isInfoEnabled()) {
            System.out.println(msg);
        }
    }


    @Override
    public void compute(Vertex<LongWritable, WDPMessageWritable, NullWritable> vertex,
                        Iterable<WDPMessageWritable> messages) throws IOException {
        log_info(vertex);
        if (getSuperstep() == 0) {
            sendMessageToAllEdges(vertex, vertex.getValue());
        }
        else {
            if (getSuperstep() == 1) {
                long msgNum = 0;
                for (WDPMessageWritable message : messages) {
                    msgNum++;
                    break;
                }
                if(msgNum==0)
                {
                    sendMessageToAllEdges(vertex,vertex.getValue());
                    RemoveVertex(vertex);
                }
                else
                    vertex.voteToHalt();

            } else {
                WDPMessageWritable maxMsg=vertex.getValue();
                WDPMessageWritable temp;
                for (WDPMessageWritable message : messages) {
                    if (vertex.getValue().get().getVertexId().IsNotConflictingWith(message.get().getVertexId())) {
                        temp=new WDPMessageWritable(new WDPMessage(vertex.getValue().get().getValue()+message.get().getValue(),message.get().getVertexId().Concat(vertex.getValue().get().getVertexId())));
                        if (vertex.getNumEdges() == 0) {
                            if (maxMsg.get().getValue() <temp.get().getValue())
                                maxMsg = temp;
                        }
                        else {
                            sendMessageToAllEdges(vertex,temp );
                           }
                        aggregate(MAX_AGG, new DoubleWritable(temp.get().getValue()));
                    }
                }

                if (vertex.getNumEdges() == 0) {
                    vertex.setValue(maxMsg);
                    vertex.voteToHalt();
                }
                else
                       RemoveVertex(vertex);

            }
        }
    }



    private void RemoveVertex(Vertex<LongWritable, WDPMessageWritable, NullWritable> vertex) throws IOException {
        removeVertexRequest(vertex.getId());
        log_info("removeVertexRequest "+vertex);

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
        @Override
        public void  compute(){
                System.out.println(getAggregatedValue(MAX_AGG));
        }
    }
}