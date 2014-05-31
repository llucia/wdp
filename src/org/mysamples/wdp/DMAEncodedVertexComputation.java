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
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;
import org.mysamples.io.types.*;

import java.io.IOException;

/**
 * Computes the revenue for the winner determination problem (WDP) in Combinatorial Auctions
 */
@Algorithm(
        name = "Dynamic WDP for Combinatorial Auctions",
        description = "Computes the revenue for all vertices in the graph creating new vertices dynamically"
)
public class DMAEncodedVertexComputation extends BasicComputation<
        EncodedGoodsWritable, EncodedMessageWritable, NullWritable, EncodedMessageWritable> {

    /**
     * Class logger
     */
    private static final Logger LOG =
            Logger.getLogger(DMAEncodedVertexComputation.class);

    public void log_info(Vertex<EncodedGoodsWritable, EncodedMessageWritable, NullWritable>  vertex) {
        if (LOG.isInfoEnabled()) {
            System.out.println("Superstep: " + getSuperstep() + " TotalNumVertices: " + getTotalNumVertices() + " TotalNumEdges: " + getTotalNumEdges());
            System.out.println("Processing Vertex " + vertex.getId() + " Value= " + vertex.getValue() + " NumEdges: " + vertex.getNumEdges() + " to: ");
            for (Edge<EncodedGoodsWritable, NullWritable> edge : vertex.getEdges()) {
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
    public void compute(Vertex<EncodedGoodsWritable, EncodedMessageWritable, NullWritable> vertex,
                        Iterable<EncodedMessageWritable> messages) throws IOException {
        log_info(vertex);
        EncodedMessageWritable vertexValue=new EncodedMessageWritable(new EncodedMessage(vertex.getValue().get().getValue(),vertex.getId().get()));
        if (getSuperstep() == 0) {
            sendMessageToAllEdges(vertex, vertexValue);
        }
        else {
            if (getSuperstep() == 1) {
                long msgNum = 0;
                for (EncodedMessageWritable message : messages) {
                    msgNum++;
                    break;
                }
                if(msgNum==0)
                {
                    sendMessageToNonConflictingEdges(vertex, vertexValue);
                    RemoveVertex(vertex);
                }
                else
                    vertex.voteToHalt();

            } else {
                EncodedMessageWritable maxMsg=vertexValue;
                EncodedMessageWritable temp;
                for (EncodedMessageWritable message : messages) {
                        temp=new EncodedMessageWritable(new EncodedMessage(vertex.getValue().get().getValue()+message.get().getValue(),message.get().getEncodedGoods().Concat(vertexValue.get().getEncodedGoods())));
                        if (vertex.getNumEdges() == 0) {
                            if (maxMsg.get().getValue() <temp.get().getValue())
                                maxMsg = temp;
                        }
                        else {
                            sendMessageToNonConflictingEdges(vertex, temp);
                           }
                        aggregate(MAX_AGG, new DoubleWritable(temp.get().getValue()));
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

    private void sendMessageToNonConflictingEdges(Vertex<EncodedGoodsWritable, EncodedMessageWritable, NullWritable> vertex, EncodedMessageWritable message) {
        for (Edge<EncodedGoodsWritable, NullWritable> edge : vertex.getEdges()) {
            if (message.get().getEncodedGoods().IsNotConflictingWith(edge.getTargetVertexId().get())) {
                sendMessage(
                        edge.getTargetVertexId(),
                        message
                        );
            }
        }
    }


    private void RemoveVertex(Vertex<EncodedGoodsWritable, EncodedMessageWritable, NullWritable> vertex) throws IOException {
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
            System.out.println(" Superstep: "+getSuperstep());
            System.out.println("Edges: "+ getTotalNumEdges());
            System.out.println("Vertices: "+getTotalNumVertices());
            System.out.println("MaxAGG: "+getAggregatedValue(MAX_AGG));
        }
    }
}