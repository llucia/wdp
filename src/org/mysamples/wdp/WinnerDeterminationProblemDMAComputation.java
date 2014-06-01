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
 * Computes the maximum revenue for the winner determination problem (WDP)
 */
@Algorithm(
        name = "Winner Determination Problem using a Dynamic Messages Approach ",
        description = "Computes the maximum revenue for WDP by processing bins one at a time in a top-bottom direction"
)
public class WinnerDeterminationProblemDMAComputation<WDPVertexId extends IWDPVertexId,WDPVertexValue extends IWDPVertexValue,WDPMessage extends IWDPMessage> extends BasicComputation<
     WDPVertexId,WDPVertexValue, NullWritable, WDPMessage> {

    /**
     * Class logger
     */
    private static final Logger LOG =
            Logger.getLogger(WinnerDeterminationProblemDMAComputation.class);

    public void log_info(Vertex<WDPVertexId, WDPVertexValue, NullWritable>  vertex) {
        if (LOG.isInfoEnabled()) {
            System.out.println("Superstep: " + getSuperstep() + " TotalNumVertices: " + getTotalNumVertices() + " TotalNumEdges: " + getTotalNumEdges());
            System.out.println("Processing Vertex " + vertex.getId() + " Value= " + vertex.getValue() + " NumEdges: " + vertex.getNumEdges() + " to: ");
            for (Edge<WDPVertexId, NullWritable> edge : vertex.getEdges()) {
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
    public void compute(Vertex<WDPVertexId,WDPVertexValue, NullWritable> vertex,
                        Iterable<WDPMessage> messages) throws IOException {
     //   log_info(vertex);
        WDPMessage initialMsg=(WDPMessage)vertex.getValue().ConvertToMessage(vertex.getId());
        if (getSuperstep() == 0) {
            sendMessageToAllEdges(vertex, initialMsg);
        }
        else {
            if (getSuperstep() == 1) {

                if(messages.iterator().hasNext())
                    vertex.voteToHalt();
                else
                {
                    sendMessageToNonConflictingEdges(vertex, initialMsg);
                    RemoveVertex(vertex);
                }

            } else {
                WDPMessage maxMsg=initialMsg;
                WDPMessage temp;
                for (IWDPMessage message : messages) {
                    temp = (WDPMessage)message.AggregateVertexValue(vertex.getValue(),vertex.getId());
                    sendMessageToNonConflictingEdges(vertex, temp);
                    aggregate(MAX_AGG, new DoubleWritable(temp.getValue()));
                    if (vertex.getNumEdges() == 0 && maxMsg.compareTo(temp) < 0)
                        maxMsg = temp;
                }

                if (vertex.getNumEdges() == 0) {
                    vertex.setValue((WDPVertexValue)maxMsg.ConvertToVertexValue(vertex.getId()));
                    vertex.voteToHalt();
                }
                else
                       RemoveVertex(vertex);

            }
        }
    }



    private void sendMessageToNonConflictingEdges(Vertex<WDPVertexId, WDPVertexValue, NullWritable> vertex, WDPMessage message) {
        for (Edge<WDPVertexId, NullWritable> edge : vertex.getEdges()) {
            if (message.IsNotConflictingWith(edge.getTargetVertexId())) {
                sendMessage(edge.getTargetVertexId(),message);
            }
        }
    }


    private void RemoveVertex(Vertex<WDPVertexId, WDPVertexValue, NullWritable> vertex) throws IOException {
        removeVertexRequest(vertex.getId());
       // log_info("removeVertexRequest "+vertex);

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