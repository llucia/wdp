package org.mysamples.wdp;

/**
 * Created by user on 4/27/14.
 */

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.log4j.Logger;
import org.mysamples.io.types.WDPMessage;
import org.mysamples.io.types.WDPMessageWritable;
import org.mysamples.io.types.WDPVertexId;
import org.mysamples.io.types.WDPVertexIdWritable;

import java.io.IOException;

/**
 * Computes the revenue for the winner determination problem (WDP) in Combinatorial Auctions
 */
@Algorithm(
        name = "Dynamic WDP for Combinatorial Auctions",
        description = "Computes the revenue for all vertices in the graph creating new vertices dynamically"
)
public class AggregateDynamicWDPComputation extends BasicComputation<
        WDPVertexIdWritable, DoubleWritable, FloatWritable, WDPMessageWritable> {

    /**
     * Class logger
     */
    private static final Logger LOG =
            Logger.getLogger(AggregateDynamicWDPComputation.class);

    public void log_info(Vertex<WDPVertexIdWritable, DoubleWritable, FloatWritable>  vertex) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Superstep: " + getSuperstep() + " TotalNumVertices: " + getTotalNumVertices() + " TotalNumEdges: " + getTotalNumEdges());
            LOG.info("Processing Vertex " + vertex.getId() + " Value= " + vertex.getValue() + " NumEdges: " + vertex.getNumEdges()+ " to: ");
            for (Edge<WDPVertexIdWritable, FloatWritable> edge : vertex.getEdges()) {
                LOG.info(edge.getTargetVertexId()+", ");
            }

        }
    }
    public void log_info(String msg) {
        if (LOG.isInfoEnabled()) {
            LOG.info(msg);
        }
    }

    private double DummyMessage = -1;

    @Override
    public void compute(Vertex<WDPVertexIdWritable, DoubleWritable, FloatWritable> vertex,
                        Iterable<WDPMessageWritable> messages) throws IOException {
        log_info(vertex);
        double vertexValue = vertex.getValue().get();

        if (getSuperstep() == 0) {
            SendMessageToNonConflictingEdges(vertex,  vertexValue);
        }
        else
        {
            long newVertexNum = 0, msgNum = 0;
            for (WDPMessageWritable message : messages) {
                msgNum++;
                if (!IsDummyMessage(message)) {
                    WDPVertexId newVertexIndex = GetNewVertexId(vertex.getId().get(),message.get().getVertexId());
                    CreateVertex(newVertexIndex, vertex, vertexValue, message);
                    newVertexNum++;
                }
            }

            if (newVertexNum > 0)
               SendMessageToNonConflictingEdges(vertex,DummyMessage);

            if (msgNum == 0) {
                if (IsNewVertex(vertex))
                    SendMessageToNonConflictingEdges(vertex,vertexValue);
                if (vertex.getNumEdges() == 0)
                    vertex.voteToHalt();
                else
                    RemoveVertex(vertex);

            }
        }
        System.out.print(getAggregatedValue(MAX_AGG)+"; ");

    }


    /**
     * Valid for small graphs just for testing graph mutation
     *
     * @param currentVertexId
     * @param sourceVertexId
     * @return a unique vertexId if there is not overlapping among currentVertex and sourceVertex
     * otherwise returns -1
     */
    private WDPVertexId GetNewVertexId(WDPVertexId currentVertexId, WDPVertexId sourceVertexId) {
       return sourceVertexId.Concat(currentVertexId);
    }


    private boolean IsNewVertex(Vertex<WDPVertexIdWritable, DoubleWritable, FloatWritable> vertex) {
        return vertex.getId().get().IsComposite();
    }

    private boolean IsDummyMessage(WDPMessageWritable message) {
        return message.get().getValue() ==DummyMessage;
    }

    private void CreateVertex(WDPVertexId newVertexId, Vertex<WDPVertexIdWritable, DoubleWritable, FloatWritable> vertex, double vertexValue, WDPMessageWritable message) throws IOException {
        WDPVertexIdWritable vertexIndex = new WDPVertexIdWritable(newVertexId);
        DoubleWritable newValue= new DoubleWritable( vertexValue + message.get().getValue());
        addVertexRequest(vertexIndex,newValue);
        log_info("addVertexRequest "+vertexIndex);
        for (Edge<WDPVertexIdWritable, FloatWritable> edge : vertex.getEdges()) {
            if (newVertexId.IsNotConflictingWith(edge.getTargetVertexId().get())) {
                addEdgeRequest(vertexIndex, EdgeFactory.create(new WDPVertexIdWritable(edge.getTargetVertexId().get()), new FloatWritable(0.0f)));
                log_info("addEdgeRequest from Vertex "+vertexIndex+" to "+edge.getTargetVertexId());
            }
        }
        aggregate(MAX_AGG, newValue);
    }

    private void RemoveVertex(Vertex<WDPVertexIdWritable, DoubleWritable, FloatWritable> vertex) throws IOException {
        removeVertexRequest(vertex.getId());
        log_info("removeVertexRequest "+vertex);

    }
    private void SendMessageToNonConflictingEdges(Vertex<WDPVertexIdWritable, DoubleWritable, FloatWritable> vertex, Double vertexValue)
    {
        WDPMessageWritable msg= new WDPMessageWritable(new WDPMessage(vertexValue,vertex.getId().get()));
        sendMessageToAllEdges(vertex,msg);
        log_info("Messages sent to all Edges "+msg);

    }
    /*private void SendMessageToNonConflictingEdges(Vertex<WDPVertexIdWritable, DoubleWritable, FloatWritable> vertex, Double vertexValue)
    {
        WDPMessageWritable msg= new WDPMessageWritable(new WDPMessage(vertexValue,vertex.getId().get()));
        for(Edge<WDPVertexIdWritable,FloatWritable> edge: vertex.getEdges()){
            {
                sendMessage( edge.getTargetVertexId(),msg);
                log_info("Message "+msg+" sent to Vertex "+edge.getTargetVertexId());
            }
        }
    }*/

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