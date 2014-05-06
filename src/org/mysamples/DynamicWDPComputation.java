package org.mysamples;

import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.EdgeFactory;
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
        name = "Dynamic WDP for Combinatorial Auctions",
        description = "Computes the revenue for all vertices in the graph creating new vertices dynamically"
)
public class DynamicWDPComputation extends BasicComputation<
        LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    /**
     * Class logger
     */
    private static final Logger LOG =
            Logger.getLogger(DynamicWDPComputation.class);


    public void log_debug(String message) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(message);
        }
    }

    public void log_info(Vertex vertex) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Superstep: " + getSuperstep() + " TotalNumVertices: " + getTotalNumVertices() + " TotalNumEdges: " + getTotalNumEdges());
            LOG.info("Processing Vertex " + vertex.getId() + " Value= " + vertex.getValue() + " NumEdges: " + vertex.getNumEdges());
        }
    }

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
                        Iterable<DoubleWritable> messages) throws IOException {
        log_info(vertex);
        ApplyValidations();
        double vertexValue = vertex.getValue().get();

        if (getSuperstep() == 0) {
            SendMessageToNonConflictingEdges(vertex,  vertexValue);
        }
        else
        {
            long newVertexNum = 0, msgNum = 0;
            for (DoubleWritable message : messages) {
                msgNum++;
                if (!IsDummyMessage(message.get())) {

                    long newVertexIndex = GetNewVertexId(vertex.getId().get(), newVertexNum);
                    if (IsValidNewVertex(newVertexIndex)) {
                        CreateVertex(vertex, vertexValue, message, newVertexIndex);
                        newVertexNum++;
                    }
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
    }


    /**
     * Valid for small graphs just for testing graph mutation
     *
     * @param currentVertexId
     * @param sourceVertexId
     * @return a unique vertexId if there is not overlapping among currentVertex and sourceVertex
     * otherwise returns -1
     */
    private long GetNewVertexId(long currentVertexId, long sourceVertexId) {
        long shift = (long) Math.pow(10, getSuperstep());
        return currentVertexId * shift + sourceVertexId;
    }

    private boolean IsValidNewVertex(long newVertexIndex) {
        return newVertexIndex > 0;
    }

    private boolean IsNewVertex(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex) {
        return vertex.getId().get() > 9;
    }

    private double DummyMessage = -1;

    private boolean IsDummyMessage(double message) {
        return message <= 0;
    }

    private void ApplyValidations() {
        //newVertexIndex>0 && message>0
    }

    private void CreateVertex(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, double vertexValue, DoubleWritable message, long newVertexIndex) throws IOException {
        LongWritable vertexIndex = new LongWritable(newVertexIndex);
        addVertexRequest(vertexIndex, new DoubleWritable(vertexValue + message.get()));
        for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
            addEdgeRequest(vertexIndex, EdgeFactory.create(edge.getTargetVertexId(), new FloatWritable(0.0f)));
        }
    }

    private void RemoveVertex(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex) throws IOException {
        removeVertexRequest(vertex.getId());
    }
    private void SendMessageToNonConflictingEdges(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, Double vertexValue)
    {
        sendMessageToAllEdges(vertex, new DoubleWritable(vertexValue));
    }
}