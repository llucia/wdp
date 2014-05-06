package org.mysamples;


import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
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
public class TestingMutation extends BasicComputation<
        LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(TestingMutation.class);

    public void log_debug(String message){
        if (LOG.isDebugEnabled()) {
            LOG.debug(message);
        }
    }

    @Override
    public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {

        int seed = -1;
        double revenue = vertex.getValue().get();
        log_debug("Vertex " + vertex.getId() + " Revenue = " + revenue);
        boolean fakeEdge = false;
        int i = 0;
        for (DoubleWritable message : messages) {
            i++;
            if (message.get() == seed)
                vertex.voteToHalt();
            else {
                fakeEdge=true;
                createNewVertex(vertex, message, seed, i);
            }
        }
        if (i == 0) {
            sendMessageToAllEdges(vertex, new DoubleWritable(revenue));
            vertex.voteToHalt();
        }
        if(fakeEdge)
            addEdgeRequest(vertex.getId(), EdgeFactory.create(vertex.getId(), new FloatWritable(seed)));
    }
    private void createNewVertex(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex, DoubleWritable message, int seed, int i) throws IOException {
        long newVertexIndex=getNewVertexId(vertex.getId().get(),i);
        if(newVertexIndex==seed)return;
        LongWritable vertexIndex = new LongWritable(newVertexIndex);
        addVertexRequest(vertexIndex, new DoubleWritable(vertex.getValue().get() + message.get()));
        for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges())
            addEdgeRequest(vertexIndex, EdgeFactory.create(edge.getTargetVertexId(), new FloatWritable(0.0f)));
    }

    private long getNewVertexId(long sourceVertexId, int currentVertexId) {
        long shift=(long)Math.pow(10,getSuperstep());
        long newVertexId= sourceVertexId*shift+currentVertexId;
        log_debug("NewVertexId: "+newVertexId+" for SourceVertex: "+sourceVertexId+" CurrentVertex: "+currentVertexId);
        return newVertexId;
    }

}
