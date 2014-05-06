package org.mysamples;

import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by user on 5/4/14.
 */
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
public class TestAddRequestComputation extends BasicComputation<
        LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(TestAddRequestComputation.class);

    public void log_info(Vertex<LongWritable, DoubleWritable, FloatWritable>  vertex) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Superstep: " + getSuperstep() + " TotalNumVertices: " + getTotalNumVertices() + " TotalNumEdges: " + getTotalNumEdges());
            LOG.info("Processing Vertex " + vertex.getId() + " Value= " + vertex.getValue() + " NumEdges: " + vertex.getNumEdges()+ " to: ");
            for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
                LOG.info(edge.getTargetVertexId()+", ");
            }
        }
    }
    @Override
    public void compute(
            Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {
        log_info(vertex);
        if (getSuperstep() == 0) {
            if (vertex.getId().get() == 2) {
                addVertexRequest(new LongWritable(7), new DoubleWritable(70));
                for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges())
                    addEdgeRequest(new LongWritable(7), EdgeFactory.create(new LongWritable(edge.getTargetVertexId().get()), new FloatWritable(0)));

            }
        }
        if (getSuperstep() > 4)
            vertex.voteToHalt();

    }
}
