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
import java.util.Arrays;

/**
 * Computes the revenue for the winner determination problem (WDP) in Combinatorial Auctions
 */
@Algorithm(
        name = "Dynamic WDP for Combinatorial Auctions",
        description = "Computes the revenue for all vertices in the graph creating new vertices dynamically"
)
public class DMAEncodedVertexWithBidsComputation extends WinnerDeterminationProblemDMAComputation<
        EncodedGoodsWritable, EncodedVertexValueWithBidsWritable, EncodedMessageWithBidsWritable> {
}
