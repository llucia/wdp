package org.mysamples.wdp;

/**
 * Created by user on 4/27/14.
 */

import org.apache.giraph.Algorithm;
import org.mysamples.io.types.*;

/**
 * Computes the revenue for the winner determination problem (WDP) in Combinatorial Auctions
 */
@Algorithm(
        name = "Dynamic WDP for Combinatorial Auctions",
        description = "Computes the revenue for all vertices in the graph creating new vertices dynamically"
)
public class SimpleDMAEncodedVertexComputation extends WinnerDeterminationProblemDMAComputation<
        EncodedGoodsWritable, EncodedVertexValueWritable, EncodedMessageWritable> {
}
