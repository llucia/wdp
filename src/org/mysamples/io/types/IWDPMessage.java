package org.mysamples.io.types;

import org.apache.hadoop.io.Writable;

/**
 * Created by user on 5/31/14.
 */
public interface IWDPMessage extends Writable,Comparable {
    double getValue();
    boolean IsNotConflictingWith(IWDPVertexId vertexId);
    IWDPMessage AggregateVertexValue(IWDPVertexValue vertexValue,IWDPVertexId vertexId);
    IWDPVertexValue ConvertToVertexValue(IWDPVertexId vertexId);
}
