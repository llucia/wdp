package org.mysamples.io.types;

import org.apache.hadoop.io.Writable;

/**
 * Created by user on 5/31/14.
 */
public interface IWDPVertexValue extends Writable {
    IWDPMessage ConvertToMessage(IWDPVertexId vertexId);
}
