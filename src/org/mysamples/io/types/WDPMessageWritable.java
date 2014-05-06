package org.mysamples.io.types;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by user on 4/27/14.
 */
public class WDPMessageWritable implements Writable {
    WDPMessage wdpMessage;
    public WDPMessageWritable(){}


    public WDPMessageWritable(WDPMessage wdpMessage){
        this.wdpMessage=wdpMessage;
    }

    /** Set the value of this LongWritable. */
    public void set(WDPMessage value) { this.wdpMessage = value; }

    /** Return the value of this LongWritable. */
    public WDPMessage get() { return wdpMessage; }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(wdpMessage.value);
        long[][]bids=wdpMessage.vertexId.getBids();
        out.writeInt(bids.length);                 // write values
        for (int i = 0; i < bids.length; i++) {
            out.writeInt(bids[i].length);
        }
        for (int i = 0; i < bids.length; i++) {
            for (int j = 0; j < bids[i].length; j++) {
                out.writeLong(bids[i][j]);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        double value=in.readDouble();
        // construct matrix
        long[][]bids = new long[in.readInt()][];
        for (int i = 0; i < bids.length; i++) {
            bids[i] = new long[in.readInt()];
        }
        // construct values
        for (int i = 0; i < bids.length; i++) {
            for (int j = 0; j < bids[i].length; j++) {
                bids[i][j]   = in.readLong();
            }
        }
        WDPVertexId wdpVertexId=new WDPVertexId(bids);
        wdpMessage=new WDPMessage(value,wdpVertexId);
    }

    /** Returns true iff they have the same amount of bids and the bids in the same position are equals */
    public boolean equals(Object o) {
        if (!(o instanceof WDPMessageWritable))
            return false;
        WDPMessageWritable other = (WDPMessageWritable)o;
        return wdpMessage.equals(other.wdpMessage);
    }
    public int hashCode() {
        return wdpMessage.hashCode();
    }
    public String toString () {
        return wdpMessage.toString();
    }

}
