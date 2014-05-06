package org.mysamples.io.types;

import org.apache.hadoop.io.WritableComparable;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by user on 4/27/14.
 */
public class WDPVertexIdWritable implements WritableComparable {
    private WDPVertexId wdpVertexId;
    public WDPVertexIdWritable(){}
    public WDPVertexIdWritable(JSONArray jsonVertexId) throws JSONException{

        wdpVertexId=new WDPVertexId(jsonVertexId);
    }
    public WDPVertexIdWritable(long[][]bids){
        wdpVertexId=new WDPVertexId(bids);
    }
    public WDPVertexIdWritable(WDPVertexId wdpVertexId){
        this.wdpVertexId=wdpVertexId;
    }
    /** Set the value of this LongWritable. */
    public void set(WDPVertexId value) { this.wdpVertexId = value; }

    /** Return the value of this LongWritable. */
    public WDPVertexId get() { return wdpVertexId; }

    @Override
    public int compareTo(Object o) {
        WDPVertexId thatValue = ((WDPVertexIdWritable)o).wdpVertexId;
        return this.wdpVertexId.compareTo(thatValue);
    }
    public void readFields(DataInput in) throws IOException {
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
        wdpVertexId=new WDPVertexId(bids);
    }

    public void write(DataOutput out) throws IOException {
        long[][]bids=wdpVertexId.getBids();
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

    /** Returns true iff they have the same amount of bids and the bids in the same position are equals */
    public boolean equals(Object o) {
        if (!(o instanceof WDPVertexIdWritable))
            return false;
        WDPVertexIdWritable other = (WDPVertexIdWritable)o;
        return wdpVertexId.equals(other.wdpVertexId);
    }
    public int hashCode() {
        return wdpVertexId.hashCode();
    }
    public String toString () {
        return wdpVertexId.toString();
    }

}
