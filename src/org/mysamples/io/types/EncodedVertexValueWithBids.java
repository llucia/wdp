package org.mysamples.io.types;

import java.util.Arrays;

/**
 * Created by user on 4/26/14.
 */
public class EncodedVertexValueWithBids {
    double value;
    long[] bids;

    public EncodedVertexValueWithBids(){};

    public EncodedVertexValueWithBids(double value, long[] bids){
        this.value=value;
        this.bids=bids;
    }

    public double getValue(){
        return value;
    }
    public long[] getBids(){
        return bids;
    }
    public boolean equals(Object o) {
        if (!(o instanceof EncodedVertexValueWithBids))
            return false;
        EncodedVertexValueWithBids other = (EncodedVertexValueWithBids)o;
        return this.value==other.value && AreEquals(bids,other.getBids());
    }
    private boolean AreEquals(long[]bids,long[]other) {
        if (bids == null || other == null || bids.length != other.length) return false;
        for (int i = 0; i < bids.length; i++)
            if (bids[i] != other[i]) return false;
        return true;
    }


    public int hashCode() {
        return this.toString().hashCode();
    }
    public String toString () {
        return value+", "+Arrays.toString(bids);
    }
}
