package org.mysamples.io.types;

import org.json.JSONArray;

import java.util.Arrays;

/**
 * Created by user on 4/26/14.
 */
public class EncodedMessageWithBids {
    double value;
    EncodedGoods encodedGoods;
    long[]bids;

    public EncodedMessageWithBids(){};

    public EncodedMessageWithBids(double value, EncodedGoods encodedGoods, long[]bids){
        this.value=value;
        this.encodedGoods=encodedGoods;
        this.bids=bids;
    }

    public double getValue(){
        return value;
    }
    public EncodedGoods getEncodedGoods(){
        return encodedGoods;
    }
    public long[]getBids(){
        return bids;
    }
    public boolean equals(Object o) {
        if (!(o instanceof EncodedMessageWithBids))
            return false;
        EncodedMessageWithBids other = (EncodedMessageWithBids)o;
        return this.value==other.value&& this.encodedGoods.equals(other.encodedGoods)&& AreEquals(bids,other.getBids());
    }

    public int hashCode() {
        return this.toString().hashCode();
    }
    public String toString () {
        if(encodedGoods==null)return "";
        return value+", "+encodedGoods.toString()+", "+ Arrays.toString(bids);
    }

    private boolean AreEquals(long[]bids,long[]other) {
        if (bids == null || other == null || bids.length != other.length) return false;
        for (int i = 0; i < bids.length; i++)
            if (bids[i] != other[i]) return false;
        return true;
    }

}
