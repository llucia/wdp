package org.mysamples.io.types;

import org.apache.hadoop.io.WritableComparable;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by user on 4/27/14.
 */
public class EncodedGoodsWritable implements WritableComparable {
    private EncodedGoods encodedGoods;
    public EncodedGoodsWritable(){}
    public EncodedGoodsWritable(JSONArray jsonVertexId) throws JSONException{

        encodedGoods=new EncodedGoods(jsonVertexId);
    }
    public EncodedGoodsWritable(long[]goods){
        encodedGoods=new EncodedGoods(goods);
    }
    public EncodedGoodsWritable(EncodedGoods encodedGoods){
        this.encodedGoods=encodedGoods;
    }
    /** Set the value of this LongWritable. */
    public void set(EncodedGoods value) { encodedGoods = value; }

    /** Return the value of this LongWritable. */
    public EncodedGoods get() { return encodedGoods; }

    @Override
    public int compareTo(Object o) {
        EncodedGoods thatValue = ((EncodedGoodsWritable)o).encodedGoods;
        return this.encodedGoods.compareTo(thatValue);
    }
    public void readFields(DataInput in) throws IOException {
        // construct matrix
        long[]goods = new long[in.readInt()];
        for (int i = 0; i < goods.length; i++) {
               goods[i]   = in.readLong();
        }
        encodedGoods=new EncodedGoods(goods);
    }

    public void write(DataOutput out) throws IOException {
        long[]goods=encodedGoods.getGoods();
        out.writeInt(goods.length);                 // write values
            for (int j = 0; j < goods.length; j++) {
                out.writeLong(goods[j]);
            }
    }

    /** Returns true iff they have the same amount of bids and the bids in the same position are equals */
    public boolean equals(Object o) {
        if (!(o instanceof EncodedGoodsWritable))
            return false;
        EncodedGoodsWritable other = (EncodedGoodsWritable)o;
        return encodedGoods.equals(other.encodedGoods);
    }
    public int hashCode() {
        return encodedGoods.hashCode();
    }
    public String toString () {
        return encodedGoods.toString();
    }

}
