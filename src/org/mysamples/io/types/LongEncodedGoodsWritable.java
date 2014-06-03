package org.mysamples.io.types;

import com.google.common.primitives.Longs;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by user on 4/27/14.
 */
public class LongEncodedGoodsWritable implements IWDPVertexId {

    private EncodedGoods encodedGoods;
    private long id;

    public LongEncodedGoodsWritable(){}
    public LongEncodedGoodsWritable(long id, long[] goods){
        this.id=id;
        encodedGoods=new EncodedGoods(goods);
    }
    public LongEncodedGoodsWritable(long value,JSONArray jsonVertexId) throws JSONException{
        id=value;
        encodedGoods=new EncodedGoods(jsonVertexId);
    }
    public LongEncodedGoodsWritable(long id, EncodedGoods encodedGoods){
        this.id=id;
        this.encodedGoods=encodedGoods;
    }


    public void set(EncodedGoods value) { encodedGoods = value; }
    public EncodedGoods get() { return encodedGoods; }
    public long getLongId(){return id;}

    @Override
    public int compareTo(Object o) {
        LongEncodedGoodsWritable other=(LongEncodedGoodsWritable)o;
        return encodedGoods.compareTo(other.encodedGoods);
    }

    public void readFields(DataInput in) throws IOException {
        // construct matrix
        id=in.readLong();
        long[]goods = new long[in.readInt()];
        for (int i = 0; i < goods.length; i++) {
               goods[i]   = in.readLong();
        }
        encodedGoods=new EncodedGoods(goods);
    }
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        long[]goods=encodedGoods.getGoods();
        out.writeInt(goods.length);                 // write values
            for (int j = 0; j < goods.length; j++) {
                out.writeLong(goods[j]);
            }
    }



    public boolean equals(Object o) {
        if (!(o instanceof LongEncodedGoodsWritable))
            return false;
        LongEncodedGoodsWritable other=(LongEncodedGoodsWritable)o;
        return encodedGoods.equals((other).encodedGoods);
    }
    public int hashCode() {
        return encodedGoods.hashCode();
    }
    public String toString () {
        return  id + " " +encodedGoods.toString();
    }


}
