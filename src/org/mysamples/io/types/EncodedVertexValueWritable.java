package org.mysamples.io.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by user on 4/27/14.
 */
public class EncodedVertexValueWritable implements IWDPVertexValue {

    double encodedValue;
    long[] encodedGoods;

    public EncodedVertexValueWritable(){}
    public EncodedVertexValueWritable(double value,long[]goods){
        this.encodedValue=value;
        this.encodedGoods=goods;
    }
    public EncodedVertexValueWritable(double value){
        this.encodedValue=value;
    }

    public void set(double value) { this.encodedValue = value; }
    public double get() { return encodedValue; }

    /*************Writable Members*******************/

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(encodedValue);
        if(encodedGoods==null){
            out.writeInt(0);
        }
        else{
            out.writeInt(encodedGoods.length);                 // write values
            for (int j = 0; j < encodedGoods.length; j++) {
                out.writeLong(encodedGoods[j]);
            }

        }

    }
    @Override
    public void readFields(DataInput in) throws IOException {
        encodedValue = in.readDouble();
        int n=in.readInt();
        if(n>0){
            encodedGoods = new long[n];
            for (int j = 0; j < encodedGoods.length; j++) {
                encodedGoods[j] = in.readLong();
            }
        }
    }
    /*************END Writable Members*****************/

    public boolean equals(Object o) {
        if (!(o instanceof EncodedVertexValueWritable))
            return false;
        return encodedValue==((EncodedVertexValueWritable)o).encodedValue;
    }
    public int hashCode() {
        return Double.valueOf(encodedValue).hashCode();
    }
    public String toString () {
        return encodedGoods==null? encodedValue+"":encodedValue+" "+ Arrays.toString(encodedGoods);
    }

    /*************IWDPVertexValue Members*******************/

    @Override
    public EncodedMessageWritable ConvertToMessage(IWDPVertexId vertexId) {
        return new EncodedMessageWritable(new EncodedMessage(encodedValue,((EncodedGoodsWritable)vertexId).get()));
    }


    /*************END IWDPVertexValue Members*******************/

}
