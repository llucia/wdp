package org.mysamples.io.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by user on 4/27/14.
 */
public class EncodedVertexValueWithBidsWritable implements IWDPVertexValue {

    EncodedVertexValueWithBids encodedValue;

    public EncodedVertexValueWithBidsWritable(){}
    public EncodedVertexValueWithBidsWritable(EncodedVertexValueWithBids encodedValue){
        this.encodedValue=encodedValue;
    }

    public void set(EncodedVertexValueWithBids value) { this.encodedValue = value; }
    public EncodedVertexValueWithBids get() { return encodedValue; }

    /*************Writable Members*******************/

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(encodedValue.value);
        long[] bids = encodedValue.getBids();
        out.writeInt(bids.length);                 // write values
        for (int j = 0; j < bids.length; j++) {
            out.writeLong(bids[j]);
        }
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        double value = in.readDouble();
        long[] bids = new long[in.readInt()];
        for (int j = 0; j < bids.length; j++) {
            bids[j] = in.readLong();
        }
        encodedValue = new EncodedVertexValueWithBids(value,bids);
    }
    /*************END Writable Members*****************/

    public boolean equals(Object o) {
        if (!(o instanceof EncodedVertexValueWithBidsWritable))
            return false;
        return encodedValue.equals(((EncodedVertexValueWithBidsWritable)o).encodedValue);
    }
    public int hashCode() {
        return encodedValue.hashCode();
    }
    public String toString () {
        return encodedValue.toString();
    }

    /*************IWDPVertexValue Members*******************/

    @Override
    public EncodedMessageWithBidsWritable ConvertToMessage(IWDPVertexId vertexId) {
        EncodedGoodsWritable encodedGoodsWritable=(EncodedGoodsWritable)vertexId;
        return new EncodedMessageWithBidsWritable(new EncodedMessageWithBids(encodedValue.getValue(),encodedGoodsWritable.get(),encodedValue.getBids()));
    }


    /*************END IWDPVertexValue Members*******************/

}
