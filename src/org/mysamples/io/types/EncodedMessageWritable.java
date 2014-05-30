package org.mysamples.io.types;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by user on 4/27/14.
 */
public class EncodedMessageWritable implements Writable {
    EncodedMessage wdpMessage;
    public EncodedMessageWritable(){}


    public EncodedMessageWritable(EncodedMessage wdpMessage){
        this.wdpMessage=wdpMessage;
    }

    /** Set the value of this LongWritable. */
    public void set(EncodedMessage value) { this.wdpMessage = value; }

    /** Return the value of this LongWritable. */
    public EncodedMessage get() { return wdpMessage; }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(wdpMessage.value);
        long[] goods = wdpMessage.encodedGoods.getGoods();
        out.writeInt(goods.length);                 // write values
        for (int j = 0; j < goods.length; j++) {
            out.writeLong(goods[j]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        double value = in.readDouble();
        long[] goods = new long[in.readInt()];
        for (int j = 0; j < goods.length; j++) {
            goods[j] = in.readLong();
        }
        wdpMessage = new EncodedMessage(value, new EncodedGoods(goods));
    }

    /** Returns true iff they have the same amount of bids and the bids in the same position are equals */
    public boolean equals(Object o) {
        if (!(o instanceof EncodedMessageWritable))
            return false;
        EncodedMessageWritable other = (EncodedMessageWritable)o;
        return wdpMessage.equals(other.wdpMessage);
    }
    public int hashCode() {
        return wdpMessage.hashCode();
    }
    public String toString () {
        return wdpMessage.toString();
    }

}
