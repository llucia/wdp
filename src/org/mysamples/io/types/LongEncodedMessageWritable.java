package org.mysamples.io.types;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by user on 4/27/14.
 */
public class LongEncodedMessageWritable implements Writable, IWDPMessage {
    EncodedMessage wdpMessage;
    public LongEncodedMessageWritable(){}


    public LongEncodedMessageWritable(EncodedMessage wdpMessage){
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
        if (!(o instanceof LongEncodedMessageWritable))
            return false;
        return wdpMessage.equals(((LongEncodedMessageWritable)o).wdpMessage);
    }
    public int hashCode() {
        return wdpMessage.hashCode();
    }
    public String toString () {
        return wdpMessage.toString();
    }

    @Override
    public double getValue() {
        return wdpMessage.getValue();
    }

    @Override
    public boolean IsNotConflictingWith(IWDPVertexId vertexId) {
        return wdpMessage.getEncodedGoods().IsNotConflictingWith(((LongEncodedGoodsWritable) vertexId).get());
    }

    @Override
    public IWDPMessage AggregateVertexValue(IWDPVertexValue vertexValue, IWDPVertexId vertexId) {
        LongEncodedVertexValueWritable encodedValue=(LongEncodedVertexValueWritable)vertexValue;
        return  new LongEncodedMessageWritable(
                new EncodedMessage(
                        encodedValue.get()+wdpMessage.getValue(),wdpMessage.getEncodedGoods().Concat(((LongEncodedGoodsWritable)vertexId).get())));

    }

    @Override
    public IWDPVertexValue ConvertToVertexValue(IWDPVertexId vertexId) {
        return new LongEncodedVertexValueWritable(getValue(), wdpMessage.getEncodedGoods().getGoods());
    }

    @Override
    public int compareTo(Object o) {
        LongEncodedMessageWritable other = (LongEncodedMessageWritable) o;
        if (getValue() < other.getValue()) return -1;
        if (getValue() > other.getValue()) return 1;
        return 0;
    }
}
