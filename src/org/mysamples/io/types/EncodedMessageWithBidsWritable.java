package org.mysamples.io.types;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by user on 4/27/14.
 */
public class EncodedMessageWithBidsWritable implements IWDPMessage {

    EncodedMessageWithBids wdpMessage;

    public EncodedMessageWithBidsWritable(){}
    public EncodedMessageWithBidsWritable(EncodedMessageWithBids wdpMessage){
        this.wdpMessage=wdpMessage;
    }

    public void set(EncodedMessageWithBids value) { this.wdpMessage = value; }
    public EncodedMessageWithBids get() { return wdpMessage; }

    /*************Writable Members*******************/

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(wdpMessage.value);
        long[] goods = wdpMessage.encodedGoods.getGoods();
        out.writeInt(goods.length);                 // write values
        for (int j = 0; j < goods.length; j++) {
            out.writeLong(goods[j]);
        }
        long[] bids = wdpMessage.getBids();
        out.writeInt(bids.length);                 // write values
        for (int j = 0; j < bids.length; j++) {
            out.writeLong(bids[j]);
        }
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        double value = in.readDouble();
        long[] goods = new long[in.readInt()];
        for (int j = 0; j < goods.length; j++) {
            goods[j] = in.readLong();
        }
        long[] bids = new long[in.readInt()];
        for (int j = 0; j < bids.length; j++) {
            bids[j] = in.readLong();
        }
        wdpMessage = new EncodedMessageWithBids(value, new EncodedGoods(goods),bids);
    }
    /*************END Writable Members*****************/

    public boolean equals(Object o) {
        if (!(o instanceof EncodedMessageWithBidsWritable))
            return false;
        EncodedMessageWithBidsWritable other = (EncodedMessageWithBidsWritable)o;
        return wdpMessage.equals(other.wdpMessage);
    }
    public int hashCode() {
        return wdpMessage.hashCode();
    }
    public String toString () {
        return wdpMessage.toString();
    }

      /*************IWDPMessage Members*******************/

    @Override
    public double getValue() {
        return wdpMessage.getValue();
    }

    @Override
    public IWDPVertexValue ConvertToVertexValue(IWDPVertexId vertexId) {
        return new EncodedVertexValueWithBidsWritable(new EncodedVertexValueWithBids(getValue(),wdpMessage.getBids()));
    }

    @Override
    public int compareTo(Object o) {
        EncodedMessageWithBidsWritable other = (EncodedMessageWithBidsWritable) o;
        if (getValue() < other.getValue()) return -1;
        if (getValue() > other.getValue()) return 1;
        return 0;
    }

    @Override
    public boolean IsNotConflictingWith(IWDPVertexId vertexId) {
        EncodedGoodsWritable other = (EncodedGoodsWritable)vertexId;
        return wdpMessage.getEncodedGoods().IsNotConflictingWith(other.get());
    }

    @Override
    public IWDPMessage AggregateVertexValue(IWDPVertexValue vertexValue, IWDPVertexId vertexId) {
        EncodedVertexValueWithBidsWritable encodedValue=(EncodedVertexValueWithBidsWritable)vertexValue;
        return  new EncodedMessageWithBidsWritable(
                new EncodedMessageWithBids(
                        encodedValue.get().getValue()+wdpMessage.getValue(),wdpMessage.getEncodedGoods().Concat(((EncodedGoodsWritable)vertexId).get()),ConcatBids(encodedValue.get())));

    }

    private long[] ConcatBids(EncodedVertexValueWithBids encodedValue) {
        //skip dummy bids in the allocation
        if(encodedValue.getValue()==0)
            return wdpMessage.getBids();
        //add my valid bid to the ongoing allocation
        int init=wdpMessage.getBids().length;
        long[]result= Arrays.copyOf(wdpMessage.getBids(), init + encodedValue.getBids().length);
        for (int i = init; i< result.length ; i++) {
            result[i]=encodedValue.getBids()[i-init];
        }
        return result;
    }

    /*************END IWDPMessage Members****************/

}
