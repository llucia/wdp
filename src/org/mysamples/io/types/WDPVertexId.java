package org.mysamples.io.types;

import org.json.JSONArray;
import org.json.JSONException;

import java.util.Arrays;

/**
 * Created by user on 4/26/14.
 */
public class WDPVertexId implements Comparable {
    private long[][] bids;

    public WDPVertexId(JSONArray jsonVertexId) throws JSONException {
        bids = new long[1][];
        long[] goods;
        goods = new long[jsonVertexId.length()];
        bids[0] = goods;

        for (int i = 0; i < goods.length; i++) {
            goods[i] = jsonVertexId.getLong(i);
        }
        Arrays.sort(goods);
    }

    public WDPVertexId(long[][] bids) {
        this.bids = bids;//sorted bids
    }

    /**
     * Set the value of this wDPVertexId.
     */
    public void setBids(long[][] value) {
        this.bids = value;
    }

    /**
     * Return the value of this WDPVertexId.
     */
    public long[][] getBids() {
        return bids;
    }

    public int NumberOfBids(){
        return bids.length;
    }
    public boolean IsComposite(){
        return bids.length>1;
    }
    public boolean IsNotConflictingWith(WDPVertexId other){
        for (int i = 0; i < bids.length; i++) {
            for (int j = 0; j < bids[i].length; j++) {
                for (int k = 0; k <other.bids.length ; k++) {
                    if(Contains(other.bids[k],bids[i][j])){
                        return false;
                    }
                }
            }
        }
        return true;
    }
    private boolean Contains(long[]goods, long good){
        for (int i = 0; i < goods.length; i++) {
            if(goods[i]==good)
                return true;
        }
        return false;
    }
    public WDPVertexId Concat(WDPVertexId other){
        long[][]temp=new long[bids.length+other.bids.length][];
        for (int i = 0; i <bids.length ; i++) {
            temp[i]=bids[i];
        }
        int shift=bids.length;
        for (int i = 0; i < other.bids.length; i++) {
            temp[i+shift]=other.bids[i];
        }
        return new WDPVertexId(temp);
    }

    /** Returns true iff they have the same amount of bids and the bids in the same position are equals */
    public boolean equals(Object o) {
        if (!(o instanceof WDPVertexId))
            return false;

        WDPVertexId other = (WDPVertexId)o;

        if(this.bids.length!=other.bids.length)return false;
        for (int i = 0; i <this.bids.length ; i++) {
            if(this.bids[i].length!=other.bids[i].length)return false;
            for (int j = 0; j < this.bids[i].length; j++) {
                if(this.bids[i][j]!=other.bids[i][j])return false;
            }
        }
        return true;
    }
    public int hashCode() {
        return print(bids).hashCode();
    }
    public String toString () {
        if(bids==null)return "";
        return print(bids);
    }
    @Override
    public int compareTo(Object o) {
        long[][] thatValue = ((WDPVertexId)o).bids;
        long[][]thisValue=this.bids;
        int min=Math.min(thisValue.length, thatValue.length);
        int temp=0;
        for (int i = 0; i <min ; i++) {
            temp=compareBids(thisValue[i],thatValue[i]);
            if(temp!=0)return temp;
        }
        if(thisValue.length==thatValue.length)return 0;
        return min==thisValue.length?-1:1;
    }

    private String print(long[][]bids){
        StringBuilder sb =new StringBuilder("[");
        for (int i = 0; i < bids.length; i++) {
            sb.append("[");
            for (int j = 0; j < bids[i].length; j++) {
                if(j>0)sb.append(",");
                sb.append(bids[i][j]);
            }
            sb.append("]");
        }
        sb.append("]");
        return sb.toString();
    }
    private int compareBids(long[]a, long[]b){
        int min=Math.min(a.length, b.length);
        for (int i = 0; i <min ; i++) {
            if(a[i]<b[i])return -1;
            if(a[i]>b[i])return 1;
        }
        if(a.length==b.length)return 0;
        return min==a.length?-1:1;
    }
}
