package org.mysamples.io.types;

import org.json.JSONArray;
import org.json.JSONException;

import java.util.Arrays;

/**
 * Created by user on 4/26/14.
 */
public class EncodedGoods implements Comparable {
    private long[] goods;

    public EncodedGoods(){goods=new long[0];}

    public EncodedGoods(JSONArray jsonVertexId) throws JSONException {

        goods = new long[jsonVertexId.length()];
        for (int i = 0; i < goods.length; i++) {
            goods[i] = jsonVertexId.getLong(i);
        }
    }

    public EncodedGoods(long[] goods) {
        this.goods = goods;
    }

    /**
     * Set the value of this wDPVertexId.
     */
    public void setGoods(long[]value) {
        this.goods = value;
    }

    /**
     * Return the value of this EncodedGoods.
     */
    public long[] getGoods() {
        return goods;
    }

    public boolean IsNotConflictingWith(EncodedGoods other){
        if(goods!=null)
        for (int i = 0; i < this.goods.length; i++) {
           if( (((this.goods[i]|other.goods[i])^other.goods[i])^this.goods[i])!=0)
               return false;
        }
        return true;
    }
   public EncodedGoods Concat(EncodedGoods other){
       if(goods==null)return new EncodedGoods();
        long[]temp=new long[goods.length];
       for (int i = 0; i < goods.length; i++) {
           temp[i]=goods[i]| other.goods[i];
       };
       return new EncodedGoods(temp);
    }

    public boolean equals(Object o) {
        if(goods==null)return false;
        if (!(o instanceof EncodedGoods))
            return false;
        EncodedGoods other = (EncodedGoods)o;
        for (int i = 0; i < goods.length; i++) {
            if(goods[i]!=other.goods[i])
                return false;
        }
        return true;
    }
    public int hashCode() {
        return goods==null?0:java.util.Arrays.hashCode(goods);
    }
    public String toString () {
        if(goods==null)return "";
        return Arrays.toString(goods);

    }
    @Override
    public int compareTo(Object o) {
        if(goods==null || goods.length==0)return -1;
        if(this.equals(o))return 0;
        EncodedGoods other = (EncodedGoods)o;
        return goods[1]<other.goods[1]?-1:1;
    }
}
