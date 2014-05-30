package org.mysamples.io.types;

/**
 * Created by user on 4/26/14.
 */
public class EncodedMessage {
    double value;
    EncodedGoods encodedGoods;

    public EncodedMessage(){};
    public EncodedMessage(double value, EncodedGoods encodedGoods){
        this.value=value;
        this.encodedGoods = encodedGoods;
    }

    public double getValue(){
        return value;
    }
    public EncodedGoods getEncodedGoods(){
        return encodedGoods;
    }
    public boolean equals(Object o) {
        if (!(o instanceof EncodedMessage))
            return false;
        EncodedMessage other = (EncodedMessage)o;
        return this.value==other.value&& this.encodedGoods.equals(other.encodedGoods);
    }
    public int hashCode() {
        return (int)Double.doubleToLongBits(value)+31*encodedGoods.hashCode();
    }
    public String toString () {
        if(encodedGoods==null)return "";
        return value+", "+encodedGoods.toString();
    }

}
