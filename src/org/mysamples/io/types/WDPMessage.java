package org.mysamples.io.types;

/**
 * Created by user on 4/26/14.
 */
public class WDPMessage {
    double value;
    WDPVertexId vertexId;

    public WDPMessage(){};
    public WDPMessage(double value, WDPVertexId vertexId){
        this.value=value;
        this.vertexId = vertexId;
    }

    public double getValue(){
        return value;
    }
    public WDPVertexId getVertexId(){
        return vertexId;
    }

    /** Returns true iff they have the same amount of bids and the bids in the same position are equals */
    public boolean equals(Object o) {
        if (!(o instanceof WDPMessage))
            return false;
        WDPMessage other = (WDPMessage)o;
        return this.value==other.value&& this.vertexId.equals(other.vertexId);
    }
    public int hashCode() {
        return (int)Double.doubleToLongBits(value)+2*vertexId.hashCode();
    }
    public String toString () {
        if(vertexId==null)return "";
        return value+", "+vertexId.toString();
    }

}
