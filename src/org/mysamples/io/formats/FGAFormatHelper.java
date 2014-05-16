package org.mysamples.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONArray;
import org.json.JSONException;
import org.mysamples.io.types.WDPVertexId;
import org.mysamples.io.types.WDPVertexIdWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by user on 4/27/14.
 */
public class FGAFormatHelper {
    public WDPVertexIdWritable getId(JSONArray jsonVertex) throws JSONException,
            IOException {
        return GetVertexFromJsonArray(jsonVertex.getJSONArray(0));
    }

    public DoubleWritable getValue(JSONArray jsonVertex) throws
            JSONException, IOException {
        return new DoubleWritable(jsonVertex.getDouble(1));
    }

    /*
    * Only creates non conflicting edges
    */
    public Iterable<Edge<WDPVertexIdWritable, FloatWritable>> getEdges(
            JSONArray jsonVertex) throws JSONException, IOException {

        WDPVertexIdWritable sourceVertex=getId(jsonVertex);
        JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
        List<Edge<WDPVertexIdWritable, FloatWritable>> edges =
                Lists.newArrayListWithCapacity(jsonEdgeArray.length());
        for (int i = 0; i < jsonEdgeArray.length(); ++i) {
            JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
            WDPVertexIdWritable wdpVertexIdWritable = GetVertexFromJsonArray(jsonEdge);
            if (IsNotConflicting(sourceVertex,wdpVertexIdWritable))
                edges.add(EdgeFactory.create(wdpVertexIdWritable, new FloatWritable(0)));
        }
        return edges;
    }

    public Vertex<WDPVertexIdWritable, DoubleWritable, FloatWritable>
    handleException(Text line, JSONArray jsonVertex, JSONException e) {
        throw new IllegalArgumentException(
                "Couldn't get WDP vertex from line " + line, e);
    }

    private long[][] GetArrayOfBids(JSONArray jsonArray) throws JSONException {
        long[][]bids = new long[jsonArray.length()][];
        for (int k = 0; k < jsonArray.length(); k++) {
            long[] goods;
            JSONArray bidArray=jsonArray.getJSONArray(k);
            goods = new long[bidArray.length()];
            for (int i = 0; i < goods.length; i++) {
                goods[i] = bidArray.getLong(i);
            }
            Arrays.sort(goods);
            bids[k]=goods;
        }
        return bids;
    }
    private WDPVertexIdWritable GetVertexFromJsonArray(JSONArray jsonArray) throws JSONException {
        return new WDPVertexIdWritable(new WDPVertexId(GetArrayOfBids(jsonArray)));
    }

    private boolean IsNotConflicting(WDPVertexIdWritable sourceVertex, WDPVertexIdWritable wdpVertexIdWritable) {
        // sourceVertex.get().IsNotConflictingWith(wdpVertexIdWritable.get())
        long[][]source=sourceVertex.get().getBids();
        long[][]target=wdpVertexIdWritable.get().getBids();
        long[][]realTarget=new long[target.length-source.length][];
        for (int i =0; i <realTarget.length ; i++) {
            realTarget[i]=target[ source.length+i];
        }
        WDPVertexId wdpRealTarget=new WDPVertexId(realTarget);
        boolean notConflicting=sourceVertex.get().IsNotConflictingWith(wdpRealTarget);
        return notConflicting;
    }

}
