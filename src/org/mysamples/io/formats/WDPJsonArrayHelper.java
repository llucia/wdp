package org.mysamples.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.json.JSONArray;
import org.json.JSONException;
import org.mysamples.io.types.WDPVertexIdWritable;

import java.io.IOException;
import java.util.List;

/**
 * Created by user on 4/27/14.
 */
public class WDPJsonArrayHelper {
    public WDPVertexIdWritable getId(JSONArray jsonVertex) throws JSONException,
            IOException {
        return new WDPVertexIdWritable(jsonVertex.getJSONArray(0));
    }

    public DoubleWritable getValue(JSONArray jsonVertex) throws
            JSONException, IOException {
        return new DoubleWritable(jsonVertex.getDouble(1));
    }

    public Iterable<Edge<WDPVertexIdWritable, FloatWritable>> getEdges(
            JSONArray jsonVertex) throws JSONException, IOException {

        WDPVertexIdWritable sourceVertex=getId(jsonVertex);
        JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
        List<Edge<WDPVertexIdWritable, FloatWritable>> edges =
                Lists.newArrayListWithCapacity(jsonEdgeArray.length());
        for (int i = 0; i < jsonEdgeArray.length(); ++i) {
            JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
            WDPVertexIdWritable wdpVertexIdWritable = new WDPVertexIdWritable(jsonEdge);
            if (sourceVertex.get().IsNotConflictingWith(wdpVertexIdWritable.get()))
                edges.add(EdgeFactory.create(wdpVertexIdWritable, new FloatWritable(0)));
        }
        return edges;
    }

}
