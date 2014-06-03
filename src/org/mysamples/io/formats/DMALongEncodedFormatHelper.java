package org.mysamples.io.formats;

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONArray;
import org.json.JSONException;
import org.mysamples.io.types.LongEncodedGoodsWritable;
import org.mysamples.io.types.LongEncodedVertexValueWritable;
import org.mysamples.io.types.WDPVertexId;
import org.mysamples.io.types.WDPVertexIdWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by user on 4/27/14.
 */
public class DMALongEncodedFormatHelper {

    public LongEncodedGoodsWritable getId(JSONArray jsonVertex) throws JSONException,
            IOException {
          return new LongEncodedGoodsWritable(jsonVertex.getLong(0),jsonVertex.getJSONArray(1));
    }

    public LongEncodedVertexValueWritable getValue(JSONArray jsonVertex) throws
            JSONException, IOException {
        return new LongEncodedVertexValueWritable(
                jsonVertex.getDouble(2));
    }

    /*
    * Only creates non conflicting edges
    */
    public Iterable<Edge<LongEncodedGoodsWritable, NullWritable>> getEdges(
            JSONArray jsonVertex) throws JSONException, IOException {

        LongEncodedGoodsWritable sourceVertex=getId(jsonVertex);
        JSONArray jsonEdgeArray = jsonVertex.getJSONArray(3);
        List<Edge<LongEncodedGoodsWritable, NullWritable>> edges =
                Lists.newArrayListWithCapacity(jsonEdgeArray.length());
        for (int i = 0; i < jsonEdgeArray.length(); ++i) {
            JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
            long edgeId=jsonEdge.getLong(0);
            long[] links=new long[jsonEdge.length()-1];
            for (  int j=0; j<links.length; j++ )
                links[j]=jsonEdge.getLong(j+1);
            LongEncodedGoodsWritable encodedGoodsWritable = new LongEncodedGoodsWritable(edgeId,links);
            if (sourceVertex.get().IsNotConflictingWith(encodedGoodsWritable.get()))
                edges.add(EdgeFactory.create(encodedGoodsWritable, NullWritable.get()));
        }
        return edges;
    }

}
