package org.mysamples.io.formats;

/**
 * Created by user on 4/27/14.
 */

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.mysamples.io.types.WDPVertexId;
import org.mysamples.io.types.WDPVertexIdWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * VertexInputFormat that features <code>WDPVertexId</code> vertex ID's,
 * <code>double</code> vertex values and <code>float</code>
 * out-edge weights, and <code>WDPMessage</code> message types,
 *  specified in JSON format.
 */
public class FGAVertexInputFormat extends
        TextVertexInputFormat<WDPVertexIdWritable, DoubleWritable, FloatWritable> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split,
                                               TaskAttemptContext context) {
        return new JsonWDPVertexIdDoubleFloatWDPMessageVertexReader();
    }

    /**
     * VertexReader that features <code>double</code> vertex
     * values and <code>float</code> out-edge weights. The
     * files should be in the following JSON format:
     * JSONArray(<vertex id>, <vertex value>,
     *   JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
     * Here is an example with vertex id 1, vertex value 4.3, and two edges.
     * First edge has a destination vertex 2, edge value 2.1.
     * Second edge has a destination vertex 3, edge value 0.7.
     * [1,4.3,[[2,2.1],[3,0.7]]]
     */
     class JsonWDPVertexIdDoubleFloatWDPMessageVertexReader extends
            TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray,
                    JSONException> {

        @Override
        protected JSONArray preprocessLine(Text line) throws JSONException {
            return new JSONArray(line.toString());
        }

        @Override
        protected WDPVertexIdWritable getId(JSONArray jsonVertex) throws JSONException,
                IOException {
            return GetVertexFromJsonArray(jsonVertex.getJSONArray(0));
        }

        @Override
        protected DoubleWritable getValue(JSONArray jsonVertex) throws
                JSONException, IOException {
            return new DoubleWritable(jsonVertex.getDouble(1));
        }

        /*
        * Only creates non conflicting edges
        */
        @Override
        protected Iterable<Edge<WDPVertexIdWritable, FloatWritable>> getEdges(
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

        @Override
        protected Vertex<WDPVertexIdWritable, DoubleWritable, FloatWritable>
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

