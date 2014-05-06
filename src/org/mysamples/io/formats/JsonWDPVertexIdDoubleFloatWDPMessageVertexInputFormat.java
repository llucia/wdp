package org.mysamples.io.formats;

/**
 * Created by user on 4/27/14.
 */

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;
import org.mysamples.io.types.WDPVertexId;
import org.mysamples.io.types.WDPVertexIdWritable;

import java.io.IOException;
import java.util.List;

/**
 * VertexInputFormat that features <code>WDPVertexId</code> vertex ID's,
 * <code>double</code> vertex values and <code>float</code>
 * out-edge weights, and <code>WDPMessage</code> message types,
 *  specified in JSON format.
 */
public class JsonWDPVertexIdDoubleFloatWDPMessageVertexInputFormat extends
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
            return new WDPVertexIdWritable(jsonVertex.getJSONArray(0));
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
                WDPVertexIdWritable wdpVertexIdWritable = new WDPVertexIdWritable(jsonEdge);
                if (sourceVertex.get().IsNotConflictingWith(wdpVertexIdWritable.get()))
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

    }
}

