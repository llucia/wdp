package org.mysamples.io.formats;

/**
 * Created by user on 4/27/14.
 */

import com.google.common.collect.Lists;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.mysamples.io.types.*;

import java.io.IOException;
import java.util.List;

/**
 * VertexInputFormat that features <code>WDPVertexId</code> vertex ID's,
 * <code>double</code> vertex values and <code>float</code>
 * out-edge weights, and <code>WDPMessage</code> message types,
 *  specified in JSON format.
 */
public class DMALongEncodedVertexInputFormat extends
        TextVertexInputFormat<LongEncodedGoodsWritable, LongEncodedVertexValueWritable, NullWritable> {

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
        protected LongEncodedGoodsWritable getId(JSONArray jsonVertex) throws JSONException,
                IOException {
            return new LongEncodedGoodsWritable(jsonVertex.getLong(0),jsonVertex.getJSONArray(1));
        }

        @Override
        protected LongEncodedVertexValueWritable getValue(JSONArray jsonVertex) throws
                JSONException, IOException {
            return new LongEncodedVertexValueWritable(
                    jsonVertex.getDouble(2));
        }

        /*
        * Only creates non conflicting edges
        */
        @Override
        protected Iterable<Edge<LongEncodedGoodsWritable, NullWritable>> getEdges(
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

        @Override
        protected Vertex<LongEncodedGoodsWritable, LongEncodedVertexValueWritable, NullWritable>
        handleException(Text line, JSONArray jsonVertex, JSONException e) {
            throw new IllegalArgumentException(
                    "Couldn't get WDP encoded vertex from line " + line, e);
        }

    }
}

