package org.mysamples.tests;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.json.JSONArray;
import org.json.JSONException;
import org.mysamples.io.formats.DMALongEncodedFormatHelper;
import org.mysamples.io.formats.FGAFormatHelper;
import org.mysamples.io.formats.WDPJsonArrayHelper;
import org.mysamples.io.types.*;

import java.io.IOException;

/**
 * Created by user on 4/27/14.
 */
public class DMALongEncodedHeperTest {
    public static void main(String args[]) throws JSONException, IOException {
        System.out.println("fga parser test");
        DMALongEncodedFormatHelper jsonHelper=new DMALongEncodedFormatHelper();
        TestLeafNode(jsonHelper);
       // TestLeafNode1(jsonHelper);
        //TestRootNode(jsonHelper);

    }

    private static void TestLeafNode(DMALongEncodedFormatHelper jsonHelper) throws JSONException, IOException {

        String line = "[7,[512],0,[]]";
        JSONArray jsonLine = new JSONArray(line);

        LongEncodedGoodsWritable wdpVertexIdWritable = jsonHelper.getId(jsonLine);
        LongEncodedVertexValueWritable vertexValueWritable = jsonHelper.getValue(jsonLine);
        Iterable<Edge<LongEncodedGoodsWritable, NullWritable>> edges = jsonHelper.getEdges(jsonLine);

        EncodedGoods wdpVertexId = wdpVertexIdWritable.get();
        EncodedGoods otherWdpVertexId = new EncodedGoods(new long[]{512});
        boolean testVertexId = wdpVertexId.equals(otherWdpVertexId);
        boolean testVertexValue = vertexValueWritable.get() == 0;
        int count = 0;
        boolean testEdges;
        for (Edge<LongEncodedGoodsWritable, NullWritable> edge : edges) {
            count++;
        }
        testEdges = count == 0;

        if (testVertexId && testVertexValue && testEdges)
            System.out.println("test root node successfully passed!");
        else
            System.out.println("test root node failed!");

    }

    private static void TestLeafNode1(FGAFormatHelper jsonHelper) throws JSONException, IOException {

        String line = "[[[1,2],[3,4]],40,[]]";
        JSONArray jsonLine = new JSONArray(line);

        WDPVertexIdWritable wdpVertexIdWritable = jsonHelper.getId(jsonLine);
        DoubleWritable vertexValueWritable = jsonHelper.getValue(jsonLine);
        Iterable<Edge<WDPVertexIdWritable, FloatWritable>> edges = jsonHelper.getEdges(jsonLine);

        WDPVertexId wdpVertexId = wdpVertexIdWritable.get();
        WDPVertexId otherWdpVertexId = new WDPVertexId(new long[][]{{1,2},{3,4}});
        boolean testVertexId = wdpVertexId.equals(otherWdpVertexId);
        boolean testVertexValue = vertexValueWritable.get() == 40;
        int count = 0;
        boolean testEdges;
        for (Edge<WDPVertexIdWritable, FloatWritable> edge : edges) {
            count++;
        }
        testEdges = count == 0;

        if (testVertexId && testVertexValue && testEdges)
            System.out.println("test root node successfully passed!");
        else
            System.out.println("test root node failed!");

    }

    private static void TestRootNode(DMALongEncodedFormatHelper jsonHelper) throws JSONException, IOException {

        String line = "[2,[6],10,[[3,24],[4,40],[5,256]]]";
        JSONArray jsonLine = new JSONArray(line);

        LongEncodedGoodsWritable wdpVertexIdWritable = jsonHelper.getId(jsonLine);
        LongEncodedVertexValueWritable vertexValueWritable = jsonHelper.getValue(jsonLine);
        Iterable<Edge<LongEncodedGoodsWritable, NullWritable>> edges = jsonHelper.getEdges(jsonLine);

        EncodedGoods wdpVertexId = wdpVertexIdWritable.get();
        EncodedGoods otherWdpVertexId = new EncodedGoods(new long[]{6});
        boolean testVertexId = wdpVertexId.equals(otherWdpVertexId);
        boolean testVertexValue = vertexValueWritable.get() == 10;
        int count = 0;
        boolean testEdges;
        for (Edge<LongEncodedGoodsWritable, NullWritable> edge : edges) {
            count++;
        }
        testEdges = count == 3;

        if (testVertexId && testVertexValue && testEdges)
            System.out.println("test root node successfully passed!");
        else
            System.out.println("test root node failed!");

    }

    private static void TestConflictiveNode(FGAFormatHelper jsonHelper) throws JSONException, IOException {

        String line = "[[[1,2,3],[13,12]],40,[[[1,2,3],[13,12],[11]],[[1,2,3],[13,12],[11],[5,2,4]]]]";


        JSONArray jsonLine = new JSONArray(line);

        WDPVertexIdWritable wdpVertexIdWritable = jsonHelper.getId(jsonLine);
        DoubleWritable vertexValueWritable = jsonHelper.getValue(jsonLine);
        Iterable<Edge<WDPVertexIdWritable, FloatWritable>> edges = jsonHelper.getEdges(jsonLine);

        WDPVertexId wdpVertexId = wdpVertexIdWritable.get();
        WDPVertexId otherWdpVertexId = new WDPVertexId(new long[][]{{1,2,3},{13,12}});
        boolean testVertexId = wdpVertexId.equals(otherWdpVertexId);
        boolean testVertexValue = vertexValueWritable.get() == 40;
        int count = 0;
        boolean testEdges;
        for (Edge<WDPVertexIdWritable, FloatWritable> edge : edges) {
            count++;
        }
        testEdges = count == 1;

        if (testVertexId && testVertexValue && testEdges)
            System.out.println("test root node successfully passed!");
        else
            System.out.println("test root node failed!");

    }

    private static void TestComplexNode(WDPJsonArrayHelper jsonHelper) throws JSONException, IOException {

        String line = "[[1,2],40,[[3,4]]]";
        JSONArray jsonLine = new JSONArray(line);

        WDPVertexIdWritable wdpVertexIdWritable = jsonHelper.getId(jsonLine);
        DoubleWritable vertexValueWritable = jsonHelper.getValue(jsonLine);
        Iterable<Edge<WDPVertexIdWritable, FloatWritable>> edges = jsonHelper.getEdges(jsonLine);

        WDPVertexId wdpVertexId = wdpVertexIdWritable.get();
        WDPVertexId otherWdpVertexId = new WDPVertexId(new long[][]{{1,2}});
        boolean testVertexId = wdpVertexId.equals(otherWdpVertexId);
        boolean testVertexValue = vertexValueWritable.get() == 40;
        int count = 0;
        boolean testEdges;
        for (Edge<WDPVertexIdWritable, FloatWritable> edge : edges) {
            count++;
        }
        testEdges = count == 1;

        if (testVertexId && testVertexValue && testEdges)
            System.out.println("test root node successfully passed!");
        else
            System.out.println("test root node failed!");

    }

}
