package org.mysamples.tests;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.json.JSONArray;
import org.json.JSONException;
import org.mysamples.io.formats.FGAFormatHelper;
import org.mysamples.io.formats.WDPJsonArrayHelper;
import org.mysamples.io.types.WDPVertexId;
import org.mysamples.io.types.WDPVertexIdWritable;

import java.io.IOException;

/**
 * Created by user on 4/27/14.
 */
public class FGAParserTest {
    public static void main(String args[]) throws JSONException, IOException {
        System.out.println("fga parser test");
        FGAFormatHelper jsonHelper=new FGAFormatHelper();
        //TestLeafNode(jsonHelper);
       // TestLeafNode1(jsonHelper);
        //TestRootNode(jsonHelper);
        TestConflictiveNode(jsonHelper);

    }

    private static void TestLeafNode(FGAFormatHelper jsonHelper) throws JSONException, IOException {

        String line = "[[[1,2]],40,[]]";
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

    private static void TestRootNode(FGAFormatHelper jsonHelper) throws JSONException, IOException {

        String line = "[[[4],[5,6],[7,8,9]],40,[[[1,2,3],[13,12]],[[11]]]]";
        JSONArray jsonLine = new JSONArray(line);

        WDPVertexIdWritable wdpVertexIdWritable = jsonHelper.getId(jsonLine);
        DoubleWritable vertexValueWritable = jsonHelper.getValue(jsonLine);
        Iterable<Edge<WDPVertexIdWritable, FloatWritable>> edges = jsonHelper.getEdges(jsonLine);

        WDPVertexId wdpVertexId = wdpVertexIdWritable.get();
        WDPVertexId otherWdpVertexId = new WDPVertexId(new long[][]{{4},{5,6},{7,8,9}});
        boolean testVertexId = wdpVertexId.equals(otherWdpVertexId);
        boolean testVertexValue = vertexValueWritable.get() == 40;
        int count = 0;
        boolean testEdges;
        for (Edge<WDPVertexIdWritable, FloatWritable> edge : edges) {
            count++;
        }
        testEdges = count == 2;

        if (testVertexId && testVertexValue && testEdges)
            System.out.println("test root node successfully passed!");
        else
            System.out.println("test root node failed!");

    }

    private static void TestConflictiveNode(FGAFormatHelper jsonHelper) throws JSONException, IOException {

        String line = "[[[4],[5,6],[7,8,9]],40,[[[1,2,3],[8,12]],[[11]]]]";

        JSONArray jsonLine = new JSONArray(line);

        WDPVertexIdWritable wdpVertexIdWritable = jsonHelper.getId(jsonLine);
        DoubleWritable vertexValueWritable = jsonHelper.getValue(jsonLine);
        Iterable<Edge<WDPVertexIdWritable, FloatWritable>> edges = jsonHelper.getEdges(jsonLine);

        WDPVertexId wdpVertexId = wdpVertexIdWritable.get();
        WDPVertexId otherWdpVertexId = new WDPVertexId(new long[][]{{4},{5,6},{7,8,9}});
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
