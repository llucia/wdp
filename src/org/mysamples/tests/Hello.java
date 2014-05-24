package org.mysamples.tests;

import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.json.JSONArray;
import org.json.JSONException;
import org.mysamples.io.formats.JsonWDPVertexIdDoubleFloatWDPMessageVertexInputFormat;
import org.mysamples.io.formats.WDPJsonArrayHelper;
import org.mysamples.io.types.WDPVertexId;
import org.mysamples.io.types.WDPVertexIdWritable;

import java.io.IOException;

/**
 * Created by user on 4/27/14.
 */
public class Hello {
    public static void main(String args[]) throws JSONException, IOException {
        System.out.println("hello");
       // WDPJsonArrayHelper jsonHelper=new WDPJsonArrayHelper();
        //TestLeafNode(jsonHelper);
        //TestRootNode(jsonHelper);
      //  TestConflictiveNode(jsonHelper);
        long g1=1;
        long g2=2;

        long x=8;
        long y=1;

       Long check_func= ((x|y)^x)^y;
        System.out.println (check_func==0);
        System.out.println (Long.toBinaryString(x));
        System.out.println (Long.toBinaryString(y));
    }

    private static void TestLeafNode(WDPJsonArrayHelper jsonHelper) throws JSONException, IOException {

        String line = "[[4],40,[]]";
        JSONArray jsonLine = new JSONArray(line);

        WDPVertexIdWritable wdpVertexIdWritable = jsonHelper.getId(jsonLine);
        DoubleWritable vertexValueWritable = jsonHelper.getValue(jsonLine);
        Iterable<Edge<WDPVertexIdWritable, FloatWritable>> edges = jsonHelper.getEdges(jsonLine);

        WDPVertexId wdpVertexId = wdpVertexIdWritable.get();
        WDPVertexId otherWdpVertexId = new WDPVertexId(new long[][]{{4}});
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

    private static void TestRootNode(WDPJsonArrayHelper jsonHelper) throws JSONException, IOException {

        String line = "[[4],40,[[5]]]";
        JSONArray jsonLine = new JSONArray(line);

        WDPVertexIdWritable wdpVertexIdWritable = jsonHelper.getId(jsonLine);
        DoubleWritable vertexValueWritable = jsonHelper.getValue(jsonLine);
        Iterable<Edge<WDPVertexIdWritable, FloatWritable>> edges = jsonHelper.getEdges(jsonLine);

        WDPVertexId wdpVertexId = wdpVertexIdWritable.get();
        WDPVertexId otherWdpVertexId = new WDPVertexId(new long[][]{{4}});
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

    private static void TestConflictiveNode(WDPJsonArrayHelper jsonHelper) throws JSONException, IOException {

        String line = "[[1,2],40,[[3,2]]]";
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
