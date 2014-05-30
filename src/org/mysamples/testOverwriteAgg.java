package org.mysamples;

import org.apache.giraph.aggregators.*;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;
import org.apache.giraph.master.DefaultMasterCompute;
import java.io.IOException;


public class testOverwriteAgg extends BasicComputation<
        LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {

    /** Class logger */
    private static final Logger LOG =
            Logger.getLogger(testOverwriteAgg.class);

    /** ACO_STATE AGG NAME */
    private static String ACO_STATE = "acostate";

    @Override
    public void compute(
            Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
            Iterable<DoubleWritable> messages) throws IOException {

        long k = this.getSuperstep();

        if (k==0) {
            System.out.println("ACOSTATE. Original value=" + getAggregatedValue(ACO_STATE) + " New value=" + k);
            aggregate(ACO_STATE, new IntWritable(5));
        }

        if (k==1) {
            System.out.println("ACOSTATE. Final value=" + getAggregatedValue(ACO_STATE));
        }
        if(k>1)
        {
            System.out.println("ACOSTATE. Final value=" + getAggregatedValue(ACO_STATE));
            vertex.voteToHalt();
        }

    }

    public static class testOverwriteAggMC extends
            DefaultMasterCompute {
        @Override
        public void initialize() throws InstantiationException,
                IllegalAccessException {

            registerPersistentAggregator(ACO_STATE, IntOverwriteAggregator.class);

        }
        @Override
        public void  compute(){
            System.out.println(getAggregatedValue(ACO_STATE));
        }
    }
}
