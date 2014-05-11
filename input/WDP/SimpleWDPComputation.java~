/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Liannet Reyes
 * 31-03-2014
 */

package org.apache.giraph.examples;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Computes the revenue for the winner determination problem (WDP) in Combinatorial Auctions
 */
@Algorithm(
    name = "Simple WDP for Combinatorial Autions",
    description = "Computes the revenue for all vertexes in the graph"
)
public class SimpleWDPComputation extends BasicComputation<
    LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(SimpleWDPComputation.class);

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
      Iterable<DoubleWritable> messages) throws IOException {
   
    double revenue = 0;
    if (getSuperstep() == 0) {
      //send initial value
      revenue= vertex.getValue().get();
    }
    else
    {
	    //send received value Validate messages.size()<=1
	    for (DoubleWritable message : messages) {
	      revenue = message.get();
	    }   
	    
	    //set vertex value
            double newValue=vertex.getValue().get()+revenue; 
	    vertex.setValue(new DoubleWritable(newValue));	      
 	    if (LOG.isDebugEnabled()) {
	      LOG.debug("Vertex " + vertex.getId() + " with value = " + vertex.getValue() +
		  " got newValue = " + newValue);
	    } 
     }
	
     //send message 
     sendMessageToAllEdges(vertex.getId(), new DoubleWritable(revenue));
     if (LOG.isDebugEnabled()) {
	LOG.debug("Vertex " + vertex.getId() + " sending message to all edges Revenue = " + revenue);
      }
     vertex.voteToHalt();
  }
}
