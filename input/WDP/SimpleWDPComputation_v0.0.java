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
 * 18-03-2014
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
   
    double newValue =  vertex.getValue().get();
    for (DoubleWritable message : messages) {
      newValue += message.get();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Vertex " + vertex.getId() + " with value = " + vertex.getValue() +
          " got newValue = " + newValue);
    }
    if (newValue > vertex.getValue().get()) {
      vertex.setValue(new DoubleWritable(newValue));
    }
    for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Vertex " + vertex.getId() + " sent to " +
            edge.getTargetVertexId() + " = " + newValue);
      }
      sendMessage(edge.getTargetVertexId(), new DoubleWritable(newValue));
    }    
    vertex.voteToHalt();
  }
}
