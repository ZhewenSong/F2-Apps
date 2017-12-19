/**
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
 */
package org.apache.tez.examples;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;

/**
 * Simple example to perform Anagram using Tez API's. Anagram is the 
 * HelloWorld program of distributed data processing and counts the number
 * of occurrences of a word in a distributed text data set.
 */
public class Anagram extends TezExampleBase {

  static String INPUT = "Input";
  static String OUTPUT = "Output";
  static String TOKENIZER = "Tokenizer";
  static String SUMMATION = "Summation";
  private static final Logger LOG = LoggerFactory.getLogger(Anagram.class);

  /*
   * Example code to write a processor in Tez.
   * Processors typically apply the main application logic to the data.
   * TokenProcessor tokenizes the input data.
   * It uses an input that provide a Key-Value reader and writes
   * output to a Key-Value writer. The processor inherits from SimpleProcessor
   * since it does not need to handle any advanced constructs for Processors.
   */
  public static class TokenProcessor extends SimpleProcessor {
    // IntWritable one = new IntWritable(1);
    Text val = new Text();
    Text word = new Text();

    public TokenProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);
      // the recommended approach is to cast the reader/writer to a specific type instead
      // of casting the input/output. This allows the actual input/output type to be replaced
      // without affecting the semantic guarantees of the data type that are represented by
      // the reader and writer.
      // The inputs/outputs are referenced via the names assigned in the DAG.
      KeyValueReader kvReader = (KeyValueReader) getInputs().get(INPUT).getReader();
      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(SUMMATION).getWriter();

      long time_start = System.currentTimeMillis();
      LOG.info("F2DATA: input_ready," + System.currentTimeMillis() + "," + TOKENIZER);
      while (kvReader.next()) {
        StringTokenizer itr = new StringTokenizer(kvReader.getCurrentValue().toString());
        while (itr.hasMoreTokens()) {
            int[] arr = new int[256];
            String w = itr.nextToken();
            for (int j = 0; j < w.length(); j++) {
                int index = w.charAt(j);
                arr[index] += 1;
            }
            LOG.info("Tuple: " + Arrays.toString(arr));
            word.set(Arrays.toString(arr));
            val.set(w);
          // Count 1 every time a word is observed. Word is the key a 1 is the value
            LOG.info("Zhewen - In Token Processor - Word: " + word.toString());
            kvWriter.write(word, val);
        }
      }
      long time_finish = System.currentTimeMillis();
      LOG.info("F2DATA: task_done," + time_finish + "," + TOKENIZER);
      LOG.info("F2DATA: task_time," + (time_finish - time_start) / 1000 + "," + TOKENIZER);
    }

  }

  /*
   * Example code to write a processor that commits final output to a data sink
   * The SumProcessor aggregates the sum of individual word counts generated by 
   * the TokenProcessor.
   * The SumProcessor is connected to a DataSink. In this case, its an Output that
   * writes the data via an OutputFormat to a data sink (typically HDFS). Thats why
   * it derives from SimpleMRProcessor that takes care of handling the necessary 
   * output commit operations that makes the final output available for consumers.
   */
  public static class SumProcessor extends SimpleMRProcessor {
    public SumProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      Preconditions.checkArgument(getInputs().size() == 1);
      Preconditions.checkArgument(getOutputs().size() == 1);
      KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT).getWriter();
      // The KeyValues reader provides all values for a given key. The aggregation of values per key
      // is done by the LogicalInput. Since the key is the word and the values are its counts in 
      // the different TokenProcessors, summing all values per key provides the sum for that word.
      KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(TOKENIZER).getReader();

      long time_start = System.currentTimeMillis();
      while (kvReader.next()) {
        Text word = (Text) kvReader.getCurrentKey();
        String sum = "";
        int freq = 0;
        for (Object value : kvReader.getCurrentValues()) {
          sum += value.toString() + " ";
          freq++;
        }
        kvWriter.write(word, new Text(sum));
      }
      long time_finish = System.currentTimeMillis();
      LOG.info("F2DATA: time_compute," + (time_finish - time_start) / 1000 + ",SUMMATION");
      // deriving from SimpleMRProcessor takes care of committing the output
      // It automatically invokes the commit logic for the OutputFormat if necessary.
    }
  }

  private DAG createDAG(TezConfiguration tezConf, String inputPath, String outputPath,
      int numPartitions) throws IOException {

    // Create the descriptor that describes the input data to Tez. Using MRInput to read text 
    // data from the given input path. The TextInputFormat is used to read the text data.
    DataSourceDescriptor dataSource = MRInput.createConfigBuilder(new Configuration(tezConf),
        TextInputFormat.class, inputPath).groupSplits(!isDisableSplitGrouping()).build();

    // Create a descriptor that describes the output data to Tez. Using MROoutput to write text
    // data to the given output path. The TextOutputFormat is used to write the text data.
    DataSinkDescriptor dataSink = MROutput.createConfigBuilder(new Configuration(tezConf),
        TextOutputFormat.class, outputPath).build();

    // Create a vertex that reads the data from the data source and tokenizes it using the 
    // TokenProcessor. The number of tasks that will do the work for this vertex will be decided 
    // using the information provided by the data source descriptor.
    Vertex tokenizerVertex = Vertex.create(TOKENIZER, ProcessorDescriptor.create(
        TokenProcessor.class.getName())).addDataSource(INPUT, dataSource);

    // Create the edge that represents the movement and semantics of data between the producer 
    // Tokenizer vertex and the consumer Summation vertex. In order to perform the summation in 
    // parallel the tokenized data will be partitioned by word such that a given word goes to the 
    // same partition. The counts for the words should be grouped together per word. To achieve this
    // we can use an edge that contains an input/output pair that handles partitioning and grouping 
    // of key value data. We use the helper OrderedPartitionedKVEdgeConfig to create such an
    // edge. Internally, it sets up matching Tez inputs and outputs that can perform this logic.
    // We specify the key, value and partitioner type. Here the key type is Text (for tuple), the 
    // value type is Text (for word) and we using a hash based partitioner. This is a helper
    // object. The edge can be configured by configuring the input, output etc individually without
    // using this helper. The setFromConfiguration call is optional and allows overriding the config
    // options with command line parameters.
    OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
        .newBuilder(Text.class.getName(), Text.class.getName(),
            HashPartitioner.class.getName())
        .setFromConfiguration(tezConf)
        .build();

    // Create a vertex that reads the tokenized data and calculates the sum using the SumProcessor.
    // The number of tasks that do the work of this vertex depends on the number of partitions used 
    // to distribute the sum processing. In this case, its been made configurable via the 
    // numPartitions parameter.
    Vertex summationVertex = Vertex.create(SUMMATION,
        ProcessorDescriptor.create(SumProcessor.class.getName()), numPartitions)
        .addDataSink(OUTPUT, dataSink);

    // No need to add jar containing this class as assumed to be part of the Tez jars. Otherwise 
    // we would have to add the jars for this code as local files to the vertices.
    
    // Create DAG and add the vertices. Connect the producer and consumer vertices via the edge
    DAG dag = DAG.create("Anagram");
    dag.addVertex(tokenizerVertex)
        .addVertex(summationVertex)
        .addEdge(
            Edge.create(tokenizerVertex, summationVertex, edgeConf.createDefaultEdgeProperty()));
    return dag;  
  }

  @Override
  protected void printUsage() {
    System.err.println("Usage: " + " wordcount in out [numPartitions]");
  }

  @Override
  protected int validateArgs(String[] otherArgs) {
    if (otherArgs.length < 2 || otherArgs.length > 3) {
      return 2;
    }
    return 0;
  }

  @Override
  protected int runJob(String[] args, TezConfiguration tezConf,
      TezClient tezClient) throws Exception {
    DAG dag = createDAG(tezConf, args[0], args[1],
        args.length == 3 ? Integer.parseInt(args[2]) : 1);
    LOG.info("Running Anagram");
    return runDag(dag, false, LOG);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Anagram(), args);
    System.exit(res);
  }
}
