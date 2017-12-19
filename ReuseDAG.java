package org.apache.tez.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ReuseDAG extends TezExampleBase {

        static String INPUT = "Table";
        static String INPUT1 = "input1";
        static String OUTPUT = "Output";
        static String TOKENIZER = "Tokenizer";
        static String TOKENIZER1 = "Tokenizer1";
        static String SUMMATION = "Summation";
        //static String ORDERING = "3rdStageOrdering";

        private static final Logger LOG = LoggerFactory.getLogger(ReuseDAG.class);

        @Override
        protected void printUsage() {
                System.err.println("Usage: " + " robertdag in out [numPartitions]");
        }

        @Override
        protected int validateArgs(String[] otherArgs) {
                if (otherArgs.length < 1 || otherArgs.length > 3) {
                        return 2;
                }
                return 0;
        }

       
        // A processor is something which consumes {@link LogicalInput}s and produces
        // {@link LogicalOutput}s. User has to implement the logic.
        public static class TokenProcessor extends SimpleProcessor {
                IntWritable one = new IntWritable(1);
                Text word = new Text();

                public TokenProcessor(ProcessorContext context) {
                        super(context);
                }

                @Override
                public void run() throws Exception {
                        LOG.info("Reuse - TokenProcessor - start processing logic");

                        Preconditions.checkArgument(getInputs().size() == 1);
                        Preconditions.checkArgument(getOutputs().size() == 1);

                        // the recommended approach is to cast the reader/writer to a specific type instead
                        // of casting the input/output. This allows the actual input/output type to be replaced
                        // without affecting the semantic guarantees of the data type that are represented by
                        // the reader and writer.
                        // The inputs/outputs are referenced via the names assigned in the DAG.
                        KeyValueReader kvReader = (KeyValueReader)getInputs().get(INPUT).getReader();
                        LOG.info("Reuse - got a reader instance from INPUT");
                        KeyValueWriter kvWriter = (KeyValueWriter)getOutputs().get(SUMMATION).getWriter();
                        LOG.info("Reuse - got a writer instance for SUMMATION");
                        int numKeysVals = 0;
                        while (kvReader.next()) {
                                StringTokenizer itrK = new StringTokenizer(kvReader.getCurrentKey().toString());
                                //while (itrK.hasMoreTokens()) {
                                //      LOG.info("Reuse - itrK:" + itrK.nextToken());
                                //}

                                StringTokenizer itr = new StringTokenizer(kvReader.getCurrentValue().toString());
                                while (itr.hasMoreTokens()) {
                                        String value = itr.nextToken();
                                        //LOG.info("Reuse - value:" + value);
                                        word.set(value);
                                        // Count 1 every time a word is observed. Word is the key a 1 is the value
                                        kvWriter.write(word, one);
                                        numKeysVals++;
                                }
                        }
                        LOG.info("Reuse - wrote: " + numKeysVals + " key/val pairs");
                        LOG.info("Reuse - TokenProcessor - end processing logic");
                }
        }


        /*
         * Example code to write a processor that commits final output to a data sink.
         * The SumProcessor aggregates the sum of individual word counts generated by the TokenProcessor.
         * The SumProcessor is connected to a DataSink. In this case, its an Output that writes the data via
         * an OutputFormat to a data sink (typically HDFS). That's why it derives from SimpleMRProcessor
         * that takes care of handling the necessary output commit operations that makes the final output
         * available for consumers.
         */
        public static class SumProcessor extends SimpleMRProcessor {

                public SumProcessor(ProcessorContext context) {
                        super(context);
                }

                @Override
                public void run() throws Exception {
                        LOG.info("Reuse - SumProcessor - start processing logic");
                        Preconditions.checkArgument(getInputs().size() == 1);
                        Preconditions.checkArgument(getOutputs().size() == 1);
                        LOG.info("Reuse - get a reader instance from TOKENIZER");
                        KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(TOKENIZER).getReader();
                        
                        LOG.info("Reuse - get a writer instance for ORDERING");
                        KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT/*ORDERING*/).getWriter();
                        // The KeyValues reader provides all values for a given key. The aggregation of values
                        // per key is done by the Logicalinput. Since the key is the work and the values are its
                        // counts in the different TokenProcessors, summing all values per key provides the sum
                        // for that word.

                        int numKeys = 0;
                        int numTotalValues = 0;
                        Text word = null;
                        while (kvReader.next()) {
                                word = (Text) kvReader.getCurrentKey();
                                int sum = 0;
                                numKeys++;
                                //LOG.info("Reuse - for word:" + word);
                                for (Object value : kvReader.getCurrentValues()) {
                                        sum += ((IntWritable) value).get();
                                        //LOG.info("\tReuse - sum: " + sum);
                                        numTotalValues++;
                                }
                                //kvWriter.write(word, new IntWritable(sum));
                                // write the sum as the key and the word as the value
                                kvWriter.write(new IntWritable(sum), word);
                        }
                        LOG.info("Reuse - SumProcessor - end processing logic");
                        LOG.info("Reuse - nnumKeys:" + numKeys + " nnumTotalValues:" + numTotalValues + " lastWord:" + word);
                }
        }

        // No-op sorter processor. It does not need to apply any logic since the ordered partitioned
        // edge ensures that we get the data sorted and grouped by the sum key.
        public static class NoOpProcessor extends SimpleMRProcessor {

                public NoOpProcessor(ProcessorContext context) {
                        super(context);
                }

                @Override
                public void run() throws Exception {
                        LOG.info("Reuse - NoOpProcessor - start processing logic");
                        Preconditions.checkArgument(getInputs().size() == 1);
                        Preconditions.checkArgument(getOutputs().size() == 1);
                        LOG.info("Reuse - get a reader instance from SUMMATION");
                        // This one will do all the shuffle actually -> here
                        KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(SUMMATION).getReader();

                        LOG.info("Reuse - get a writer instance for OUTPUT");
                        KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT).getWriter();
                        while (kvReader.next()) {
                                Object sum = kvReader.getCurrentKey();
                                for (Object word : kvReader.getCurrentValues()) {
                                        kvWriter.write(word, sum);
                                }
                        }
                        LOG.info("Reuse - NoOpProcessor - end processing logic");
                }
        }

        private DAG createDAG(TezConfiguration tezConf, String inputPath,
                String outputPath, int numPartitions) throws IOException {

                LOG.info("Reuse - createDAG with inputPath:" + inputPath
                        + " outputPath:" + outputPath
                        + " numPartitions:" + numPartitions);

                // Create the descriptor that describes the input data to Tez.
                // Use MRInput to read text data from the given input path.
                // The TextInputFormat is used to read the text data.
                DataSourceDescriptor dataSource = MRInput.createConfigBuilder(new Configuration(tezConf),
                        TextInputFormat.class, inputPath).groupSplits(!isDisableSplitGrouping()).build();

                LOG.info("Reuse - number of shards in dataSource:" + dataSource.getNumberOfShards());


                // Create a descriptor that describes the output data to Tez.
                // Use MROutput to write text data to the given output path.
                // The TextOutputFormat is used to write the text data.
                DataSinkDescriptor dataSink = MROutput.createConfigBuilder(new Configuration(tezConf),
                        TextOutputFormat.class, outputPath).build();

                // Create a vertex that reads the data from the data source and tokenizes it using the
                // TokenProcessor. The number of tasks that will do the work for this vertex will be decided
                // using the information provided by the data source descriptor.
                ProcessorDescriptor processorSource = ProcessorDescriptor.create(TokenProcessor.class.getName());
                Vertex sourceVertex = Vertex.create(TOKENIZER, processorSource);
                sourceVertex.addDataSource(INPUT, dataSource);

                
                // Create a vertex that reads the tokenized data and calculates the sum using the SumProcessor.
                // The number of tasks that do the work of this vertex depends on the number of partitions
                // used to distribute the sum processing. In this case, its been made configurable via the
                // numParititons parameter.
                ProcessorDescriptor processorDest = ProcessorDescriptor.create(SumProcessor.class.getName());
                Vertex destinationVertex = Vertex.create(SUMMATION, processorDest, numPartitions);
                destinationVertex.addDataSink(OUTPUT, dataSink);

                // Add Sorting vertex
//              ProcessorDescriptor processorFinal = ProcessorDescriptor.create(NoOpProcessor.class.getName());
//              Vertex finalVertex = Vertex.create(ORDERING, processorFinal, 1);
//              finalVertex.addDataSink(OUTPUT, dataSink);

                // Create the edge that represents the movement and semantics of data between the producer
                // Tokenizer vertex and the consumer Summation vertex. In order to perform the summation in
                // parallel the tokenizer data will be partitioned by word such that a given word goes to the
                // same partition. The counts for the words should be grouped together per word. To achieve
                // this we can use an edge that contains an input/output pair that handles partitioning and
                // grouping of key value data. We use the helper OrderedPartitionedKVEdgeConfig to create
                // such an edge. Internally, it sets up matching Tez inputs and outputs that can perform this
                // logic. We specify the key, value and partitioner type. Here the key type is Text (for word)
                // , the value type is IntWritable (for count) and we using a hash based partitioner. This is
                // a helper object. The edge can be configured by configuring the input, output etc individually
                // without using this helper. The setFromConfiguration call is optional and allows overriding
                // the config options with command line parameters.
                OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
                        .newBuilder(Text.class.getName(), IntWritable.class.getName(),
                                HashPartitioner.class.getName()).setFromConfiguration(tezConf).build();

                // Add the other edge
                OrderedPartitionedKVEdgeConfig edge2Conf = OrderedPartitionedKVEdgeConfig
                                .newBuilder(IntWritable.class.getName(), Text.class.getName(),
                                        HashPartitioner.class.getName()).setFromConfiguration(tezConf).build();


                // No need to add jar containing this class as assumed to be part of the Tez jars. Otherwise
                // we would have to add the jars for this code as local files to the vertices.

                // Create DAG and add the vertices. Connect the producer and consumer vertices via the edge.
                DAG dag = DAG.create("ReuseDag");
                dag.addVertex(sourceVertex)
                   .addVertex(destinationVertex)
                   //.addVertex(finalVertex)
                   .addEdge(
                           Edge.create(sourceVertex, destinationVertex, edgeConf.createDefaultEdgeProperty()));
//                 .addEdge(
//                         Edge.create(destinationVertex, finalVertex, edge2Conf.createDefaultEdgeProperty()));

                return dag;
        }

        @Override
        protected int runJob(String[] args, TezConfiguration tezConf, TezClient tezClient) throws Exception {
                LOG.info("Reuse - start job from runJob");
                DAG dag = createDAG(tezConf, args[0], args[1],
                                args.length == 3 ? Integer.parseInt(args[2]) : 1);
                LOG.info("Running Reuse DAG");
                return runDag(dag, false, LOG);
        }

        public static void main(String[] args) throws Exception {
                LOG.info("Reuse - start job from main");
                int res = ToolRunner.run(new Configuration(), new ReuseDAG(), args);
                System.exit(res);
        }
}
