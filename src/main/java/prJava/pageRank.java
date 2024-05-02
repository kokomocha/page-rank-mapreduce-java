package prJava;
// Import Java Utils
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
// Import Hadoop Libs
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
// Data types
// Logging
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
// Interface
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class pageRank extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(pageRank.class);

    private enum Counters {
        // Custom counter to complete in 2 jobs instead of 3.
        TotalRecords;
    }

    //-----------------------
    public static void main(String[] args) throws Exception {
        try {
            if (args.length != 3) {
                throw new IllegalArgumentException(
                        "Incorrect number of arguments! Usage: <Output Path> <k> <Iterations>");
            }
            ToolRunner.run(new pageRank(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
    //-----------------------

    public int run(String[] args) throws Exception {
        // Setup and Configurations: Job1
        final Configuration confJob1 = getConf(); // Using JobConf for ChainMapper/Reducer support
        final Job job1 = Job.getInstance(confJob1, "Links-Ranks-Gen");
        confJob1.setLong("Iterations", Long.parseLong(args[2]));
        confJob1.set("mapreduce.output.autoformatting.separator", ",");

        // Execution Job1 : Links-Ranks-Generation
        job1.setJarByClass(pageRank.class);

        // Job1: Read Links, Get count, init PageRanks
//        job1.setMapperClass(LinksMapper.class);
        job1.setNumReduceTasks(0);//Explicitly Setting reducers 0
        ChainMapper.addMapper(job1, IncomingLinksMapper.class,
                Object.class, Text.class, LongWritable.class,
                Text.class, new Configuration(false)); // Links
//        System.out.println("After ChainMapper.addMapper: " + job1.toString());
//        ChainMapper.addMapper(job1, LinksRanksJoinMapper.class,
//                NullWritable.class, NullWritable.class, LongWritable.class,
//                Text.class, new Configuration(false)); //Ranks
//        ChainReducer.setReducer(job1, RankUpdaterReducer.class,NullWritable.class, NullWritable.class, LongWritable.class,
//                Text.class, new Configuration(false)); //Update Ranks
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        return (job1.waitForCompletion(true) ? 0 : 1);
        //job2.setPartitionerClass(GroupedKeyPartitioner.class);
    }

    public static class GroupedKey implements WritableComparable<GroupedKey> {
        private final LongWritable PrimaryKey;
        private final LongWritable SecondaryKey;

        // Custom Constructor
        public GroupedKey(LongWritable PrimaryKey,LongWritable SecondaryKey) {
            this.PrimaryKey = PrimaryKey;
            this.SecondaryKey = SecondaryKey;
        }

        // getters, readFields, and write methods;
        public LongWritable getPrimaryKey() {
            return PrimaryKey;
        }

        public LongWritable getSecondaryKey() {
            return SecondaryKey;
        }

        @Override
        public void write(DataOutput dataOut) throws IOException {
            dataOut.writeLong(PrimaryKey.get());
            dataOut.writeLong(SecondaryKey.get());
        }

        @Override
        public void readFields(DataInput dataIn) throws IOException {
            dataIn.readLong();
            dataIn.readLong();
        }

        @Override
        public int compareTo(GroupedKey other) {
            // Implement comparison logic here
            int keyComparison = (PrimaryKey).compareTo(other.PrimaryKey);
            if (keyComparison == 0) {
                return (SecondaryKey).compareTo(other.SecondaryKey);
            }
            return keyComparison;
        }
    }

    // Custom Partitioner based on Composite Key (Followed Secondary Sort Order)
    public static class GroupedKeyPartitioner extends Partitioner<GroupedKey, Text> {

        @Override
        public int getPartition(GroupedKey key, Text value, int numPartitions) {
            // key.getPrimaryKey() returns LongWritable not long!
            int partitions = (int) key.getPrimaryKey().get() % numPartitions;
            return (partitions+1);
        }
    }

    // Initializing Links from csv file with keys as LongWritable to allow inherent sorting
    public static class IncomingLinksMapper extends Mapper<Object, Text, GroupedKey, IntWritable> {
        @Override
        protected void map(final Object key,final Text value,final Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split(",");
            // Inverted the Nodes for Incoming Links
            LongWritable node2 = new LongWritable(Long.parseLong(nodes[1]));
            LongWritable node1 = new LongWritable(Long.parseLong(nodes[0]));
            // Write Context with 1 to have count of Total records
            context.write(new GroupedKey(node2,node1),new IntWritable(1));
        }
    }

    public static class TotalRecordsReducer extends Reducer<GroupedKey, IntWritable, LongWritable, Double> {
        @Override
        public void reduce(final GroupedKey key, final Iterable<IntWritable> values,
                           final Context context) throws IOException, InterruptedException {
            int count = 0;
            // Only Accumulator Updated
            for(IntWritable val:values){
                count += 1;
            }
            // Get Total Data Records
            context.getCounter(Counters.TotalRecords).increment(count);
        }
    }

    public static class InitRanksMapper extends Mapper<GroupedKey, IntWritable, LongWritable, Double> {
        @Override
        public void map(final GroupedKey key,final IntWritable values,
                           final Context context) throws IOException, InterruptedException {
            // Note: this is just for initializing PageRanks
            // Links and Ranks join occurs in next modular block for repetition
            try {
                context.write(key.getSecondaryKey(),
                        1.00/context.getCounter(Counters.TotalRecords).getValue());
            } catch (ArithmeticException e) {
                logger.error("Error: " + e.getMessage());
            }
        }
    }

    public static class PageRanksReducer extends Reducer<LongWritable, LongWritable, LongWritable, Double> {

    }

    public static class RanksUpdationMapper extends Mapper<GroupedKey, IntWritable, LongWritable, Double> {
        @Override
        public void map(final GroupedKey key,final IntWritable values,
                        final Context context) throws IOException, InterruptedException {
            // Note: this is just for initializing PageRanks
            // Links and Ranks join occurs in next modular block for repetition
            try {
                context.write(key.getSecondaryKey(),
                        1.00/context.getCounter(Counters.TotalRecords).getValue());
            } catch (ArithmeticException e) {
                logger.error("Error: " + e.getMessage());
            }
        }
    }
}