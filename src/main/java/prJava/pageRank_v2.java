package prJava;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank_v2 extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(PageRank_v2.class);
    public static long MaxLinks;
    public static double RandomnessFactor;
    public static String BigConstant;
    public static double RedistributableMass;
    private enum Counters{
        DanglingMass,
    }
    /* This Algorithm will work for TwitterGraph too(if run on EMR)!
    0) Data:
        "1,2","2,3","3,0","4,5","5,6","6,0","7,8","8,9","9,0"
    1) If Node % LinkSize == 0, it contributes to Dummy Node.
    2) If Node % LinkSize == 1, it receives no contribution, except Dangling Mass.
    3) Dangling Mass multiplied by big constant to be stored as counter(long). Every map iteration it will be called
    to redistribute mass.
    4) Joins (thus additional steps) avoided by utilizing single links Circulation.
    5) Declared keys as LongWritable and Dangling Node as "0". LongWritable provides inherent sorting during
    shuffle hence, "0" will come first. We can create a private hashmap in reduce to keep a count of dangling mas
    and avoid Global Counter altogether.
    */
    //-----------------------
    public static void main(String[] args) throws Exception {
        try {
            if (args.length != 4) {
                throw new IllegalArgumentException(
                        "Incorrect number of arguments! " + "Usage: <Input Path> " +
                                "<Output Path> <maxLinks> <Iterations> ");
            }
            ToolRunner.run(new PageRank_v2(), args);
        } catch (final Exception e) {logger.error("", e);}
    }

    public int run(String[] args) throws Exception {
        // Setup and Configurations: Job1
        final Configuration conf = getConf(); // Using JobConf for ChainMapper/Reducer support
        // Define defaults for all jobs
        conf.set("Max Links", args[2]);
        conf.set("Iterations", args[3]);
        conf.set("Randomness", "0.15");
        conf.set("Big Constant","100000000");
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        double redistMass = 0.0; // Total Dangling mass
        // First Job Execution
        final Job job1 = Job.getInstance(conf, "Links-Ranks-Gen");
        final Configuration confJob1 = job1.getConfiguration();
        job1.setJarByClass(PageRank_v2.class);
        job1.setMapperClass(RanksUpdaterMapper.class);
        job1.setReducerClass(ContributionsReducer.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));

        final Configuration iterConf = confJob1;
        // Iterative Execution
        int iter = 1;
        redistMass = job1.getCounters().findCounter(Counters.DanglingMass).getValue();
        while (iter < Integer.parseInt(args[3])) {
            final Job job = Job.getInstance(iterConf, "Page-Rank-"+iter);
            // Reset value of Dangling Mass from Previous Iteration
            iterConf.set("Total Redistributable Mass", String.valueOf(redistMass));
            logger.info("Dangling Mass for Iteration"+(iter-1)+":"+redistMass);
            ChainReducer.addMapper(job, RanksUpdaterMapper.class,
                    Object.class, Text.class, LongWritable.class,
                    Text.class, iterConf);
            ChainReducer.setReducer(job, ContributionsReducer.class,
                    Object.class, Text.class, LongWritable.class,
                    Text.class, iterConf);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            redistMass=(double) job.getCounters().findCounter(Counters.DanglingMass).getValue();
            // Get Counter from previous Iteration
            if (iter==(Integer.parseInt(args[3])-1)){
                FileOutputFormat.setOutputPath(job, new Path(args[1]
                        + String.valueOf(iter)));
                if (!job.waitForCompletion(true)) {
                    System.exit(1);
                }
            }
        }
        return 0;
    }

    public static class RanksUpdaterMapper extends Mapper<Object, Text, LongWritable, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            RedistributableMass=context.getConfiguration().getDouble("Total Redistributable Mass", 0.0);
            MaxLinks = context.getConfiguration().getLong("Max Links",0);
            RandomnessFactor = context.getConfiguration().getDouble("Randomness",0.15);
            BigConstant = context.getConfiguration().get("Big Constant",String.valueOf(Math.pow(10,9)));
        }

        protected void map(final Object nodeKey, final Text value, final Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length!=2) {
                // Iteration Circulation // Format: U,incomingContributions, AdjacencyList
                // U is Placeholder Flag to differentiate from initial input in this combined Mapper
                double updatedRank=0.0;
                String[] outgoingNodes= fields[2].split("-");

                // Rank Update Step
                try {
                    updatedRank = (1 - RandomnessFactor) * (Double.parseDouble(fields[1]) +
                            RedistributableMass / Math.pow(MaxLinks, 2)) + (RandomnessFactor) / Math.pow(MaxLinks, 2);
                } catch (ArithmeticException e) { //if accidentally LinkSize has issue default=0
                    logger.error("ArithmeticException: " + e.getMessage());
                    System.exit(1);
                }

                // Emit PageRank Value, Format: nodeKey, RankVal
                context.write(new LongWritable(Long.parseLong(nodeKey.toString())), new Text("P-"+updatedRank+","));

                for (String node:outgoingNodes) {
                    // Emit Contribution, Format: key:(outNode) val: (inNode,contribution)
                    context.write(new LongWritable(Long.parseLong(node)),new Text(nodeKey+","+updatedRank));

                    // Adjacency Circulation
                    context.write(new LongWritable((long)nodeKey),new Text("O-"+node+","));
                }

                // No incoming Contributions
                if ((int) (Long.parseLong(fields[0]) % MaxLinks) == 1) {
                    //Condition2: Set Contribution to 1st Nodes as its zero
                    context.write(new LongWritable(Long.parseLong(fields[0])), new Text(",Null-0.0"));
                }

                // Updated Contributions
//                context.write(new LongWritable(Long.parseLong(fields[1])), new Text(fields[0] + "," + updatedRank));

            }else{ // Init Phase // input(csv): Node1, Node2
                // Initializing Page Values // Init Probability same for all
                double initRankVal = 1.00 / Math.pow(MaxLinks,2);
                // Emit PageRank Value, Format: inNode, RankVal //Only required for Iteration 10
                context.write(new LongWritable(Long.parseLong(fields[0])), new Text("P-"+initRankVal+","));

                // Condition 4: Adjacency List
                context.write(new LongWritable(Long.parseLong(fields[0])), new Text("O-"+fields[1]+","));

                // Emit Contribution, Format: key:(outNode) val: (inNode,contribution)
                context.write(new LongWritable(Long.parseLong(fields[1])), new Text( fields[0] + "," + initRankVal));

                if (Long.parseLong(fields[0]) % MaxLinks ==1) {
                    // Condition 2: Set Contribution to 1st Nodes as its zero
                    context.write(new LongWritable(Long.parseLong(fields[0])),
                            new Text("Null-0.0,"));
                }
            }
        }
    }

    public static class ContributionsReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            MaxLinks = context.getConfiguration().getLong("Max Links",0);
            BigConstant = context.getConfiguration().get("Big Constant",String.valueOf(Math.pow(10,9)));
        }

        @Override
        protected void reduce(final LongWritable nodeKey, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            double contributions = 0.0;
            HashSet<Long> outNodes = new HashSet<>();
            long mass;
            for (Text val : values) {
                String[] fields = val.toString().split(",");
                if (fields.length != 2) {
                    String[] result= fields[0].split("-");
                    switch(result[0]) {
                        case  "P":
                            break;
                        case "O":
                            // Adjacency List
                            outNodes.add((Long.parseLong(result[1])));
                            break;
                        case "Null":
                            // Only for first nodes
                            contributions = 0.0;
                            break;
                    }
                } else {
                    contributions += Double.parseDouble(fields[1]);
                }
            }

            StringBuilder sb= new StringBuilder();
            // Adjacency List
            if (!outNodes.isEmpty()) {
                for (Long element : outNodes) {
                    sb.append(element).append("-"); // Concatenate each element followed by a comma and space
                }
            }

            // Dangling Mass as Global Counter
            if (nodeKey.get()!=0) {
                context.write(nodeKey, new Text("U," + contributions + "," + sb.toString()));
            }else{
                mass = new BigDecimal(String.valueOf(contributions))
                        .multiply(new BigDecimal(BigConstant)).setScale(0, RoundingMode.UP).longValue();
                context.getCounter(PageRank_v2.Counters.DanglingMass).setValue(mass);
            }
        }
    }
}
