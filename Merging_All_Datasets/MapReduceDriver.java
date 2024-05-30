import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MapReduceDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MapReduceDriver <input path 1> <input path 2> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduceDriver");

        job.setJarByClass(MapReduceDriver.class);
        job.setMapperClass(Mapper1.class);
        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reducer1.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,Mapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Mapper2.class);
 
        TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
