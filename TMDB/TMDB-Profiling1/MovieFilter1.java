import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieFilter1 {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MovieFilter <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(MovieFilter1.class);
        job.setJobName("Movie Filter");
        job.setNumReduceTasks(1); // No reducer phase

    	FileInputFormat.addInputPath(job, new Path(args[0]));
   	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
	job.setMapperClass(MovieMapper1.class);
        job.setReducerClass(MovieReducer1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

