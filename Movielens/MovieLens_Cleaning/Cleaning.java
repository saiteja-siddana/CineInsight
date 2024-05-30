import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class Cleaning {
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: Cleaning <input path ratings> <input path movies> <input path links> <output path>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(Cleaning.class);
		job.setMapperClass(CleaningMapper.class);
		job.setReducerClass(CleaningReducer.class);

		job.setNumReduceTasks(1);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[2]));

		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		MultipleOutputs.addNamedOutput(job, "ratings", TextOutputFormat.class, IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "movies", TextOutputFormat.class, IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "links", TextOutputFormat.class, IntWritable.class, Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

