import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.Text;

public class Profiling {
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: Profiling <input path to cleaned ratings> <input path to cleaned movies> <output path>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(Profiling.class);
		job.setMapperClass(ProfilingMapper.class);
		job.setReducerClass(ProfilingReducer.class);

		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		MultipleOutputs.addNamedOutput(job, "averageratings", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "genrecount", TextOutputFormat.class, Text.class, Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
