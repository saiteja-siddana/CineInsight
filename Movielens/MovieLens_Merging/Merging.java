import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class Merging {
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: Merging <input path to cleaned ratings> <input path to cleaned movies> <input path to cleaned links> <output path>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		job.setJarByClass(Merging.class);
		job.setMapperClass(MergingMapper.class);
		job.setReducerClass(MergingReducer.class);

		job.setNumReduceTasks(1);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[2]));

		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
