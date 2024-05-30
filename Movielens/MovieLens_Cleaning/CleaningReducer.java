import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CleaningReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
	MultipleOutputs<NullWritable, Text> multipleOutputs;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<>(context);
	}

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text valueText : values) {
			String value = valueText.toString();
			String[] tokens = value.split(",");
			if (!tokens[0].equals("ratings") && !tokens[0].equals("links")) {
				multipleOutputs.write(NullWritable.get(), new Text(value), "movies/part");
			}
			else {
				multipleOutputs.write(NullWritable.get(), new Text(key.toString() + "," + tokens[1] + "," + tokens[2]), tokens[0]+"/part");
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}

