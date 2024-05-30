import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ProfilingReducer extends Reducer<Text, Text, Text, Text> {
	MultipleOutputs<Text, Text> multipleOutputs;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<>(context);
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		if (key.toString().matches("\\d+")) {
			float rating = 0;
			for (Text value : values) {
				count++;
				try {
					rating += Float.parseFloat(value.toString());
				} catch (NumberFormatException e) {
				}
			}
			float avgRating = rating/count;
			String movieId = key.toString();
			movieId = movieId.replaceFirst("^0+", "");
			multipleOutputs.write(new Text(movieId), new Text(Float.toString(avgRating)), "averageratings/part");
		} else {
			for (Text value : values) {
				count++;
			}
			multipleOutputs.write(key, new Text(Integer.toString(count)), "genrecount/part");
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}

}
