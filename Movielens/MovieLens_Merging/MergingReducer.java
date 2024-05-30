import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MergingReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		float rating = 0;
		float avgRating = 0;
		String genres = "";
		String title = "";
		String imdbId = "";
		String tmdbId = "";
		for (Text value : values) {
			String line = value.toString();
			String[] tokens = line.split(",");
			if (tokens[0].equals("rating")) {
				count++;
				rating += Float.parseFloat(tokens[1]);
			} else if (tokens[0].equals("genres")) {
				genres += tokens[1];
			} else if (tokens[0].equals("title")) {
				title = tokens[1];
				for (int i=2; i<tokens.length; i++) {
					title += "," + tokens[i];
				}
			} else if (tokens[0].equals("imdb")) {
				imdbId += tokens[1];
			} else if (tokens[0].equals("tmdb")) {
				tmdbId += tokens[1];
			}
		}
		avgRating = rating/count;
		String movieId = key.toString();
		context.write(NullWritable.get(), new Text(movieId + "," + imdbId + "," + tmdbId + "," + title + "," + genres + "," 
					+ Float.toString(avgRating) + "," + Integer.toString(count)));
	}
}
