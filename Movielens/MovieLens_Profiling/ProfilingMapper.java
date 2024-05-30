import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ProfilingMapper extends Mapper<LongWritable, Text, Text, Text> {
	String currentFilePath;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		currentFilePath = fileSplit.getPath().toString();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		if (currentFilePath.contains("ratings")) {
			profileRatings(line, context);
		} else if (currentFilePath.contains("movies")) {
			profileMovies(line, context);
		}
	}

	public void profileRatings(String line, Context context) throws IOException, InterruptedException {
		String[] tokens = line.split(",");
		String movieId = String.format("%06d", Integer.parseInt(tokens[1]));
		context.write(new Text(movieId), new Text(tokens[2]));
	}

	public void profileMovies(String line, Context context) throws IOException, InterruptedException {
		String[] tokens = line.split(",");
		String genres = tokens[tokens.length - 1];
		String[] genre = genres.split("\\|");

		for (int i=0; i<genre.length; i++) {
			context.write(new Text(genre[i]), new Text("1"));
		}
	}
}
