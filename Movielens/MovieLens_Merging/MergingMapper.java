import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MergingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
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
			mergeRatings(line, context);
		} else if (currentFilePath.contains("movies")) {
			mergeMovies(line, context);
		} else if (currentFilePath.contains("links")) {
			mergeLinks(line, context);
		}
	}

	public void mergeRatings(String line, Context context) throws IOException, InterruptedException {
		String[] tokens = line.split(",");
		int movieId = Integer.parseInt(tokens[1]);
		context.write(new IntWritable(movieId), new Text("rating," + tokens[2]));
	}

	public void mergeMovies(String line, Context context) throws IOException, InterruptedException {
		String[] tokens = line.split(",");
		String genres = tokens[tokens.length - 1];
		int movieId = Integer.parseInt(tokens[0]);
		String title = tokens[1];
		for (int i=2; i<tokens.length-1; i++) {
			title += "," + tokens[i];
		}
		context.write(new IntWritable(movieId), new Text("genres," + genres));
		context.write(new IntWritable(movieId), new Text("title," + title));
	}

	public void mergeLinks(String line, Context context) throws IOException, InterruptedException {
		String[] tokens = line.split(",");
		int movieId = Integer.parseInt(tokens[0]);
		int imdbId = Integer.parseInt(tokens[1]);
		int tmdbId = Integer.parseInt(tokens[2]);
		context.write(new IntWritable(movieId), new Text("imdb," + Integer.toString(imdbId)));
		context.write(new IntWritable(movieId), new Text("tmdb," + Integer.toString(tmdbId)));
	}
}
