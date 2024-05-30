import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public  class CleaningMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	String currentFileName;
	boolean isHeader;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		currentFileName = fileSplit.getPath().getName();
		isHeader = true;
	}


	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (isHeader) {
			isHeader = false;
			return;
		}
		String line = value.toString();

		if (currentFileName.equalsIgnoreCase("ratings.csv")) {
			cleanRatings(line, context);
		} else if (currentFileName.equalsIgnoreCase("movies.csv")) {
			cleanMovies(line, context);
		} else if (currentFileName.equalsIgnoreCase("links.csv")) {
			cleanLinks(line,context);
		}
	}

	public void cleanRatings(String line, Context context) throws IOException, InterruptedException {
		String[] tokens = line.split(",");
		if (tokens.length == 4) {
			try {
				int userId = Integer.parseInt(tokens[0]);
				int movieId = Integer.parseInt(tokens[1]);
				float rating = Float.parseFloat(tokens[2]);
				context.write(new IntWritable(userId), new Text("ratings" + "," + tokens[1] + "," + tokens[2]));

			} catch (NumberFormatException e) {
			}
		}
	}

	public void cleanMovies(String line, Context context) throws IOException, InterruptedException {
		String[] tokens = line.split(",");
		if (tokens.length >= 3) {
			try {
				int movieId = Integer.parseInt(tokens[0]);
				String genres = tokens[tokens.length - 1];
				String title = "";

				for (int i = 1; i < tokens.length - 1; i++) {
					if (i>1) {
						title += ",";
					}
					title += tokens[i];
				}
				context.write(new IntWritable(movieId), new Text(tokens[0] + "," + title + "," + genres));
			} catch (NumberFormatException e) {
			}
		}
	}

	public void cleanLinks(String line, Context context) throws IOException, InterruptedException {
		String[] tokens = line.split(",");
		if (tokens.length == 3) {
			try {
				int movieId = Integer.parseInt(tokens[0]);
				int imdbId = Integer.parseInt(tokens[1]);
				int tmdbId = Integer.parseInt(tokens[2]);
				context.write(new IntWritable(movieId), new Text("links" + "," + tokens[1] + "," + tokens[2]));
			} catch (NumberFormatException e) {
			}
		}
	}
}
