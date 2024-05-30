import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer2 extends Reducer< Text, Text, Text, Text> {
    private String mostPopularMovie ;
    private String highestRatedMovie ;
    private double maxPopularity = Double.MIN_VALUE;
    private double maxRating = Double.MIN_VALUE;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            double popularity = Double.parseDouble(parts[1].trim());
            double rating = Double.parseDouble(parts[0].trim());

            // Check for most popular movie
            if (popularity > maxPopularity) {
                maxPopularity = popularity;
                mostPopularMovie = key.toString();
            }

            // Check for highest-rated movie
            if (rating > maxRating) {
                maxRating = rating;
                highestRatedMovie = key.toString();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output the most popular and highest-rated movies
        context.write(new Text("Most Popular Movie:"), new Text( mostPopularMovie+"-"+String.valueOf(maxPopularity) ));
        context.write(new Text("Highest Rated Movie:"), new Text( highestRatedMovie+"-"+String.valueOf(maxRating) ));
    }
}

