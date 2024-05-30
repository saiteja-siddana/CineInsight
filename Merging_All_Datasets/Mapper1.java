import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<Object, Text, IntWritable, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        String res="";
        String tmdb_id = tokens[0];
        String genres = tokens[1];
        String ml_avg_rating = tokens[2];
        String ml_votes = tokens[3];
        String title = tokens[4];
        String imdb_rating = tokens[5];
        String imdb_votes = tokens[6];
        String year = tokens[7];
        String runtime = tokens[8];

        res += "A\t" + title + "\t" + genres + "\t" + year + "\t" + runtime + "\t" + ml_avg_rating + "\t" + ml_votes + "\t" + imdb_rating + "\t" + imdb_votes;

        context.write(new IntWritable(Integer.parseInt(tmdb_id)), new Text(res));
    }
}