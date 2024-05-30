import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Object, Text, IntWritable, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String res="";
        String tmdb_id = tokens[0].trim();
        String popularity = tokens[1];
        String tmdb_votes = tokens[2];
        String tmdb_rating = tokens[3];

        res += tmdb_rating + "\t" + tmdb_votes + "\t" + popularity;
        context.write(new IntWritable(Integer.parseInt(tmdb_id)), new Text(res));  
    }
}
