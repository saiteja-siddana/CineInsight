import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;

public class Reducer1 extends Reducer<IntWritable, Text, Text, Text> {
	
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String[] res=new String[11];

        for (Text value : values) {
			
			String[] tokens = value.toString().split("\t");

			if (tokens[0].equals("A")) {
				res[0] = tokens[1]; // title
				res[1] = tokens[2]; // genres
				res[2] = tokens[3]; // year
				res[3] = tokens[4]; // rumtime
				res[4] = tokens[5]; // ml_avg_rating
				res[5] = tokens[6]; // ml_votes
				res[6] = tokens[7]; // imdb_rating
				res[7] = tokens[8]; // imdb_votes
			}
			else {
				res[8] = tokens[0]; // tmdb_rating
				res[9] = tokens[1]; // tmdb_votes
				res[10] = tokens[2]; // popularity	
			}
			
        }

		if (!Arrays.asList(res).contains("\\N") && !Arrays.asList(res).contains(null) && !Arrays.asList(res).contains("")) {
			String output = res[0] + "\t" + res[1] + "\t" + res[2] + "\t" + res[3] + "\t" + res[4] + "\t" + res[5]+ "\t" + res[6] + "\t" + res[7] + "\t" + res[8] + "\t" + res[9] + "\t" + res[10];
			context.write(new Text(key.toString()), new Text(output));
		}
    }
}








