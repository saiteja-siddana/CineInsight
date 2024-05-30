import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
	
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String[] res=new String[9];
		Set<String> genresSet = new HashSet<>();
		int imdb_id=-1;
        for (Text value : values) {
			
			String[] v = value.toString().split("\t");
			String[] g;
			
			
			if (v[0].equals("A"))
			{

				res[0]=v[1]; // get tmdb_id

				if(!v[2].equals("(no genres listed)"))
				{
					g = v[2].toString().split("\\|");
				
					for (String element : g) {
						genresSet.add(element);
					}
					StringJoiner joiner = new StringJoiner("|");
					for (String element : genresSet) {
						joiner.add(element);
					}
					res[1] = joiner.toString(); // get genres
				}
				

				res[2]=v[3]; // get ratings
				res[3] = v[4]; // get votes
			}
			else
			{
				
				// if(v.length == 6)
				// {
					imdb_id=Integer.parseInt(key.toString());
					res[4]=v[0]; // get title

					g = v[1].toString().split(",");
					for (String element : g) {
						genresSet.add(element);
					}
					StringJoiner joiner = new StringJoiner("|");
					for (String element : genresSet) {
						joiner.add(element);
					}
					res[1] = joiner.toString(); // get genres

					res[5]=v[2]; // get ratings
					res[6]=v[3]; // get votes
					res[7]=v[4]; // get year
					res[8]=v[5]; // get runtime

				// }
				
				
			}
			
        }
		if(imdb_id!=-1)
		{
			if (!Arrays.asList(res).contains("\\N") && !Arrays.asList(res).contains(null) && !Arrays.asList(res).contains(""))
			{
				context.write(new Text(res[0]), new Text(res[1] + "\t" + res[2] + "\t" + res[3] + "\t" + res[4] + "\t" + res[5]+ "\t" + res[6] + "\t" + res[7] + "\t" + res[8]));
			}
		}
    }
}








