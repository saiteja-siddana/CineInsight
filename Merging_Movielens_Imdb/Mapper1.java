import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String res="";
        String id = tokens[0];
        String imdb_id=tokens[1];
        String tmdb_id = tokens[2];
        String genres= tokens[tokens.length-3]; 

        try {
            double ratings = Double.parseDouble(tokens[tokens.length-2]);
            int votes=Integer.parseInt(tokens[tokens.length-1]);
            res+= "A\t"+tmdb_id+"\t"+genres+"\t"+String.valueOf(ratings)+"\t"+String.valueOf(votes);  
            context.write(new Text(imdb_id), new Text(res));
        } catch (NumberFormatException e) {
            context.write(new Text(imdb_id), new Text(res));
        }

        
		
        
    }
}