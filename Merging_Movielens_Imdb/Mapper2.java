import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        String res="";
        String id = tokens[0].substring(2);
        int imdb_id = -1;
        String title= tokens[2];
        String genres= tokens[4];
        try {
            double ratings = Double.parseDouble(tokens[5]);
            int votes=Integer.parseInt(tokens[6]);
            int year=Integer.parseInt(tokens[1]);
            int runtime=Integer.parseInt(tokens[3]);
            imdb_id= Integer.parseInt(id);
            res+=title + "\t" + genres + "\t" +String.valueOf(ratings) + "\t" + String.valueOf(votes) + "\t" + String.valueOf(year) + "\t" + String.valueOf(runtime);
            context.write(new Text(String.valueOf(imdb_id)), new Text(res));
        } catch (NumberFormatException e) {
            context.write(new Text(String.valueOf(imdb_id)), new Text(res));
            
        }
        
		
        
    }
}