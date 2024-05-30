import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t+");
        String res="";
        String id = tokens[0];
        try {
            double ratings = Double.parseDouble(tokens[1]);
            int votes=Integer.parseInt(tokens[2]);
            res+=String.valueOf(ratings) + "\t" + String.valueOf(votes);
            context.write(new Text(id), new Text(res));
        } catch (NumberFormatException e) {
            context.write(new Text(id), new Text(res));
            
        }
        
		
        
    }
}