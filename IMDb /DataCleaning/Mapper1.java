import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        String res="";
        String id = tokens[0];
        String genres = tokens[8];
        String title= tokens[2]; 
        try {
            int year = Integer.parseInt(tokens[5]);
            int time = Integer.parseInt(tokens[7]);
            res+= "A\t"+String.valueOf(year)+"\t"+title+"\t"+String.valueOf(time)+"\t"+genres;  
            context.write(new Text(id), new Text(res));
        } catch (NumberFormatException e) {
            context.write(new Text(id), new Text(res));
        }

        
		
        
    }
}