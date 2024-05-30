import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperProfiling extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Splitting the input line into tokens
        String[] tokens = value.toString().split("\\t");
        // Assuming the year is the second token and genres are the last token
        String year = tokens[1];
        String genres = tokens[3];
        
        // Emitting year as key and genres as value
        context.write(new Text(year), new Text(genres));
    }
}
