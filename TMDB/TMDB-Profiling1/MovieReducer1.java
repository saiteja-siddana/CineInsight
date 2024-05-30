import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer1 extends Reducer< Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
	    int count = 0;
	    Double total = 0.0;
	    for (Text value : values) {
      		count = count+1;
		total = total + Double.parseDouble(value.toString());
    	    }
	    total = total/count;
	    context.write( key, new Text(String.valueOf(total)+ "," + String.valueOf(count)));
    }
}

