import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Arrays;

public class Reducer1 extends Reducer<Text, Text, Text, Text> {
	
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String[] res=new String[6];;
        for (Text value : values) {
			
			String[] v = value.toString().split("\t");
			
			if (v[0].equals("A"))
			{
				
				res[0]=v[1]; // get year
				res[1]=v[2]; // get title
				res[2]=v[3]; // get time
				res[3] = v[4]; // get genres
			}
			else
			{
				if(v.length == 2)
				{
					res[4]=v[0]; // get ratings
					res[5]=v[1]; // get 
				}
				
				
			}
			
        }

        if (!Arrays.asList(res).contains("\\N") && !Arrays.asList(res).contains(null) && !Arrays.asList(res).contains(""))
		{
        	context.write(key, new Text(res[0] + "\t" + res[1] + "\t" + res[2] + "\t" + res[3] + "\t" + res[4] + "\t" + res[5]));
    	}
    }
}








