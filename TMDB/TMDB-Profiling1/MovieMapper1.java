import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper1 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
	    String line = value.toString();
	    int ID = Integer.parseInt(line.split(",", 2)[0].trim());
	    String line1 = line.split(",", 2)[1].trim();
	    double popularity,rating;
	    String title,cast,overview,genres;
	    int count,year;
	    String[] genre_parts;
	    line = line.split(",", 2)[1].trim();
	    popularity = Double.parseDouble(line.split(",", 2)[0].trim());
	    line = line.split(",", 2)[1].trim();
	    //so far cut out and retrived popularity and ID
	    if( !csplit(line)[0].isEmpty()){
	    
	    	title = csplit(line)[0].trim();
	    	line  = csplit(line)[1].substring(1).trim();
	    	// extracted Moviename and checking if everything is working so far cause next things minght not be presnet
	     	if( !csplit(line)[0].isEmpty()){	
			cast = csplit(line)[0].trim();;
	    		line = csplit(line)[1].substring(1).trim();
			if( !csplit(line)[0].isEmpty() && csplit(line).length ==2){
				overview = title = csplit(line)[0].trim();
				System.out.println(line);
				line = csplit(line)[1].substring(1).trim();;

				try{
	    				count = Integer.parseInt(line.split(",", 2)[0].trim());
	    				line = line.split(",", 2)[1].trim();
					rating = Double.parseDouble(line.split(",", 2)[0].trim());
					line = line.split(",", 2)[1].trim();
					year = Integer.parseInt(line.split(",", 2)[0].trim());
					line = line.split(",", 2)[1].trim();
                			genres = line.split("\"", 3)[1].trim();
				        if(count > 30 || popularity > 10){
	    					genre_parts = genres.split(",");
						for(String temp : genre_parts){
						     context.write(new Text(temp), new Text(String.valueOf(rating)));
						}
					}
	    			}
	    			catch (NumberFormatException e){
		    			e.printStackTrace();
	    			}
		 	}
		}
	   }
    }
     private String[] csplit(String in) {
	    String [] Out = in.split("\"",2);
	    Out = Out[1].split("\"",2);
            return Out;
    }
}
