import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerProfiling extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> genreCountMap = new HashMap<>();

        // Counting the frequency of each genre
        for (Text value : values) {
            String[] genres = value.toString().split(",");
            for (String genre : genres) {
                genre = genre.trim(); // Remove leading and trailing whitespace
                genreCountMap.put(genre, genreCountMap.getOrDefault(genre, 0) + 1);
            }
        }

        // Building the output string
        StringBuilder output = new StringBuilder();
        for (Map.Entry<String, Integer> entry : genreCountMap.entrySet()) {
            output.append(entry.getKey() + ": " + entry.getValue() + ", ");
        }

        // Emitting the year and the genre counts
        context.write(key, new Text(output.toString()));
    }
}
