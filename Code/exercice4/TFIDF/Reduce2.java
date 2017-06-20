package ecp.Lab1.TFIDF;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;

public class Reduce2 extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException

    {
    	ArrayList<String> temp = new ArrayList<String>();
    	
    	int motsInDoc = 0;
    	for (Text value : values){
    		temp.add(value.toString());
    		String[] wc1 = value.toString().split(";");
    		motsInDoc += Integer.parseInt(wc1[1]);
    	}

    	for (int it = 0; it < temp.size(); ++it) {
    		String[] wc2 = temp.get(it).split(";");
    		String word_doc = wc2[0]+";"+key.toString();
    		String wc_motsInDoc = wc2[1]+";"+Integer.toString(motsInDoc);
    		context.write(new Text(word_doc), new Text(wc_motsInDoc));
    	}
    }
}