package ecp.Lab1.TFIDF;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;

public class Reduce3 extends Reducer<Text, Text,  Text,DoubleWritable> {
    @Override

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException
    {

    	ArrayList<String> temp = new ArrayList<String>();
    	int docPerWord = 0;
    	for (Text value : values){
    		temp.add(value.toString());
    		docPerWord = docPerWord + (int) 1;
    	}
    	
    	for (int it = 0; it < temp.size(); ++it) {
    		String[] fileWcDpw = temp.get(it).split(";");
    		double tf = (double) Integer.parseInt(fileWcDpw[1])/Integer.parseInt(fileWcDpw[2]);
    		double idf = Math.log(2/docPerWord);
    		double tfIdf = tf*idf;
    		context.write(new Text(key.toString()+" from "+fileWcDpw[0]),new DoubleWritable(tfIdf));
    		
    	}



    }

}