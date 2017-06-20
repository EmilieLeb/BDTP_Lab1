package ecp.Lab1.PageRank;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PRReduce1 extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException
    {
    	String list_out = new String();
    	for (Text value : values){
    		list_out = list_out + " - " + value.toString();
    	}
    	list_out = list_out.substring(1);

        context.write(key, new Text("1 - "+list_out));
    }
}