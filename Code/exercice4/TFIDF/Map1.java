package ecp.Lab1.TFIDF;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable ONE = new IntWritable(1);
@Override
public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
	
	 FileSplit split = (FileSplit) context.getInputSplit();
	 String file = split.getPath().getName().toString();
	 String line = value.toString().toLowerCase();
	 
     for (String tokenizer: line.replaceAll("[^a-zA-Z0-9 ]", " ").split("\\s+")) {
    	 String wordandFile = tokenizer.toLowerCase()+";"+file;
    	 context.write(new Text(wordandFile), ONE);
     }

    }
}
