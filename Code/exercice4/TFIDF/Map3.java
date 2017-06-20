package ecp.Lab1.TFIDF;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Map3 extends Mapper<Text, Text, Text, Text> {

@Override
public void map(Text key, Text value, Context context) throws IOException,InterruptedException{
    
	 String[] wordFile = key.toString().split(";");
	 String[] counts = value.toString().split(";");
	 String dc = wordFile[1]+";"+counts[0]+";"+counts[1];
	 
     context.write(new Text(wordFile[0]), new Text(dc));
    }
}