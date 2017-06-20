package ecp.Lab1.TFIDF;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Map2 extends Mapper<Text, Text, Text, Text> {

@Override
public void map(Text key, Text value, Context context) throws IOException,InterruptedException{
	 String[] wordFile = key.toString().split(";");
     context.write(new Text(wordFile[1]), new Text(wordFile[0]+";"+value.toString()));
    }
}
