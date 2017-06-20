package ecp.Lab1.PageRank;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class PRMap1 extends Mapper<LongWritable, Text, Text, Text> {
	@Override
public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
		String line = value.toString();
		String[] interaction = line.split("\\s+");
     context.write(new Text(interaction[0]), new Text(interaction[1]));
    }
}
