package ecp.Lab1.PageRank;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class PRMap2 extends Mapper<Text, Text, Text, Text> {

@Override
public void map(Text key, Text value, Context context) throws IOException,InterruptedException{

	// Nombre de connections pour cette key
	String [] connections = value.toString().split(" - ");
	int numConnection = connections[1].split(";").length;

	for (String idPage : connections[1].split(";"))
	{
		String interactions = key.toString()+";"+connections[0]+ ";"+Integer.toString(numConnection);
		context.write(new Text(idPage), new Text(interactions));
	}
    }
}