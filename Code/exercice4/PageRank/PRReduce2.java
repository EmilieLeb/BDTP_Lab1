package ecp.Lab1.PageRank;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class PRReduce2 extends Reducer<Text, Text, Text, Text> {
	private static HashMap<String,String> line_link = new HashMap<String,String>();
   
    public PRReduce2() throws NumberFormatException, IOException{
   
     // On cherche à réutiliser le fichier output1 dans l'hdfs mais cela ne fonctionne pas... 
     BufferedReader Reader_count = new BufferedReader(new FileReader(new File("http://localhost:50070/explorer.html#/user/cloudera/output/PR1/part-r-00000")));
        while ((Reader_count.readLine()) != null)
        {
            String[] divis = Reader_count.readLine().split(" - ", 2);
            if (divis.length >= 2)
            {  
            	line_link.put(divis[0].split(",",2)[0],divis[1]);
            } 
        }
        Reader_count.close();
        System.out.println(line_link.get("0"));
      }
    
     @Override
	
    // Calcul du pagerank
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
    	double somme = 0;
    	StringBuilder result = new StringBuilder();
    	
    	for (Text value : values){
    		String [] temp = value.toString().split(";");
    		double num = Double.parseDouble(temp[1]);
    		int denum = Integer.parseInt(temp[2]);
    		somme += (double) num/denum;
    		
    		result.append(temp[0]).append(";");
    	}
    	
    	// damping
    	double newPR = (1-0.85) + 0.85*somme;
    	
    	if (line_link.get(key.toString())!=null){
    	context.write(key, new Text(String.valueOf(newPR)+" - "+line_link.get(key.toString()).toString()));
    	}
    }
}