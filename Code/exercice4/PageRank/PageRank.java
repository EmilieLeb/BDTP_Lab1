package ecp.Lab1.PageRank;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;

public class PageRank extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.println("There has to be one input file and two output files because we process in two steps.");
            System.exit(-1);
        }

        // Nous avons deux jobs successifs
        
        // Round 1: WordCount

        Job job = Job.getInstance(getConf());
        job.setJobName("PR1");

        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRMap1.class);
        job.setReducerClass(PRReduce1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Fichier csv en sortie
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");

        // Input Output
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        //Delete output si déjà présent
        FileSystem fichiersortieRound1 = FileSystem.newInstance(getConf());
        if (fichiersortieRound1.exists(new Path(args[1]))) {
        	fichiersortieRound1.delete(new Path(args[1]), true);
        }
        job.waitForCompletion(true);
        
        // Round 2: WordCount per doc

        Path inputPath = new Path(args[1]);
        Path outputPath =  null;
        
        int iterations = 30;
        // On estime que cela converge à 30 itérations
        // Boucle jusqu'à 30 itérations:
        for(int it=0;it<iterations;it++){
        	Job job1 = Job.getInstance(getConf());
        	// On set l'output avec le num de l'itération
        	outputPath = new Path(args[2]+"_"+it);
        	job1.setJobName("PR2");
        
        	// Config habituelle
            job1.setJarByClass(PageRank.class);
            job1.setMapperClass(PRMap2.class);
            job1.setReducerClass(PRReduce2.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            
            job1.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
            job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
    	    job1.setInputFormatClass(KeyValueTextInputFormat.class);
    	    job1.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job1, inputPath);
            FileOutputFormat.setOutputPath(job1, outputPath);
            
            FileSystem fichiersortieRound2 = FileSystem.newInstance(getConf());
            if (fichiersortieRound2.exists(outputPath)) {
            	fichiersortieRound2.delete(outputPath, true);
            }
            
            job1.waitForCompletion(true);
            inputPath=outputPath;
            
            }
        
        return 0;
    }

    public static void main(String[] args) throws Exception {
        PageRank driver = new PageRank();
        int res = ToolRunner.run(driver, args);
        System.exit(res);
    }
}
