package ecp.Lab1.TFIDF;
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

public class TFIDFMapRed extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        if (args.length != 4) {
            System.out.println("There has to be one input file and three output files because we process in three steps.");
            System.exit(-1);
        }

        // Comme dans l'énoncé, nous avons trois jobs successifs
        
        // Round 1: WordCount

        Job job = Job.getInstance(getConf());
        job.setJobName("TFIDF1");

        job.setJarByClass(TFIDFMapRed.class);
        job.setMapperClass(Map1.class);
        job.setReducerClass(Reduce1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

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
        
        Job job1 = Job.getInstance(getConf());
        job1.setJobName("TFIDF2");

        job1.setJarByClass(TFIDFMapRed.class);
        job1.setMapperClass(Map2.class);
        job1.setReducerClass(Reduce2.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
	    job1.setInputFormatClass(KeyValueTextInputFormat.class);
	    job1.setOutputFormatClass(TextOutputFormat.class);
	    
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        FileSystem fichiersortieRound2 = FileSystem.newInstance(getConf());
        if (fichiersortieRound2.exists(new Path(args[2]))) {
        	fichiersortieRound2.delete(new Path(args[2]), true);
        }
  
        job1.waitForCompletion(true);
        
        // Round 3 : docCount per word
        
        Job job2 = Job.getInstance(getConf());
        job2.setJobName("TFIDF3");

        job2.setJarByClass(TFIDFMapRed.class);
        job2.setMapperClass(Map3.class);
        job2.setReducerClass(Reduce3.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        job2.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
	    job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        FileSystem fichiersortieRound3 = FileSystem.newInstance(getConf());
        if (fichiersortieRound3.exists(new Path(args[3]))) {
        	fichiersortieRound3.delete(new Path(args[3]), true);
        }
        
        job2.waitForCompletion(true);
        
        return 0;
    }


    public static void main(String[] args) throws Exception {
        TFIDFMapRed exempleDriver = new TFIDFMapRed();
        int res = ToolRunner.run(exempleDriver, args);
        System.exit(res);
    }

}
