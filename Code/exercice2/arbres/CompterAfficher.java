import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CompterAfficher {

	public static void main(String[] args) throws IOException {
				
		Path filename = new Path("fichiersLab1/arbres.csv");
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		
		int nbLignes = 0;
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			while (line !=null){
				// Process of the current line
				// Ajouter un au nombre total de lignes
				nbLignes += 1;
				// Afficher le contenu de la ligne, avec "no info" si données manquantes
				String[] attributes = line.split(";");
				if (attributes[5].equals("")){
					attributes[5] = "no info";
				}
				if (attributes[6].equals("")){
					attributes[6] = "no info";
				}
				// Sans oublier de ne pas afficher les headers
				if(nbLignes != 1){
				System.out.println("year: "+attributes[5]+" - height: "+attributes[6]);
				}
				// go to the next line
				line = br.readLine();
			}
		}

		finally{
			System.out.println("There are "+nbLignes+" lines in arbres.csv.");
			//close the file
			inStream.close();
			fs.close();
		}
	}
}