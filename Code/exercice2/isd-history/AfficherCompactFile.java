import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class AfficherCompactFile {

	public static void main(String[] args) throws IOException {
		
		
		Path filename = new Path("fichiersLab1/isd-history.txt");
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// initialiser le compteur et afficher un header
			int nbLines = 0;
			System.out.println("USAFCode   Name   FIPSCountryID   ElevationStation");
			
			
			// read line by line
			String line = br.readLine();
			while (line !=null){
				
				nbLines += 1;
				
				// enlever les 22 premières lignes
				if (nbLines > 22){
					// division de la ligne avec les numéros de caractères
					String USAFCode = line.substring(0, 6);
					String Name = line.substring(13, 42);
					String FIPSCountryID = line.substring(43, 45);
					String ElevationStation = line.substring(74, 81);
					
					System.out.println(USAFCode+" - "+Name+" - "+FIPSCountryID+" - "+ElevationStation);
				}
			
				// go to the next line
				line = br.readLine();
			}
		}

		finally{
			
			//close the file
			inStream.close();
			fs.close();
		}
	}
}
