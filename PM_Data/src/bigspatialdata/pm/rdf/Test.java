package bigspatialdata.pm.rdf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import com.hp.hpl.jena.rdf.model.Model;

public class Test {
	public static void main(String args[]){
		Model model = RDFCreate.measurements("57660");
		
		try {
			FileOutputStream out = new FileOutputStream(new File("/home/jychen/test.ttl"));
			model.write(out,"Turtle");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
