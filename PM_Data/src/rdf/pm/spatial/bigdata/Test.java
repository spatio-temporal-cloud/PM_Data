package rdf.pm.spatial.bigdata;

import com.hp.hpl.jena.rdf.model.Model;

public class Test {
	public static void main(String args[]){
		Model model = RDFCreate.measurements("57660");
		model.write(System.out,"N3");
	}
}
