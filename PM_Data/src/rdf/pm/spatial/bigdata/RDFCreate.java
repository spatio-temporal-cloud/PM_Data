package rdf.pm.spatial.bigdata;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

public class RDFCreate {
	public static String uri="http://www.bigspatialdata.org/pm";
	public static Resource measurements(String id){
		Model m = ModelFactory.createDefaultModel();
		Resource measurements = m.createResource(uri+"#" + id);
		
		return measurements;
	}
}
