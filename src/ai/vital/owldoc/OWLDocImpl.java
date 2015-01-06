package ai.vital.owldoc;

import java.io.File;
import java.net.URL;

import org.coode.html.OWLHTMLKit;
import org.coode.html.OntologyExporter;
import org.coode.html.impl.OWLHTMLKitImpl;
import org.coode.owl.mngr.OWLServer;
import org.coode.owl.mngr.impl.OWLServerImpl;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for OWLDoc plugin that generates the documentation without protege
 * 
 * @author Derek
 *
 */
public class OWLDocImpl {

	private final static Logger log = LoggerFactory.getLogger(OWLDocImpl.class);
	
    // as all URLs in links should be relative, this should not matter
    private static URL DEFAULT_BASE;

    static {
        try {
            DEFAULT_BASE = new URL("http://www.co-ode.org/");
        }
        catch (Exception e) {
        	log.error(e.getLocalizedMessage(), e);
        }
    }
	
    public static File generateOWLDoc(File folder, File owlFile) throws Exception {
    	
		OWLOntologyManager ontManager = new VitalOWLOntologyManager(OWLManager.getOWLDataFactory());
		
		OWLServer svr = new OWLServerImpl(ontManager);

		svr.loadOntology(owlFile.toURI());
		
		
		for(OWLOntology ont : svr.getOntologies()) {
			log.info("Loaded ontology: " + ont.getOntologyID().getOntologyIRI().toString());
		}
		
		log.info("Active ontology: " + svr.getActiveOntology().getOntologyID().toString());

		OWLHTMLKit owlhtmlKit = new OWLHTMLKitImpl("owldoc-kit", svr, DEFAULT_BASE);
		OntologyExporter exporter = new OntologyExporter(owlhtmlKit);
		File index = exporter.export(folder);
		svr.dispose();
		
		return index;

	}

}
