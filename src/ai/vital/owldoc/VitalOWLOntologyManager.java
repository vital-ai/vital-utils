package ai.vital.owldoc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.coode.owlapi.functionalrenderer.OWLFunctionalSyntaxOntologyStorer;
import org.coode.owlapi.latex.LatexOntologyStorer;
import org.coode.owlapi.obo.renderer.OBOFlatFileOntologyStorer;
import org.coode.owlapi.owlxml.renderer.OWLXMLOntologyStorer;
import org.coode.owlapi.rdf.rdfxml.RDFXMLOntologyStorer;
import org.coode.owlapi.turtle.TurtleOntologyStorer;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.io.OWLOntologyDocumentSource;
import org.semanticweb.owlapi.io.StreamDocumentSource;
import org.semanticweb.owlapi.model.MissingImportHandlingStrategy;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLImportsDeclaration;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyLoaderConfiguration;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.util.NonMappingOntologyIRIMapper;

import com.hp.hpl.jena.rdf.model.Model;

import de.uulm.ecs.ai.owlapi.krssrenderer.KRSS2OWLSyntaxOntologyStorer;
import ai.vital.vitalsigns.model.rdf.RDFFormat;
import ai.vital.vitalsigns.ontology.VitalOntDocumentManager;
import uk.ac.manchester.cs.owl.owlapi.EmptyInMemOWLOntologyFactory;
import uk.ac.manchester.cs.owl.owlapi.OWLOntologyManagerImpl;
import uk.ac.manchester.cs.owl.owlapi.ParsableOWLOntologyFactory;
import uk.ac.manchester.cs.owl.owlapi.mansyntaxrenderer.ManchesterOWLSyntaxOntologyStorer;

public class VitalOWLOntologyManager extends OWLOntologyManagerImpl {

	private static final long serialVersionUID = 8821764864885545814L;

	private VitalOntDocumentManager innerManager; 

	//to load owl models without imports
	private OWLOntologyManager auxManager;
	
	public VitalOWLOntologyManager(OWLDataFactory dataFactory) {
		super(dataFactory);
		innerManager = new VitalOntDocumentManager();
		
		 this.addOntologyStorer(new RDFXMLOntologyStorer());
		 this.addOntologyStorer(new OWLXMLOntologyStorer());
		 this.addOntologyStorer(new OWLFunctionalSyntaxOntologyStorer());
		 this.addOntologyStorer(new ManchesterOWLSyntaxOntologyStorer());
		 this.addOntologyStorer(new OBOFlatFileOntologyStorer());
		 this.addOntologyStorer(new KRSS2OWLSyntaxOntologyStorer());
		 this.addOntologyStorer(new TurtleOntologyStorer());
		 this.addOntologyStorer(new LatexOntologyStorer());

		 this.addIRIMapper(new NonMappingOntologyIRIMapper());

		 this.addOntologyFactory(new EmptyInMemOWLOntologyFactory());
		 this.addOntologyFactory(new ParsableOWLOntologyFactory());
		 
		 auxManager = new OntologyManagerThatDoesNotLoadImports();
		 
	}

	@Override
	protected OWLOntology loadImports(OWLImportsDeclaration decl,
			OWLOntologyLoaderConfiguration conf)
			throws OWLOntologyCreationException {

		//this is necessary to forward to vital ont manager
		String iri = decl.getIRI().toString();
		
		Model model = innerManager.getModel(iri);
		
		if(model != null) {
			
			//write the model to outputstream
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			model.write(os, null, RDFFormat.RDF_XML.toJenaTypeString());
			
//			//load the ontology
//	        final OWLOntologyLoaderConfiguration config = new OWLOntologyLoaderConfiguration();
//	        config.setMissingImportHandlingStrategy(MissingImportHandlingStrategy.SILENT);
//	        OWLOntologyDocumentSource source = new StreamDocumentSource(new ByteArrayInputStream(os.toByteArray()));
//			final OWLOntology ontology = auxManager.loadOntologyFromOntologyDocument(source, config);
//	        return ontology;
			
			return loadOntologyFromOntologyDocument(new ByteArrayInputStream(os.toByteArray()));
			
		} else {
			
			return super.loadImports(decl, conf);
			
		}

	}
	
	private class OntologyManagerThatDoesNotLoadImports extends OWLOntologyManagerImpl {

	    private static final long serialVersionUID = -1874568361784481161L;

	    public OntologyManagerThatDoesNotLoadImports() {
	        super(OWLManager.getOWLDataFactory());
	        
	        this.addOntologyStorer(new RDFXMLOntologyStorer());
	        this.addOntologyStorer(new OWLXMLOntologyStorer());
	        this.addOntologyStorer(new OWLFunctionalSyntaxOntologyStorer());
	        this.addOntologyStorer(new ManchesterOWLSyntaxOntologyStorer());
	        this.addOntologyStorer(new OBOFlatFileOntologyStorer());
	        this.addOntologyStorer(new KRSS2OWLSyntaxOntologyStorer());
	        this.addOntologyStorer(new TurtleOntologyStorer());
	        this.addOntologyStorer(new LatexOntologyStorer());
	        
	        this.addIRIMapper(new NonMappingOntologyIRIMapper());
	        
	        this.addOntologyFactory(new EmptyInMemOWLOntologyFactory());
	        this.addOntologyFactory(new ParsableOWLOntologyFactory());
	        
	    }

	    @Override
	    protected OWLOntology loadImports(OWLImportsDeclaration declaration,
	            OWLOntologyLoaderConfiguration configuration) throws OWLOntologyCreationException {
	        return null;
	    }

	}
	
}
