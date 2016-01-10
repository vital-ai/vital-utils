package ai.vital.vitalutilcommands

import ai.vital.domain.ontology.VitalOntology;
import ai.vital.predict.categories.HierarchicalCategories;
import ai.vital.predict.categories.TaxonomyOWLBridge;
import ai.vital.predict.categories.HierarchicalCategories.TaxonomyNode;
import ai.vital.vitalsigns.ontology.VitalCoreOntology;

import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory;

class VitalTaxonomyCommand {

	public static void main(String[] args) {
		
		def cli = new CliBuilder(usage: 'vitaltaxonomy ...', stopAtNonOption: false);
		
		cli.with {
			h longOpt: 'help', required: false, args: 0, 'Show usage information'
			i longOpt: 'input', required: true, args: 1, 'Input taxonomy TXT file'
			o longOpt: 'output', required: true, args: 1, 'Output RDF/XML file'
		}
		
		boolean displayHelp = args.length == 0
		
		for(String arg : args) {
			if(arg == '-h' || arg == '--help') {
				displayHelp = true
			}
		}
		
		if(displayHelp) {
			cli.usage()
			return
		}
		
		def parsed = cli.parse(args)
		
		if(!parsed) return
		
		if(parsed.h) {
			cli.usage()
			return
		}
		
		File inputFile = new File(parsed.i)
		File outputFile = new File(parsed.o)
		
		println "Input file: ${inputFile.absolutePath}"
		println "Output file: ${outputFile.absolutePath}"
		
		if(!inputFile.exists()) throw new RuntimeException("Input file not found: ${inputFile.absolutePath}")
		if(outputFile.exists()) throw new RuntimeException("Output file exists: ${outputFile.absolutePath}")
		
		println "Loading categories..."
		HierarchicalCategories mapping = new HierarchicalCategories(inputFile);
		
		TaxonomyNode rootNode = mapping.getRootNode();
		
		Model model = ModelFactory.createDefaultModel()
		model.setNsPrefix("vital-core", VitalCoreOntology.NS)
		model.setNsPrefix("vital", VitalOntology.NS)
		
		TaxonomyOWLBridge.writeNode(model, rootNode)
		
		println "Writing categories to file ..."
		
		BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(outputFile))
		 
		model.write(os, "RDF/XML")
		
		os.close()
		
		println "DONE"
		
	}

}
