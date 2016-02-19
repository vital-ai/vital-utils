package ai.vital.vitalutilcommands

import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.model.VITAL_Edge
import ai.vital.vitalsigns.model.property.IProperty
import ai.vital.vitalsigns.model.property.URIProperty
import ai.vital.vitalsigns.ontology.VitalCoreOntology;
import ai.vital.vitalsigns.uri.URIGenerator
import ai.vital.vitalsigns.uri.URIGenerator.URIResponse

import java.util.Map.Entry

class VitalTransformCommand extends AbstractUtil {

	static class Stats {
		
		int total = 0
		
		int failed = 0
		
		int unchanged = 0
		
		int updated = 0
		
	}
	
	static main(args) {
	
		def cli = new CliBuilder(usage: 'vitaltransform [options]', stopAtNonOption: false)
		cli.with {
			h longOpt: "help", "show usage information", args: 0, required: false
			i longOpt: "input", "input .vital[.gz] block file", args:1, required: true
			o longOpt: "output", "output .vital[.gz block file", args:1, required: true
			ow longOpt: "overwrite", "overwrite output file", args: 0, required: false
			an longOpt: "addnamespace", "add namespace to URIs random uri part (prefix), URI (hyper)edge source/destination, mutually exclusive with --removenamespace", args: 1, required: false
			rn longOpt: "removenamespace", "remove namespace from URIs random uri part (prefix), URI (hyper)edge source/destination, , mutually exclusive with --addnamespace", args: 1, required: false
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
		
		def options = cli.parse(args)
		
		if(!options || options.h) {
			return
		}
		
		File inputFile = new File(options.i)
		File outputFile = new File(options.o)
		
		boolean overwrite = options.ow ? true : false
		
		String addNamespace = options.an ? options.an : null
		String removeNamespace = options.rn ? options.rn : null
		
		println "Input file: ${inputFile.absolutePath}"
		println "Output file: ${outputFile.absolutePath}"
		println "Overwrite output ? ${overwrite}" 
		
		if(inputFile.absolutePath.equals(outputFile.absolutePath)) {
			error "Input and Output paths must not be the same"
		}
		
		if(addNamespace && removeNamespace) {
			error("--addnamespace and --removenamespace are mutually exclusive")
		} else if(!addNamespace && !removeNamespace) {
			error("--addnamespace or --removenamespace required")
		} else if(addNamespace) {
			println "Add namespace: ${addNamespace}"
		} else if(removeNamespace) {
			println "Remove namespace: ${removeNamespace}"
		}
		
		if( ! blockFileNameFilter.accept(inputFile) ) {
			error "Input file is not a valid block file: ${inputFile.absolutePath}"
		}
		
		if( ! blockFileNameFilter.accept(outputFile) ) {
			error "Output file path is not a valid block file: ${outputFile.absolutePath}"
		}
		
		if(!inputFile.exists()) {
			error "Input file does not exist: ${inputFile.absolutePath}"
		}
		if(!inputFile.isFile()) {
			error "Input path is not a file: ${inputFile.absolutePath}"
		}
		
		if(outputFile.exists()) {
			if(!overwrite) {
				error "Output path already exists: ${outputFile.absolutePath} - use --overwrite option"
			} else {
				println "Output path will be overwritten: ${outputFile.absolutePath}"
			}
		}
		
		
		VitalSigns.get()
		
		long start = t()
		
		BlockCompactStringSerializer writer = new BlockCompactStringSerializer(outputFile)
		
		Map<String, Stats> statsMap = new LinkedHashMap<String, String>()
		
		for(String p : [VitalCoreOntology.URIProp.getURI(), VitalCoreOntology.hasEdgeSource.getURI(), VitalCoreOntology.hasEdgeDestination.getURI(), 
			VitalCoreOntology.hasHyperEdgeSource.getURI(), VitalCoreOntology.hasHyperEdgeDestination.getURI()]) {
			statsMap.put(p, new Stats())
		}
		
		int blocks = 0
		int objects = 0
		
		for( BlockIterator iterator = BlockCompactStringSerializer.getBlocksIterator(inputFile); iterator.hasNext(); ) {
			
			VitalBlock block = iterator.next()
			
			blocks++
			
			writer.startBlock()
			
			for(GraphObject g : block.toList()) {
				
				objects++
				
				for(Entry<String, IProperty> e : g.getPropertiesMap().entrySet() ) {
					
					IProperty p = e.getValue()
					
					IProperty unwrapped = p.unwrapped()
					
					Stats stats = statsMap.get(e.getKey())
					
					if(stats == null) continue
					
					if( ! ( unwrapped instanceof URIProperty ) ) {
						throw new RuntimeException("Stats for non uri property ? " + e.getKey())
					}

					stats.total++
					
					URIProperty up = unwrapped
					
					String uri = up.get()
					
					String newURI = null
					
					try {
						
						newURI = addNamespace ? addNS(addNamespace, uri) : removeNS(removeNamespace, uri)
						
					} catch(Exception ex) {  }
					
					if(newURI == null) {
						stats.failed++
					} else if(newURI.length() == 0) {
						stats.unchanged++
					} else {
						stats.updated++
						
						up.setValue(newURI)
						
					}
					
				}
				
				writer.writeGraphObject(g)
				
			}
			
			writer.endBlock()
			
		}
		
		writer.close()
		
		
		for(Entry<String, Stats> e : statsMap.entrySet()) {
			
			Stats s = e.getValue()
			
			println "${e.getKey()}\ttotal: ${s.total}, updated: ${s.updated}, unchanged: ${s.unchanged} failed: ${s.failed}"
			
		}
		
		println "Done, blocks iterated: ${blocks}, objects: ${objects}, time: ${t() - start}ms"
		
		
	}

	static String addNS(String namespace, String uri) throws Exception {
		
		int lastSlash = uri.lastIndexOf('/');
		
		if(lastSlash < 0 || lastSlash == uri.length() - 1) {
			
			throw new Exception("URI does not have random part: " + uri)
			
		}
		
		String rpart = uri.substring(lastSlash + 1)
		
		return uri.substring(0, lastSlash + 1) + namespace + rpart
		
	}
	
	static String removeNS(String namespace, String uri) {
		
		
		int lastSlash = uri.lastIndexOf('/');
		
		if(lastSlash < 0 || lastSlash == uri.length() - 1) {
			
			throw new Exception("URI does not have random part: " + uri)
			
		}
		
		String rpart = uri.substring(lastSlash + 1)
		
		if(!rpart.startsWith(namespace)) {
			
			return ""
			
		}
		
		rpart = rpart.substring(namespace.length())
		
		return uri.substring(0, lastSlash + 1) + rpart
		
	}
}
