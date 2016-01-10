package ai.vital.vitalutilcommands

import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock

import ai.vital.vitalsigns.VitalSigns


import org.apache.commons.cli.Option

// example use:
// vitalgrep --file NYCSchools.vital \\
// --type http://www.vital.ai/ontology/nycschools/NYCSchoolRecommendation.owl#NYC_School \\
// --property schoolName  --query '(?i)park'

// suppress vitalsigns logging?

// instead of vitalsigns deserialization, should this fall back to generic
// parsing of the vital block file to avoid having to specify domain jars?
// since we're just grepping values anyway, do we need a full resolution to objects?



class VitalGrep {

	// grep command for vital block files
	
	// use local version of vitalsigns without an endpoint?
	
	String vitalServiceEndpoint = 'http://ec2-23-20-120-160.compute-1.amazonaws.com:80'
	
	String input_path = null
	
	String property_name = null
	
	String query = null
	
	String type = null
	
	
	void init() {
		
		// use vitalsigns independently?
		// not using service?
		
		
	}
	
	
	void grepper() {
		
		// actual property name in vitalsigns property map
		// better way to do this?
			
		
		if(property_name.contains("#")) {
			
			property_name = property_name.split("#")[1]
			
			if(property_name.startsWith("has")) {
				
				property_name = property_name - "has"
				
				property_name = property_name[0].toLowerCase() + property_name.substring(1)
				
				
			}
			else {
				property_name = property_name[0].toLowerCase() + property_name.substring(1)
				
			}
			
			
		}
		else {
			// assume this is the short name
			// hasSchoolName --> schoolName
			
			if(property_name.startsWith("has")) {
				
				property_name = property_name - "has"
				
				property_name = property_name[0].toLowerCase() + property_name.substring(1)
				
				
				
			}
			else { // no "has", make sure first letter lower case
				
				property_name = property_name[0].toLowerCase() + property_name.substring(1)
				
				
			}
			
		}
		
		
		println "Path: " + input_path
		println "Type: " + type
		println "Property: " + property_name
		println "Query: " + query
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(input_path), "UTF-8"))
		
		for (Iterator<VitalBlock>blocksIterator = BlockCompactStringSerializer.getBlocksIterator(br); blocksIterator.hasNext();){
			
			try {
			
			VitalBlock block = blocksIterator.next()
			GraphObject go = block.getMainObject()
		
			match(go)
			
			
			List<GraphObject> dependentObjects = block.getDependentObjects()
			
			dependentObjects.each{ it -> match(it) }
			
			
			
			} catch(Exception ex) { }
						
			// Caught: java.lang.RuntimeException: Empty block!
			// due to domain jar not having a class?
			
	}
		
	}
	
	
	public void match(GraphObject go) {
		
		//println go.toCompactString()
		
		// some nice way to get type string out of GraphObject?
		
		if(type != null) {
			
			if( type == VitalSigns.get().getClassesRegistry().getClassURI(go.getClass()) ) {
			
				if(property_name != null) {
						
					try {
					String propvalue = go.getProperty(property_name)
											
					def pattern = /$query/
					
					if( propvalue =~ pattern) {
						
						//println "Match:"
						
						println go.toCompactString()
						
					
					}
					} catch(Exception e) {println "Exception: Property Not Found"}
				}
				
				
			}
			
		}
		
		
		
		
	}
	
	
	
	static main(args) {
	
		VitalGrep grepper = new VitalGrep()
		
		grepper.init()
		
		
		def cli = new CliBuilder(usage: 'vitalgrep [options]', stopAtNonOption: false)
		cli.with {
			h longOpt: "help", "Show usage information"
			f longOpt: "file", "Input File", args:1
			t longOpt: "type", "Type of Class", args:1
			q longOpt: "query", "Query", args:1
			p longOpt: "property", "Property Name", args:1
			
		}
		
		def options = cli.parse(args)
		
		String userdir = System.getProperty("user.dir")
		
		
		if(options.f) {
			
			String input = options.f
			
			if(input.startsWith("/")) {
				
				grepper.input_path = input
			}
			else {
				
				grepper.input_path = userdir + "/" + input
			}
			
		}
		
		if(options.t) {
			
			grepper.type = options.t
			
		}
		
		// need to do anything for more complex quoted queries?
				
		if(options.q) {
			grepper.query = options.q
		}
		
		
		if(options.p) {
			
			grepper.property_name = options.p
		}
		
		if(options.arguments().size > 0) {
			println "Use of undefined option(s):"
			println options.arguments()
			
			cli.usage()
			System.exit(0)
			
		}
		
		
		if (options.h || options == null) {
			cli.usage()
			System.exit(0)
		}
		
		grepper.grepper()
		
	}

}
