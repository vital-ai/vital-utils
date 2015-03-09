package ai.vital.vitalutilcommands

import ai.vital.property.BooleanProperty;
import ai.vital.property.IProperty
import ai.vital.property.URIProperty;
import ai.vital.query.graphbuilder.GraphQueryBuilder;
import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.factory.Factory;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.graph.VitalExportQuery;
import ai.vital.vitalservice.query.graph.VitalGraphQuery
import ai.vital.vitalservice.query.graph.VitalSelectQuery
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.GraphMatch;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;
import ai.vital.vitalsigns.datatype.VitalURI

import java.util.Map.Entry

import org.apache.commons.io.FileUtils;


/**
 * A script that queries vital service and persists results
 * @author Derek
 *
 */
class VitalQuery extends AbstractUtil {
	
	public static void main(String[] args) {
		
		def cli = new CliBuilder(usage: 'vitalquery [options]')
		cli.with {
			h longOpt: "help", "Show usage information", args: 0, required: false
			o longOpt: "output", "output (.vital[.gz]|.sparql) block or sparql file (depending on -s flag), it prints to console otherwise", args:1, required: false
			ow longOpt: "overwrite", "overwrite output file", args: 0, required: false
			q longOpt: "query", "qurery file (.groovy|.builder) - groovy or query builder defined query", args: 1, required: true
			s longOpt: "tosparql", "output the query as sparql instead of executing it", args: 0, required: false
			g longOpt: "group", "group graph matches into blocks, explicit boolean flag parameter [true|false], requires mainProp", args: 1, required: false
			mp longOpt: "mainProp", "main bound property, required when --group=true", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		
		def options = cli.parse(args)
		
		if(!options || options.h) return
		
		File queryScriptFile = new File(options.q);
		
		boolean outputSparql = options.s ? true : false
		
		println "Output sparql ? ${outputSparql}"
		println "Query script file: ${queryScriptFile.absolutePath}"
		
		String serviceProfile = options.prof ? options.prof : null
		
		Boolean group = options.g ? Boolean.parseBoolean( options.g ) : false 
		
		println "Group ? ${group}"
		
		String mainProperty = options.mp ? options.mp : null 
		if(group) {
			if(!mainProperty) error("mainProperty not set, required when group=true")
			println "Main Property: ${mainProperty}"
		}
		
		if(!queryScriptFile.isFile()) {
			error "Query script file path does not exist or is not a file"
			return
		}
		
		boolean groovyInput = false 
		boolean builderInput = false
		if(queryScriptFile.name.endsWith(".groovy")) {
			groovyInput = true
		} else if(queryScriptFile.name.endsWith(".builder")) {
			builderInput = true
		} else {
			error("Input query file name must end with .groovy or .builder : ${queryScriptFile.absolutePath}")
			return
		}
		
		File output = options.o ? new File(options.o) : null
				
		if(output != null) {
			
			Boolean overwrite = Boolean.TRUE.equals(options.ow)
			
			println ("Output file: ${output.absolutePath}")
			println ("Overwrite ? " + overwrite)
			
			if(outputSparql) {
				
				if(!output.name.endsWith(".sparql")) {
					error("Sparql output file name must end with .sparql : ${output.absolutePath}")
					return
				}
				
			} else {
				if(!blockFileNameFilter.accept(output)) {
					error("Output file name must end with .vital[.gz]: ${output.absolutePath}")
					return
				}
			}
			

		
			if(output.exists() && !overwrite) {
				error("Output file already exists - use '-ow' option to overwrite - ${output.absolutePath}")
				return
			}
			 
		}
		
		if(serviceProfile != null) {
			println "Setting service profile: ${serviceProfile}"
			Factory.setServiceProfile(serviceProfile)
		} else {
			println "Default service profile"
		}
		
		VitalGraphQuery queryObject = null
		
		
		if(groovyInput) {
			
			println "Parsing groovy query object definition..."
			
			String queryScript = FileUtils.readFileToString(queryScriptFile, "UTF-8")
					
			queryScript =
				"import ai.vital.vitalservice.query.graph.*;\n" +
				queryScript;
			
			
			GroovyShell shell = new GroovyShell();
			Object _query = shell.evaluate(queryScript);
			
			if(!(_query instanceof ai.vital.vitalservice.query.graph.VitalQuery)) {
				error("A script must return a select or a graph query variable.")
				return
			}
			
			queryObject = _query
			
			
		} else {
		
			println "Parsing query builder object definition..."
			
			String queryScript = FileUtils.readFileToString(queryScriptFile, "UTF-8")
			
			GraphQueryBuilder builder = new GraphQueryBuilder()
			
			queryObject = builder.queryString(queryScript).toQuery() 
		
		}
		
		if(queryObject instanceof VitalExportQuery) {
			error("Vital export query not supported by vitalquery")
			return
		}
		
		if(outputSparql) {
			queryObject.returnSparqlString = true
		} else {
			queryObject.returnSparqlString = false
		}
		
		VitalService service = Factory.getVitalService()
		println "Obtained vital service, type: ${service.class.canonicalName}"
		
		ResultList rl = null;
		
		if(queryObject instanceof VitalSelectQuery) {
			
			rl = service.selectQuery(queryObject)
			
		} else if(queryObject instanceof VitalGraphQuery) {
		
			rl = service.graphQuery(queryObject)
		 
		} else {
			error "Unexpected query object: ${queryObject.class.canonicalName}"
			return
		}
		
		if( VitalStatus.Status.ok != rl.status.status ) {
			error "Vital query error: ${rl.status.message}"
			return
		}
		
		OutputStream os = null
		BlockCompactStringSerializer writer = null
		OutputStreamWriter osw = null
		
		if(output != null) {

			if(outputSparql) {
				osw = new OutputStreamWriter(new FileOutputStream(output))
			} else {
				writer = new BlockCompactStringSerializer(output);
			}		
			 
		} else {
		
			osw = new OutputStreamWriter(System.out)
			
			if(outputSparql) {
				
			} else {
				writer = new BlockCompactStringSerializer(osw)
			}
		
		
		
		}
		
		
		if(outputSparql) {
			
			String queries = rl.status.message
			
			osw.write(queries)

			osw.flush()
						
			if(output) {
				osw.close()
			}
			
			return
			
		}
		
		println ("Results: ${rl.results.size()}, total: ${rl.totalResults}")
		
		Set<String> uris = new HashSet<String>()
		List<VitalURI> urisList = []

		Map<String, Set<String>> mainObject2Block = new LinkedHashMap<String, Set<String>>();
				
		for(GraphMatch gm : rl) {
			
			/*
			def graphURIs = gm.graphURIs
			for(IProperty uri : graphURIs) {
				String u = uri.toString()
				if(uris.add(u)) {
					urisList.add(VitalURI.withString(u))
				}
			}*/
			
			Set<String> groupURIs = null
			String mainObjectURI = null
			if(group) {
				URIProperty mainURIProp = gm[mainProperty]
				if( mainURIProp == null ) error("No bound variable in graph match: ${mainProperty}, make sure the query binds to it")
				mainObjectURI = mainURIProp.get()
				
				groupURIs = mainObject2Block.get(mainObjectURI)
				
				if(groupURIs == null) {
					groupURIs = new HashSet<String>()
					mainObject2Block.put(mainObjectURI, groupURIs)
				}
				
			}
			
			for(Entry<String, Object> e : gm.getOverriddenMap().entrySet()) {
				
				URIProperty uri = e.getValue()
				String u = uri.get()
				if(uris.add(u)) {
					urisList.add(VitalURI.withString(u))
				}
				
				if(group) {
					if(!u.equals(mainObjectURI)) {
						groupURIs.add(u)
					}
				}
				
				
			}
			
		}
		
		println "Resolving ${uris.size()} URIs ..."
		
		List<GraphObject> objects = []
		Map<String, GraphObject> mapped = [:]
		if(uris.size() > 0) {
			objects = service.get(urisList, GraphContext.ServiceWide, [])
			for(GraphObject g : objects) {
				mapped.put(g.URI, g)
			}
		}
		
		int i = 0
		
		if(group) {
			
			
			//just analyze groups
			
			for(Entry<String, Set<String>> entry : mainObject2Block.entrySet()) {
				
				String mainURI = entry.getKey()
				
				GraphObject mainObj = mapped.get(mainURI)
				
				if(mainObj == null) error("Main object not found: ${mainURI}")
				
				writer.startBlock()
				
				writer.writeGraphObject(mainObj)

				for(String uri : entry.getValue() ) {

					GraphObject g = mapped.get(uri)					
					
					if(g != null) {
						writer.writeGraphObject(g)
					} else {
						error("Graph object not found: ${uri}")
					}
				}				
				
				writer.endBlock()
				
				i++
				
			}
			
			
		} else {
		
			boolean started = false
					
			for(GraphObject r : objects) {
						
				if(!started) {
					writer.startBlock()
					started = true
				}	
						
				writer.writeGraphObject(r)
				i++
						
			}
			
			if(started) {
				writer.endBlock()
			}
		}
		
		
		if(output != null) {
			writer.close()
		}
		
		if(osw != null) {
			osw.flush()
		}

		if(output != null) {
			println "saved ${i} ${group ? 'block' : 'object'}${i != 1 ? 's': ''}"
		}
				
		
	}
	

}
