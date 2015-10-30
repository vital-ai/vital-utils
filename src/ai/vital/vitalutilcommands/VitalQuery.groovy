package ai.vital.vitalutilcommands

import ai.vital.vitalsigns.model.property.BooleanProperty;
import ai.vital.vitalsigns.model.property.IProperty
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.query.querybuilder.VitalBuilder;
import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExportQuery;
import ai.vital.vitalservice.query.VitalGraphQuery
import ai.vital.vitalservice.query.VitalPathQuery;
import ai.vital.vitalservice.query.VitalSelectAggregationQuery;
import ai.vital.vitalservice.query.VitalSelectQuery
import ai.vital.vitalservice.query.VitalSparqlQuery;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.meta.GraphContext;
import ai.vital.vitalsigns.model.AggregationResult;
import ai.vital.vitalsigns.model.GraphMatch;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.ontology.VitalCoreOntology;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;

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
			o longOpt: "output", "output (.vital[.gz]|.sparql|.txt) block or sparql or txt file (depending on -s flag and query type), it prints to console otherwise, txt file for select distinct case only", args:1, required: false
			ow longOpt: "overwrite", "overwrite output file", args: 0, required: false
			q longOpt: "query", "qurery file (.groovy|.builder) - groovy or query builder defined query", args: 1, required: true
			s longOpt: "tosparql", "output the query as sparql instead of executing it", args: 0, required: false
			g longOpt: "group", "group graph matches into blocks, explicit boolean flag parameter [true|false], requires mainProp, only graph query", args: 1, required: false
			mp longOpt: "mainProp", "main bound property, required when --group=true", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
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

		
		boolean blockOutput = false;
		boolean txtOutput = false;
		
		
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
			
				if( output.name.endsWith(".txt") ) {
					txtOutput = true
				} else if( blockFileNameFilter.accept(output) ) {
					blockOutput = true
				} else {
					error("Output file name must end with .vital[.gz] or .txt: ${output.absolutePath}")
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
			VitalServiceFactory.setServiceProfile(serviceProfile)
		} else {
			println "Default service profile"
		}

		ai.vital.vitalservice.query.VitalQuery queryObject = null

		VitalSigns.get()

		if(groovyInput) {

			println "Parsing groovy query object definition..."

			String queryScript = FileUtils.readFileToString(queryScriptFile, "UTF-8")

			queryScript =
					"import ai.vital.vitalservice.query.graph.*;\n" +
					queryScript;


			GroovyShell shell = new GroovyShell();
			Object _query = shell.evaluate(queryScript);

			if(!(_query instanceof ai.vital.vitalservice.query.VitalQuery)) {
				error("A script must return a select or a graph query variable.")
				return
			}

			queryObject = _query
		} else {

			println "Parsing query builder object definition..."

			String queryScript = FileUtils.readFileToString(queryScriptFile, "UTF-8")

			VitalBuilder builder = new VitalBuilder()

			queryObject = builder.queryString(queryScript).toQuery()
		}

		if(queryObject instanceof VitalExportQuery) {
			error("Vital export query not supported by vitalquery")
			return
		}

		boolean selectQuery = queryObject instanceof VitalSelectQuery

		boolean pathQuery = queryObject instanceof VitalPathQuery
		
		boolean graphQuery = queryObject instanceof VitalGraphQuery
		
		boolean sparqlQuery = queryObject instanceof VitalSparqlQuery
		
		boolean aggregationQuery = false 
		boolean selectDistinctQuery = false
		if(selectQuery) {
			if(queryObject instanceof VitalSelectAggregationQuery) {
				println "Aggregation Function Select Query ... (result will not be written into file)"
				aggregationQuery = true
			} else {
				if(queryObject.isDistinct()) {
					println "Select distinct query"
					selectDistinctQuery = true
					if(!outputSparql && output != null && !txtOutput) {
						error("Select Distinct query output file must be a .txt file")
						return
					}	
				} else {
					println "Regular Select Query ...";
					if(!outputSparql && output != null && !blockOutput) {
						error("Regular select query output file must be a .vital[.gz] file")
						return
					}
				}
			}
		} else if(graphQuery) {
			println "Graph Query ...";
			if(!outputSparql && output != null && !blockOutput) {
				error("Graph query output file must be a .vital[.gz] file")
				return
			}
		} else if(pathQuery) {
			println "Path Query ..."
			
			if(outputSparql) {
				error("Path query does not use sparql query")
				return
			}
			
			if(!blockOutput) {
				error("Path query output file must be a .vital[.gz] file")
				return
			}
		} else if(sparqlQuery) {
			error("Sparql query type not supported")
		} else {
			error("Unhandled query type: ${queryObject?.class.simpleName}")
			return
		}

		if(outputSparql) {
			queryObject.returnSparqlString = true
		} else {
			queryObject.returnSparqlString = false
		}

		VitalService service = VitalServiceFactory.getVitalService()
		println "Obtained vital service, type: ${service.endpointType}"

		ResultList rl = null;

		if(queryObject instanceof VitalSelectQuery || queryObject instanceof VitalGraphQuery || queryObject instanceof VitalPathQuery ) {

			rl = service.query(queryObject)
		
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
		OutputStreamWriter txtWriter = null
		OutputStreamWriter osw = null

		if(output != null) {

			if(outputSparql) {
				osw = new OutputStreamWriter(new FileOutputStream(output))
			} else if(aggregationQuery) {
			} else if(selectDistinctQuery) {
				txtWriter = new OutputStreamWriter(new FileOutputStream(output))
			} else {
				writer = new BlockCompactStringSerializer(output);
			}
		} else {

			osw = new OutputStreamWriter(System.out)

			if(outputSparql) {
			} else if(aggregationQuery) {
			} else if(selectDistinctQuery) {
				txtWriter = osw
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
		
		if(aggregationQuery) {
			
			AggregationResult ar = rl.results[0].graphObject
			
			println "Aggregation ${ar.aggregationType} value: ${ar.value}"
			
			return
		}

		println ("Results: ${rl.results.size()}, total: ${rl.totalResults}")

		if(selectDistinctQuery) {
			
			boolean first = true
			
			for(GraphMatch g : rl) {
				
				if(first) {
					first = false
				} else {
					txtWriter.write("\n")
				}
				
				txtWriter.write( "" + g.value )
				
			}
			
			txtWriter.close()
			
			return
		}
		
		Set<String> uris = new HashSet<String>()
		List<URIProperty> urisList = []

		Map<String, Set<String>> mainObject2Block = new LinkedHashMap<String, Set<String>>();


		List<GraphObject> objects = []
		Map<String, GraphObject> mapped = [:]

		if(selectQuery || pathQuery) {

			for(GraphObject g : rl) {

				objects.add(g)
			}
		} else {
			for(GraphMatch gm : rl) {

				Set<String> groupURIs = null
				String mainObjectURI = null
				if(group) {
					URIProperty mainURIProp = gm[mainProperty].unwrapped()
					if( mainURIProp == null ) error("No bound variable in graph match: ${mainProperty}, make sure the query binds to it")
					mainObjectURI = mainURIProp.get()

					groupURIs = mainObject2Block.get(mainObjectURI)

					if(groupURIs == null) {
						groupURIs = new HashSet<String>()
						mainObject2Block.put(mainObjectURI, groupURIs)
					}
				}

				for(Entry<String, Object> e : gm.getPropertiesMap().entrySet()) {

					//skip URI and vitaltype properties
					if(VitalCoreOntology.URIProp.getURI().equals(e.key) || VitalCoreOntology.vitaltype.getURI().equals(e.key)) {
						continue;
					}
					
					def un = e.getValue().unwrapped()
					
					if(!(un instanceof URIProperty)) continue
					
					
					URIProperty uri = un
					String u = uri.get()
					
					if(uris.add(u)) {
						urisList.add(URIProperty.withString(u))
					}

					if(group) {
						if(!u.equals(mainObjectURI)) {
							groupURIs.add(u)
						}
					}
				}
			}

			println "Resolving ${uris.size()} URIs ..."

			if(uris.size() > 0) {
				ResultList r = service.get(GraphContext.ServiceWide, urisList)
				for(GraphObject g : r) {
					objects.add(g)
					mapped.put(g.URI, g)
				}
			}
		}


		int i = 0

		if(selectQuery || pathQuery) {
		
			for(GraphObject g : objects) {
				
				writer.startBlock();
				writer.writeGraphObject(g)
				writer.endBlock();
				i++;
			}	
			
			
		} else {
		
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
