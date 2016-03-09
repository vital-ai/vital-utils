package ai.vital.vitalutilcommands

import java.util.Map.Entry

import org.apache.commons.io.FileUtils;

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalsigns.model.property.URIProperty
import ai.vital.query.querybuilder.VitalBuilder
import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.Connector;
import ai.vital.vitalservice.query.Destination;
import ai.vital.vitalservice.query.GraphElement;
import ai.vital.vitalservice.query.QueryContainerType;
import ai.vital.vitalservice.query.Source;
import ai.vital.vitalservice.query.VitalGraphArcContainer;
import ai.vital.vitalservice.query.VitalGraphArcElement
import ai.vital.vitalservice.query.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.VitalGraphQuery
import ai.vital.vitalservice.query.VitalGraphQueryPropertyCriterion;
import ai.vital.vitalservice.query.VitalGraphQueryTypeCriterion
import ai.vital.vitalservice.query.VitalSelectQuery
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.meta.PathElement
import ai.vital.vitalsigns.model.GraphMatch;
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.model.VITAL_Node;
import ai.vital.vitalsigns.model.VitalApp
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalServiceKey;


class VitalDeleteCommand extends AbstractUtil {

	static List expandedEndpoints = [EndpointType.VITALPRIME, EndpointType.ALLEGROGRAPH, EndpointType.INDEXDB]
	
	public static void main(String[] args) {
		
		def cli = new CliBuilder(usage: 'vitaldelete [options]', stopAtNonOption: false)
		cli.with {
			h longOpt: "help", "Show usage information", args: 0, required: false
			q longOpt: "query", "select query file, limit and offet params are ignored! all results are to be deleted, mutually exclusive with segment,  .groovy or .builder", args: 1, required: false
			s longOpt: "segment", "segment to purge - delete all data in the segment, mutually exlusive with query", args: 1, required: false
			e longOpt: "expanded", "delete expanded", args: 0, required: false
			c longOpt: "check", "check the objects list (do not delete)", args: 0, required: false
			b longOpt: "background", "delete in as background job (vitalprime only)", args: 0, required: false
			sk longOpt: 'service-key', "vital service key, default ${defaultServiceKey}", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			ds longOpt: 'dataset-uri', 'optional dataset URI (provenance) filter, applied to segment mode only', args: 1, required: false
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
			cli.usage()
			return
		}

		
		File queryScriptFile = options.q ? new File(options.q) : null;
		
		VitalServiceKey vitalServiceKey = getVitalServiceKey(options)
		
		String segment = options.s ? options.s : null
		
		if(queryScriptFile != null && segment != null) {
			error("Query script file and segment params are mutually exclusive");
			return
		} else if(queryScriptFile == null && segment == null) {
			error("Either query script file or segment param must be provided")
			return
		}
		
		String profile = options.prof ? options.prof : null
				
		if(profile != null) {
			println "Setting custom vital service profile: ${profile}"
		} else {
			profile = VitalServiceFactory.DEFAULT_PROFILE
			println "Using default vital service profile... ${profile}"
		}
		
		VitalService service = VitalServiceFactory.openService(vitalServiceKey, profile)
		EndpointType et = service.getEndpointType();
		println "Obtained vital service, type: ${et}"
		
		boolean expanded = options.e ? true : false
		boolean check = options.c ? true : false
		
		boolean background = options.b ? true : false
		
		
		String datasetURI = options.ds ? options.ds : null
		

		if(background && et != EndpointType.VITALPRIME) {
			error("Background delete operation may only performed in vitalprime endpoint")
			return
		}
		
		VitalSelectQuery sq = null
		
		if(queryScriptFile != null) {
			
			if(datasetURI != null) {
				error("--dataset-uri must not be used with query script")
				return
			}
			
			if(expanded) {
				
				if(!expandedEndpoints.contains(et)) {
					error("Expanded flag may only be used in the following endpoints: ${expandedEndpoints}")
					return
				}
				
			}
			
			println "Query file: ${queryScriptFile?.absolutePath}"
			if(!queryScriptFile.isFile()) {
				error "Query script file path does not exist or is not a file"
				return
			}
			
			Object _query = null
			
			if(queryScriptFile.name.endsWith(".groovy")) {
				
				println "Parsing groovy query object definition..."
			
				String queryScript = FileUtils.readFileToString(queryScriptFile, "UTF-8")
						
				queryScript =
					"import ai.vital.vitalservice.query.graph.*;\n" +
					queryScript;
				
				
				GroovyShell shell = new GroovyShell();
				_query = shell.evaluate(queryScript);
				
				
			} else if(queryScriptFile.name.endsWith(".builder")) {
			
				String queryScript = FileUtils.readFileToString(queryScriptFile, "UTF-8")
			
				VitalBuilder builder = new VitalBuilder()
			
				_query = builder.queryString(queryScript).toQuery() 
				
			} else {
				error("Input query file name must end with .groovy or .builder : ${queryScriptFile.absolutePath}")
				return
			}
			
			if( ! (_query instanceof VitalSelectQuery) ) throw new Exception("A script must return a select query object.");
			
			sq = (VitalSelectQuery)_query
		
			if( sq.segments == null || sq.segments.isEmpty() ) {
				error("Query segments list cannot be empty")
			}
			
				
		}

		VitalSegment segmentObject = null
				
		if(segment != null) {
			println "Segment: ${segment}"

			segmentObject = service.getSegment(segment)
			
			if(segmentObject == null) {
				error("Segment not found: ${segment}")
				return
			}
			
		}
				
		println "Dataset URI: ${datasetURI}"
		println "Expanded ? ${expanded}"
		println "Check ? ${check}"
		

		if(segmentObject != null) {
			
			if(datasetURI != null) {

				if(expanded) {
					println "Expanded flag ignored with datasetURI and segment mode"
					expanded = false
				}
				
				if(et != EndpointType.VITALPRIME) {
					
					sq = new VitalBuilder().query {
						
						SELECT {
							
							value segments: [segmentObject]
							
							node_constraint { VITAL_Node.props().provenance.equalTo(URIProperty.withString(datasetURI)) }
							
						}
						
					}.toQuery()
					
				} else {
				
					//just delete it remotely
					if(check) {
						println "All data with datasetURI: ${datasetURI.size()} will be delete from segment: ${segment} remotely"
						return
					}
				
				}
				
				
			} else {
			
				if(check) {
					println "Whole segment: ${segment} will be purged, check=true - exiting."
					return
				}
				
				//execute a simple delete that will purge all segment
				URIProperty matchAllURI = URIProperty.getMatchAllURI(segmentObject)
				VitalStatus status = service.delete(matchAllURI)
				println "status: ${status}"
				return
				
			}
			
		}		
		
		
		if(check || et != EndpointType.VITALPRIME) {
			
			println "Checking objects list..."
			
			int maxresults = 1000
			int offset = 0
			
			int total = 0
			
			Set<String> uris = new HashSet<String>()
			
			while(offset >= 0) {
				
				sq.offset = offset
				sq.limit = maxresults
				
				ResultList rl = null
				try {
					rl = service.query(sq)
					if(rl.status.status != VitalStatus.Status.ok) {
						throw new Exception("Query status: ${rl.status}")
					}
				} catch(Exception e) {
					error e.localizedMessage
				}
				if( rl.totalResults < offset + maxresults && rl.results.size() > 0 ) {
					offset += maxresults
				} else {
					offset = -1
				}
				
				for(ResultElement re : rl.results) {
					
					
					total++
					
					GraphObject g = re.graphObject
					
					uris.add(g.URI)
					
					print "${g.getClass().canonicalName} URI: ${g.URI} "
					
					if(!expanded) {
						print "\n\n"
						continue
					}
					
					print " ... "
					
					VitalGraphQuery gq = getPaths(g.URI, g.getClass(), sq.segments)
					
					if(gq == null) continue;
					
					ResultList gqRl = service.query(gq)					

					int s = gqRl.results.size()

					Set<String> ss = new HashSet<String>()
					
					for(ResultElement x : gqRl.results) {
						GraphMatch gm = x.graphObject
						
						for(Entry<String, Object> e : gm.getPropertiesMap().entrySet()) {
							
							URIProperty uri = e.getValue()
							String u = uri.get()
							ss.add(u)
						}
						
					}
					println " [${ss.size()} expanded objects]"
					
//					for(String u : ss) {
//						println "\t\t${u}"
//					}
					
					uris.addAll(ss)
										
				}
				
				
			}
			
			println "Total select query results: ${total}"
			println "URIs to delete: ${uris.size()}"
			
			if(check) {
				println "check complete"
				return
			}
			
			
			println "Deleting objects list..."
			
			List<URIProperty> urisList = []
			for(String uri : uris) {
				urisList.add(URIProperty.withString(uri))
			}
			
			if(urisList.size() < 1) {
				println "URIs list empty - exiting..."
				return
			}
			
			VitalStatus status = service.delete(urisList)
			
			println "status: ${status}"
			
		} else {
		
		
			ResultList rl = null
			
			String mainScript = 'commons/scripts/DeleteBatch.groovy'
			
			Map params = [selectQuery: sq, expanded: expanded, segment: segmentObject, datasetURI: datasetURI]
			
			String toExecute = mainScript
			if(background) {
				params['function'] = mainScript
				toExecute = "commons/scripts/RunJob.groovy"
			}
			
			try {
				rl = service.callFunction(toExecute, params)
				if(rl.status.status != VitalStatus.Status.ok) throw new Exception("Status: ${rl.status}")
				println "status: ${rl.status}"
				if(!background) {
					println "Objects deleted: ${rl.totalResults}"
				}
			} catch(Exception e) {
				error e.localizedMessage
			}
		
		}
		
		
		
		
	}

	static Map<Class, List<List<PathElement>>> cache = [:]
	
	static VitalGraphQuery getPaths(String rootURI, Class cls, List<VitalSegment> segments) {
		
		//forward pats
		List<List<PathElement>> paths = cache.get(cls) 
		
		if(paths == null) {
			paths = VitalSigns.get().getClassesRegistry().getPaths(cls, true);
			cache.put(cls, paths)
		}
				
		if(paths.size() == 0) {
			println "No taxonomy paths for class: ${cls.canonicalName}"
			return null;
		}

		VitalGraphQuery gq = new VitalGraphQuery();
		gq.segments = segments
		
		//convert paths into query path elements
		
		VitalGraphArcContainer topArc = new VitalGraphArcContainer(QueryContainerType.or, new VitalGraphArcElement(Source.CURRENT, Connector.EMPTY, Destination.EMPTY));
		VitalGraphCriteriaContainer cc = new VitalGraphCriteriaContainer(QueryContainerType.and)
		VitalGraphQueryPropertyCriterion rootURICrit = new VitalGraphQueryPropertyCriterion(VitalGraphQueryPropertyCriterion.URI)
		rootURICrit.symbol = GraphElement.Source
		rootURICrit.value = new URIProperty(rootURI)
		cc.add(rootURICrit)
		topArc.add(cc)
		
		gq.setTopContainer(topArc)
		
		VitalGraphArcContainer parent = topArc 
		
		
		for(List<PathElement> path : paths) {
			
			VitalGraphArcContainer currentParent = parent
			
			PathElement previousEl = null
			
			for(PathElement el : path) {
				
				Source src = null
				
				if(el.reversed) {
					src = Source.CURRENT
				} else if(previousEl == null) {
					src = Source.PARENT_SOURCE
				} else {
					src = previousEl.reversed ? Source.PARENT_SOURCE : Source.PARENT_DESTINATION
				}
				
				Destination dest = null
				
				if(el.reversed) {
					if(previousEl == null) {
						dest = Destination.PARENT_SOURCE
					} else {
						dest = previousEl.reversed ? Destination.PARENT_SOURCE : Destination.PARENT_DESTINATION 
					}
				} else {
					dest = Destination.CURRENT
				}
				
				
				VitalGraphArcContainer newContainer = new VitalGraphArcContainer(QueryContainerType.and, new VitalGraphArcElement(src, Connector.EDGE, dest));
				
				VitalGraphCriteriaContainer subCC = new VitalGraphCriteriaContainer(QueryContainerType.and)
				
				subCC.add(new VitalGraphQueryTypeCriterion(GraphElement.Connector, el.edgeClass));
				//set edge type
				newContainer.add(subCC)
				
				currentParent.add(newContainer)
				
				currentParent = newContainer
			
				previousEl = el
			}
			
		}
		
		cache.put(cls, paths)
		
		return gq
		
	}
}
