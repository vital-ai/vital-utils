package ai.vital.vitalutilcommands

import org.apache.commons.io.FileUtils;

import ai.vital.endpoint.EndpointType;
import ai.vital.property.URIProperty
import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.factory.Factory
import ai.vital.vitalservice.query.QueryPathElement;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.graph.Connector;
import ai.vital.vitalservice.query.graph.Destination;
import ai.vital.vitalservice.query.graph.GraphElement;
import ai.vital.vitalservice.query.graph.QueryContainerType;
import ai.vital.vitalservice.query.graph.Source;
import ai.vital.vitalservice.query.graph.VitalGraphArcContainer;
import ai.vital.vitalservice.query.graph.VitalGraphArcElement
import ai.vital.vitalservice.query.graph.VitalGraphCriteriaContainer;
import ai.vital.vitalservice.query.graph.VitalGraphQuery
import ai.vital.vitalservice.query.graph.VitalGraphQueryPropertyCriterion;
import ai.vital.vitalservice.query.graph.VitalSelectQuery
import ai.vital.vitalservice.segment.VitalSegment;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.datatype.VitalURI;
import ai.vital.vitalsigns.meta.PathElement
import ai.vital.vitalsigns.model.GraphObject;


class VitalDeleteCommand extends AbstractUtil {

	public static void main(String[] args) {
		
		def cli = new CliBuilder(usage: 'vitalexport [options]')
		cli.with {
			h longOpt: "help", "Show usage information", args: 0, required: false
			q longOpt: "query", "select query file, limit and offet params are ignored! all results are to be deleted", args: 1, required: true
			e longOpt: "expanded", "delete expanded", args: 0, required: false
			c longOpt: "check", "check the objects list (do not delete)", args: 0, required: false
		}
		
		def options = cli.parse(args)
		
		if(!options || options.h) return
		
		//TODO implement the queries!
		if(true) {
			System.err.println "NOT IMPLEMENTED"
			System.exit(-1)
		}
		VitalService service = Factory.getVitalService()
		println "Obtained vital service, type: ${EndpointType.VITALPRIME}"
		
		if(service.endpointType != EndpointType.VITALPRIME) {
			error "Only ${EndpointType.VITALPRIME} endpoint type supported"
		}
		
		boolean expanded = options.e ? true : false
		boolean check = options.c ? true : false
		
		
		File queryScriptFile = new File(options.q);
		
		println "Query file: ${queryScriptFile.absolutePath}"
		println "Expanded ? ${expanded}"
		println "Check ? ${check}"
		
		if(!queryScriptFile.isFile()) {
			error "Query script file path does not exist or is not a file"
			return
		}
		
		String queryScript = FileUtils.readFileToString(queryScriptFile, "UTF-8")
		
		queryScript =
		"import ai.vital.vitalservice.query.*;\n" +
		"import ai.vital.vitalservice.query.VitalQueryContainer.Type;\n" +
		queryScript;
		
		
		GroovyShell shell = new GroovyShell();
		Object _query = shell.evaluate(queryScript);

		if( ! (_query instanceof VitalSelectQuery) ) throw new Exception("A script must return a select query variable.");
		
		VitalSelectQuery sq = (VitalSelectQuery)_query

		if( sq.segments == null || sq.segments.isEmpty() ) {
			error("Query segments list cannot be empty")
		}
				
		if(check) {
			
			println "Checking objects list..."
			
			int maxresults = 1000
			int offset = 0
			
			int total =0
			
			Set<String> uris = new HashSet<String>()
			
			while(offset >= 0) {
				
				sq.offset = offset
				sq.limit = maxresults
				
				ResultList rl = null
				try {
					rl = service.selectQuery(sq)
					if(rl.status.status != VitalStatus.Status.ok) {
						throw new Exception("Query status: ${rl.status}")
					}
				} catch(Exception e) {
					error e.localizedMessage
				}
				if( rl.totalResults < offset + maxresults ) {
					offset += maxresults
				} else {
					offset = -1
				}
				
				for(ResultElement re : rl.results) {
					
					
					total++
					
					GraphObject g = re.graphObject
					
					uris.add(g.URI)
					
					print "${g.class.canonicalName} URI: ${g.URI} "
					
					if(!expanded) {
						print "\n\n"
						continue
					}
					
					print " ... "
					
					VitalGraphQuery gq = getPaths(g.URI, g.getClass(), sq.segments)
					
					if(gq == null) continue;
					
					ResultList gqRl = service.graphQuery(gq)					

					int s = gqRl.results.size()

					for(ResultElement x : gqRl.results) {
						GraphObject y = x.graphObject
						uris.add(y.URI)
						if(y.URI == g.URI) s--
						
					}
					println " [${s} expanded objects]"
					
					for(ResultElement x : gqRl.results) {
						GraphObject y = x.graphObject
						if(y.URI == g.URI) continue
						println "\t\t${y.class.canonicalName} URI: ${y.URI} "
					}
										
				}
				
				
			}
			
			println "Total select query results: ${total}"
			println "URIs to delete: ${uris.size()}"
			
		} else {
		
			ResultList rl = null
			
			try {
				rl = service.callFunction("commons/scripts/RunJob.groovy", [function: 'commons/scripts/DeleteBatch.groovy', selectQuery: sq, expanded: expanded])
				if(rl.status.status != VitalStatus.Status.ok) throw new Exception("Status: ${rl.status}")
				println "${rl.status}"
				println "Objects deleted: ${rl.totalResults}"
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

		VitalGraphQuery gq = new VitalGraphQuery(QueryContainerType.and);
		gq.segments = segments
		
		//convert paths into query path elements
		
		VitalGraphArcContainer topArc = new VitalGraphArcContainer(QueryContainerType.and, new VitalGraphArcElement(Source.CURRENT, Connector.EMPTY, Destination.EMPTY));
		VitalGraphCriteriaContainer cc = new VitalGraphCriteriaContainer(QueryContainerType.and)
		VitalGraphQueryPropertyCriterion rootURICrit = new VitalGraphQueryPropertyCriterion(VitalGraphQueryPropertyCriterion.URI)
		rootURICrit.symbol = GraphElement.Source
		rootURICrit.value = new URIProperty(rootURI)
		cc.add(rootURICrit)
		topArc.add(cc)
		
		VitalGraphArcContainer parent = topArc 
		
		for(List<PathElement> path : paths) {
			
			
			/*
			
			List<QueryPathElement> qpath = new ArrayList<QueryPathElement>(path.size());
			
			for(PathElement pe : path) {
				Class edgeClass = VitalSigns.get().getGroovyClass(pe.edgeTypeURI);
				if(edgeClass == null) throw new RuntimeException("No edge class found: ${pe.edgeTypeURI}")
				QueryPathElement qpe = new QueryPathElement(edgeClass, null, pe.reversed ? QueryPathElement.Direction.forward : QueryPathElement.Direction.reverse, QueryPathElement.CollectEdges.yes, QueryPathElement.CollectDestObjects.yes);
				qpath.add(qpe);
			}
			
			paths.add(qpath)
			*/
		}
		
		cache.put(cls, paths)
		
		return paths
		
	}
}
