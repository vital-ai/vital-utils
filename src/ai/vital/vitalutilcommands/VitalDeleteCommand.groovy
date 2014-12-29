package ai.vital.vitalutilcommands

import org.apache.commons.io.FileUtils;

import ai.vital.endpoint.EndpointType;
import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.factory.Factory
import ai.vital.vitalservice.query.QueryPathElement;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalSelectQuery
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.datatype.VitalURI;
import ai.vital.vitalsigns.graph.Graph;
import ai.vital.vitalsigns.meta.GraphSetsRegistry;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.PathElement


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
					
					List<List<QueryPathElement>> paths = getPaths(g.getClass())
					
					if(paths.size() == 0) {
						println "No taxonomy paths for class: ${g.class.canonicalName}"
						continue
					}
					
					VitalGraphQuery gq = new VitalGraphQuery()
					gq.pathsElements = paths
					gq.rootUris = [g.URI]
					gq.segments = sq.segments

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

	static Map<Class, List<List<QueryPathElement>> > cache = [:]
	
	static List<List<QueryPathElement>> getPaths(Class cls) {
		
		List<List<QueryPathElement>> paths = cache.get(cls)
		
		if(paths != null) return paths
		
		List<List<PathElement>> vpaths = GraphSetsRegistry.get().getPaths(cls);
		
		paths = new ArrayList<List<QueryPathElement>>(vpaths.size());
		
		//convert paths into query path elements
		for(List<PathElement> path : vpaths) {
			
			List<QueryPathElement> qpath = new ArrayList<QueryPathElement>(path.size());
			
			for(PathElement pe : path) {
				Class edgeClass = VitalSigns.get().getGroovyClass(pe.edgeTypeURI);
				if(edgeClass == null) throw new RuntimeException("No edge class found: ${pe.edgeTypeURI}")
				QueryPathElement qpe = new QueryPathElement(edgeClass, null, pe.reversed ? QueryPathElement.Direction.forward : QueryPathElement.Direction.reverse, QueryPathElement.CollectEdges.yes, QueryPathElement.CollectDestObjects.yes);
				qpath.add(qpe);
			}
			
			paths.add(qpath)
			
		}
		
		cache.put(cls, paths)
		
		return paths
		
	}
}
