package ai.vital.vitalutilcommands

import ai.vital.endpoint.EndpointType;
import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExportQuery
import ai.vital.vitalservice.segment.VitalSegment
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Node
import ai.vital.vitalsigns.ontology.VitalCoreOntology;

import java.util.zip.GZIPOutputStream

import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory;


/**
 * Exports data from segment - dumps all objects into single block file
 * 
 * @author Derek
 *
 */
class VitalExport extends AbstractUtil {

	static int DEFAULT_BLOCK_SIZE = 10

	static int DEFAULT_LIMIT = 100
		
	public static void main(String[] args) {
		
		def cli = new CliBuilder(usage: 'vitalexport [options]')
		cli.with {
			h longOpt: "help", "Show usage information", args: 0, required: false
			o longOpt: "output", "output block file or remote temp file name, supported extensions: .vital[.gz] .nt[.gz]", args:1, required: true
			ow longOpt: "overwrite", "overwrite output file", args: 0, required: false
			s longOpt: "segment", "target segment", args: 1, required: true
			b longOpt: "block", "block size (only .vital[.gz]), default ${DEFAULT_BLOCK_SIZE}", args: 1, required: false
			bf longOpt: "bigFiles", "[true|false] flag, force big files flag (only vitalprime), default true", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		
		def options = cli.parse(args)
		
		if(!options || options.h) return
		
		
		Boolean overwrite = Boolean.TRUE.equals(options.ow)
		
		Boolean bigFilesForced = options.bf ? Boolean.parseBoolean(options.bf) : null
		
		String segment = options.s
		
		String profile = options.prof ? options.prof : null
		
		Integer blockSize = DEFAULT_BLOCK_SIZE
		if(options.b) {
			blockSize = Integer.parseInt(options.b)
		}
		
		
		println "Segment: ${segment}"
		println "block size: ${blockSize}"

		if(profile != null) {
			println "Setting custom vital service profile: ${profile}"
			VitalServiceFactory.setServiceProfile(profile)
		} else {
			println "Using default vital service profile..."
		}
		
		VitalService service = VitalServiceFactory.getVitalService()
		println "Obtained vital service, type: ${service.getEndpointType()}, organization: ${service.getOrganization().ID}, app: ${service.getApp().ID}"
		
		
		VitalSegment segmentObj = null;
		for(VitalSegment s : service.listSegments()) {
			if(s.ID== segment) {
				segmentObj = s
				break
			}
		}
		
		if(!segmentObj) {
			error "Segment not found: ${segment}"
			return
		}
		
		
		boolean bigFiles = service.getEndpointType() == EndpointType.VITALPRIME
		
		println "Big files mode ? ${bigFiles} (service.getEndpointType().getName())"
		
		if(bigFiles) {
			if(bigFilesForced != null) {
				println "Forced big files setting: ${bigFilesForced}"
				bigFiles = bigFilesForced.booleanValue()
			}			
		} else {
			if(bigFilesForced != null) {
				println "WARNING: ignoring bigFiles flag - not a vitalprime endpoint"
			}
		}
		
		File output = new File(options.o)
		
		if(!blockFileNameFilter.accept(output) && !ntripleFileFilter.accept(output)) {
			error("Output file name must end with .vital[.gz] or .nt[.gz]: ${output.name}")
			return
		}
		
		if(bigFiles) {

			
			String outFile = options.o
			
			println "Output remote temp file name: ${outFile}"
			println "Overwrite ? ${overwrite}"
			
			if(outFile.contains("/") || outFile.contains("\\")){
				error("In big files mode the output path must be a single file")
			}
			
			VITAL_Node n = VitalFtpCommand.getFile(service, outFile)
			
			if(n) {
				if(!overwrite) error("remote output file already exists: ${outFile} - use --overwrite param")
				if(n.active) error("remote output file already exists and is in use")
			}
			
			ResultList rl = null
			 
			try {
				
				rl = service.callFunction("commons/scripts/RunJob.groovy", [function: 'commons/scripts/ExportToTempFile.groovy', outputFilename: outFile, blockSize: blockSize, segment: segment, limit: DEFAULT_LIMIT])
			} catch(Exception e) {
				error e.localizedMessage
			}		
			
			if(rl.status.status == VitalStatus.Status.ok) {
				
				println "Export job started - ID: ${rl.status.message}"
				println "Use ftp command to get the results when they're ready (file not in use)"
				
			} else {
			
				error "${rl.status}"
			
			}
			
		} else {
		
			println "Output file: ${output.absolutePath}"
			println "Overwrite ? ${overwrite}"
		
			if(output.exists() && !overwrite) {
				error("Output file already exists - use '-ow' option to overwrite - ${output.absolutePath}")
				return
			}
		
			int limit = DEFAULT_LIMIT
			
			VitalExportQuery sq = new VitalExportQuery()
			sq.segments = [segmentObj]
			sq.limit = limit
			
			int offset = 0;
			
			int total = -1
			
			List<GraphObject> buffer = []
			
			
			BlockCompactStringSerializer writer = null
			
			BufferedOutputStream ntOut = null;
			Model model = ModelFactory.createDefaultModel()
			
			if(blockFileNameFilter.accept(output)) {
				
				writer = new BlockCompactStringSerializer(output)
				
			} else if(ntripleFileFilter.accept(output)){
			
				OutputStream os = new FileOutputStream(output)
				if(output.name.endsWith(".gz")) {
					os = new GZIPOutputStream(os)
				}
				
				ntOut = new BufferedOutputStream(os)
				
			} else {
				error "Unhandled file: ${output}"
			}
			
			
			int c = 0
			
			int blocks = 0
			
			long start = t()
			
			while(offset >= 0) {
			
				sq.offset = offset
				
				ResultList rl = service.query(sq)
				
				if(total < 0) total = rl.totalResults != null ? rl.totalResults.intValue() : 0
				
				offset += limit
				
				//initially total may be unknown, just iterate until no objec
				
				if((total > 0 && offset >= total) || rl.results.size() < 1) {
					offset = -1
				}
				
				for(ResultElement r : rl.results) {
					
					if(writer != null) {
						
						buffer.add(r.graphObject)
						
						if(buffer.size() >= blockSize) {
							
							writer.startBlock()
							
							for(GraphObject g : buffer) {
								writer.writeGraphObject(g)
							}
							
							writer.endBlock()
							
							blocks++
							
							buffer.clear()
							
						}
						
					} else {
					
						
						r.graphObject.toRDF(model)
						
						model.write(ntOut, "N-TRIPLE")
						
						model.removeAll()
					
						blocks++
						
					}
					
					
					c++
					
				}
				
				println "Exported ${c} of ${total} ..."
		
			}
			
			if(buffer.size() > 0) {
				
				writer.startBlock()
				
				for(GraphObject g : buffer) {
					writer.writeGraphObject(g)
				}
				
				writer.endBlock()
				
				blocks++
				
			}
			
			if(writer != null) {
				writer.close()
			}
			
			if(ntOut != null) {
				ntOut.close()
			}
			
			println "Total graph objects exported: ${c}, blocks count: ${blocks}, ${t() - start}ms"
			
		}
	}
	
}
