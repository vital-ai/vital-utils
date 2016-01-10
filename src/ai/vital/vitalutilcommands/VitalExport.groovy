package ai.vital.vitalutilcommands

import ai.vital.query.querybuilder.VitalBuilder
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.ServiceOperations;
import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalExportQuery
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.VITAL_Node
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalServiceKey;
import ai.vital.vitalsigns.ontology.VitalCoreOntology;

import java.util.zip.GZIPOutputStream

import org.apache.commons.io.FileUtils;

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
		
		def cli = new CliBuilder(usage: 'vitalexport [options]', stopAtNonOption: false)
		cli.with {
			h longOpt: "help", "Show usage information", args: 0, required: false
			ex longOpt: "export-builder", "Export builder file (.builder or .groovy extension), overrides other parameters except profile", args: 1, required: false
			o longOpt: "output", "output block file or remote temp file name, supported extensions: .vital[.gz] .nt[.gz]", args:1, required: false
			ow longOpt: "overwrite", "overwrite output file", args: 0, required: false
			s longOpt: "segment", "target segment", args: 1, required: false
			b longOpt: "block", "block size (only .vital[.gz]), default ${DEFAULT_BLOCK_SIZE}", args: 1, required: false
			bulk longOpt: "bulk-mode", "bulk export data, only block compact string format", args: 0, required: false
			bf longOpt: "bigFiles", "[true|false] flag, force big files flag (only vitalprime), default true", args: 1, required: false
			sk longOpt: 'service-key', "vital service key, default ${defaultServiceKey}", args: 1, required: false
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
		
		VitalServiceKey serviceKey = getVitalServiceKey(options)
		
		String exportBuilder = options.ex ? options.ex : null
		
		String profile = options.prof ? options.prof : null
		
		if(exportBuilder != null) {
			
			File exportBuilderFile = new File(exportBuilder)
			
			println ("Export builder file provided ${exportBuilderFile.absolutePath} - ignoring other params");

			if( ! ( exportBuilderFile.name.endsWith(".groovy") || exportBuilderFile.name.endsWith(".builder") ) ) error("Builder file must end with .groovy or .builder")			
			if(!exportBuilderFile.isFile()) error("Export builder file does not exist or not a file: ${exportBuilderFile.absolutePath}")
			
			
			def builder = new VitalBuilder()
			
			println ("Parsing export builder file...")
			ServiceOperations ops = builder.queryString(FileUtils.readFileToString(exportBuilderFile, "UTF-8")).toService()
			
			if(ops.exportOptions == null) error("No export options parsed from builder file")

			if(ops.getImportOptions() != null) error("Expected only export options, not import options")
			if(ops.getOperations() != null && !ops.getOperations().isEmpty()) error("Expected only export options, no other operations")
			
			if(profile != null) {
				println "Setting custom vital service profile: ${profile}"
			} else {
				profile = VitalServiceFactory.DEFAULT_PROFILE
				println "Using default vital service profile... ${VitalServiceFactory.DEFAULT_PROFILE}"
			}
			
			VitalService service = VitalServiceFactory.openService(serviceKey, profile)
			println "Obtained vital service, type: ${service.getEndpointType()}, organization: ${service.getOrganization().organizationID}, app: ${service.getApp().appID}"
		
			VitalStatus vs = service.doOperations(ops)
			
			if(vs.status != VitalStatus.Status.ok) {
				error "Export error: " + vs.message	
			}
			
			println "DONE"
			
			return 	
		}
		
		
		Boolean overwrite = Boolean.TRUE.equals(options.ow)
		
		Boolean bigFilesForced = options.bf ? Boolean.parseBoolean(options.bf) : null
		
		String segment = options.s ? options.s : null
		
		if(segment == null) error("No segment parameter")
		
		File output = new File(options.o)
		
		if(!options.o) throw new Exception("No output parameter")
		
		Integer blockSize = DEFAULT_BLOCK_SIZE
		if(options.b) {
			blockSize = Integer.parseInt(options.b)
		}
		
		
		println "Segment: ${segment}"
		println "block size: ${blockSize}"

		boolean bulkMode = options.bulk ? true : false
		println "bulk mode ? $bulkMode"
		
		if(bulkMode) {
			if(bigFilesForced?.booleanValue()) error("Cannot use bulk and forced bigfiles mode")
		}
		
		if(profile != null) {
			println "Setting custom vital service profile: ${profile}"
		} else {
			println "Using default vital service profile... ${VitalServiceFactory.DEFAULT_PROFILE}"
			profile = VitalServiceFactory.DEFAULT_PROFILE
		}
		
		VitalService service = VitalServiceFactory.openService(serviceKey, profile)
		println "Obtained vital service, type: ${service.getEndpointType()}, organization: ${service.getOrganization().organizationID}, app: ${service.getApp().appID}"
		
		
		VitalSegment segmentObj = service.getSegment(segment)
		
		if(!segmentObj) {
			error "Segment not found: ${segment}"
			return
		}
		
		
		boolean bigFiles = false
		
		
		if(bulkMode) {
			
		} else {
		
			bigFiles = service.getEndpointType() == EndpointType.VITALPRIME
			
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
		
		}
		
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
			
		} else if(bulkMode) {
		
		
			if(!output.getName().endsWith(".vital") || output.getName().endsWith(".vital.gz")) error("only .vital[.gz] output file in bulk mode")
			
			OutputStream outputStream = new FileOutputStream(output)
			
			if( output.getName().endsWith(".vital.gz") ) {
				outputStream = new GZIPOutputStream(outputStream)
			}
			
			println "Bulk dumping to vital format..."
			BufferedOutputStream os = new BufferedOutputStream(outputStream)
			
			service.bulkExport(segmentObj, os)
			
			os.close()
			
			println "Bulk export complete"
			
			return
		
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
