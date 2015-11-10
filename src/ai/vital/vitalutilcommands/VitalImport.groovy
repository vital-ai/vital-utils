package ai.vital.vitalutilcommands

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.text.DecimalFormat;
import java.util.zip.GZIPInputStream

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import ai.vital.query.querybuilder.VitalBuilder
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.model.VITAL_Node;
import ai.vital.vitalsigns.model.VitalSegment
import ai.vital.vitalsigns.model.VitalServiceKey;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.vitalsigns.rdf.VitalNTripleIterator
import ai.vital.vitalservice.ServiceOperations
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalutilcommands.io.ProgressInputStream

/**
 * Imports block file(s) into vital service segment
 * @author Derek
 *
 */
class VitalImport extends AbstractUtil {

	static Integer DEFAULT_BLOCKS = 1
	
	static main(args) {
	
		def cli = new CliBuilder(usage: 'vitalimport [options]')
		cli.with {
			h longOpt: "help", "Show usage information", args: 0, required: false
			im longOpt: "import-builder", "Import builder file (.builder or .groovy extension), overrides other parameters except profile", args: 1, required: false
			f longOpt: "file", "input file or directory, supported extensions .vital[.gz], .nt[.gz], .groovy|.builder", args:1, required: false
			s longOpt: "segment", "target segment", args: 1, required: false
			b longOpt: "batch", "blocks per batch, default: ${DEFAULT_BLOCKS}", args: 1, required: false
			bulk longOpt: "bulk-mode", "bulk import data without checking existing objects, only block compact string format", args: 0, required: false
			v longOpt: "verbose", "report import progress (only in non-big-files mode)", args: 0, required: false
			c longOpt: "check", "check input files only - DOES NOT IMPORT, mutually exclusive with --skip-check", args: 0, required: false
			sc longOpt: 'skip-check', "skips the default data validation, mutually exclusive with --check", args: 0, required: false 
			bf longOpt: "bigFiles", "[true|false] flag, force big files flag (only vitalprime), default true", args: 1, required: false
			sk longOpt: 'service-key', "vital service key, default ${defaultServiceKey}", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			//bf longOpt: 'bigfiles', "the flag to import the data in background, after they're uploaded", args: 0 , required: false
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
		
		String profile = options.prof ? options.prof : null
		
		String importBuilder = options.im ? options.im : null
		
		VitalServiceKey serviceKey = getVitalServiceKey(options)
		
		if(importBuilder != null) {
			
			File importBuilderFile = new File(importBuilder)
			
			println ("Import builder file provided ${importBuilderFile.absolutePath} - ignoring other params");

			if( ! ( importBuilderFile.name.endsWith(".groovy") || importBuilderFile.name.endsWith(".builder") ) ) error("Builder file must end with .groovy or .builder")
			if(!importBuilderFile.isFile()) error("Import builder file does not exist or not a file: ${importBuilderFile.absolutePath}")
			
			
			def builder = new VitalBuilder()
			
			println ("Parsing import builder file...")
			ServiceOperations ops = builder.queryString(FileUtils.readFileToString(importBuilderFile, "UTF-8")).toService()
			
			if(ops.importOptions == null) error("No import options parsed from builder file")

			if(ops.getExportOptions() != null) error("Expected only import options, not export options")
			if(ops.getOperations() != null && !ops.getOperations().isEmpty()) error("Expected only import options, no other operations")
			
			if(profile != null) {
				println "Setting custom vital service profile: ${profile}"
			} else {
				profile = VitalServiceFactory.DEFAULT_PROFILE
				println "Using default vital service profile... ${profile}"
			}
			
			VitalService service = VitalServiceFactory.openService(serviceKey, profile)
			println "Obtained vital service, type: ${service.getEndpointType()}, organization: ${service.getOrganization().organizationID}, app: ${service.getApp().appID}"
		
			VitalStatus vs = service.doOperations(ops)
			
			if(vs.status != VitalStatus.Status.ok) {
				error "Import error: " + vs.message
			}
			
			println "DONE"
			
			return
		}
		
		
		List files = options.fs ? options.fs : null
		
		if(files == null || files.isEmpty()) error("No files parameter")
		
		Integer blocksPerBatch = DEFAULT_BLOCKS
		
		if(options.b) {
			blocksPerBatch = Integer.parseInt(options.b)
		}
		
		String segment = options.s ? options.s : null
		if(segment == null) error("No 'segment' parameter")
		
		println "Segment: ${segment}"
		println "Input paths [${files.size()}]: ${files}"
		println "Blocks per batch: ${blocksPerBatch}"
		boolean verbose = options.v ? true : false
		println "Verbose: ${verbose}"
		boolean check = options.c ? true : false
		println "Check data only ? ${check}"
		boolean skipCheck = options.sc ? true : false
		println "Skip data check ? ${skipCheck}"
		boolean bulkMode = options.bulk ? true : false
		println "bulk mode ? $bulkMode"
		
		if(check && skipCheck) {
			error "--check and --skip-check flags are mutually exclusive"
		}
		
		Boolean bigFilesForced = options.bf ? Boolean.parseBoolean(options.bf) : null
		
		if(bulkMode) {
			
			if(bigFilesForced?.booleanValue()) error("Cannot use bulk and forced bigfiles mode")
			
		}
		
		List<File> filesObjs = []
		
		for(String f : files) {
		
			File fo = new File(f);
			
			if(!fo.exists()) {
				error "Path does not exist: ${fo.absolutePath}"
				return
			}
			
			if(fo.isFile()) {
				
				if(!blockFileNameFilter.accept(fo) && !ntripleFileFilter.accept(fo) && !builderFileFilter.accept(fo)) {
					error("Cannot accept file: ${fo.absolutePath} - name must end with .vital[.gz] , .nt[.gz] , .groovy , .builder")
					return
				}
				
				filesObjs.add(fo)
				
			} else if ( fo.isDirectory() ) {
			
				for(File x : fo.listFiles(blockFileNameFilter)) {
					if(x.isFile()) filesObjs.add(x)
				}
				
				for(File x : fo.listFiles(ntripleFileFilter)) {
					if(x.isFile()) filesObjs.add(x)
				}
				
				for(File x : fo.listFiles(builderFileFilter)) {
					if(x.isFile()) filesObjs.add(x)
				}
			
			} else {
				error "Path is not a file nor a directory: ${fo.absolutePath}"
				return
			}
				
		}
		
		println "Input files [${filesObjs.size()}]"
		
		if(filesObjs.size() < 1) {
			error "No input files!"
		}
			
		for(File f : filesObjs) {
			println f.getAbsolutePath()
		}	
		
		println ""
		
		
		if(profile != null) {
			println "Setting custom vital service profile: ${profile}"
		} else {
			profile = VitalServiceFactory.DEFAULT_PROFILE
			println "Using default vital service profile... ${profile}"
		}
		
		VitalService service = VitalServiceFactory.openService(serviceKey, profile)
		println "Obtained vital service, type: ${service.getEndpointType()}"
		
		VitalSegment segmentObj = service.getSegment(segment)
		
		if(!segmentObj) {
			error "Segment not found: ${segment}"
			return
		}
		
		boolean bigFiles = false;
		
		
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
		
		
		
		if(!check && bigFiles) {
			
			println "Big files mode - uplading files to prime first ..."
			
			String filesPaths = ""
			
			for(int i = 0 ; i < filesObjs.size(); i++) {
				
				File f = filesObjs[i]
				
				VITAL_Node node = VitalFtpCommand.getFile(service, f.name)
				
				if(node != null && node.active) {
					error ("Remote file: ${f.name} is in use")
				}
				
			}
			
			for(int i = 0 ; i < filesObjs.size(); i++) {
				
				File f = filesObjs[i]
				
				println "Uploading file ${i+1} of ${filesObjs.size()} -> ${f.absolutePath}"
				
				BufferedInputStream bis = null
				try {
					bis = new BufferedInputStream(new FileInputStream(f))
					
					VitalStatus status = service.uploadFile(URIProperty.withString(VitalFtpCommand.FTP_URI), f.name, bis, true)
					
					if(status.status != VitalStatus.Status.ok) {
						error ("Error status: ${status}")
					}
					
					if(filesPaths.length() > 0) filesPaths += "\t"
					filesPaths += (/*VitalFtpCommand.FTP_URI + ':' + */ f.name)
					
				} catch(Exception e) {
					error e.localizedMessage
				} finally {
					IOUtils.closeQuietly(bis)
				}
				
				
			}
			
			println "All files uploaded, scheduling files import now..."

			ResultList rl = null
			 
			try {
				
				rl = service.callFunction("commons/scripts/RunJob.groovy", [function: 'commons/scripts/ImportTempFilesData.groovy', filesPaths: filesPaths, blocksPerBatch: blocksPerBatch, segment: segment])
			} catch(Exception e) {
				error e.localizedMessage
			}		
			
			if(rl.status.status == VitalStatus.Status.ok) {
				
				println "Import job started - ID: ${rl.status.message}"
				
			} else {
			
				error "${rl.status}"
			
			}
			
			
			
		} else {
		
			if( ! skipCheck ) {
				
				println "Checking the data ..."
				
				for(int i = 0 ; i < filesObjs.size(); i++) {
					File f = filesObjs[i]
					
					long totalLength = 0
					
					
					String uncompressedLengthString = ""
					if(f.name.endsWith('.gz')) {
						totalLength = getGZIPUncompressedLengthUnreliable(f)
						uncompressedLengthString = " / uncompressed length: " + humanReadableByteCount(totalLength)
					} else {
						totalLength = f.length()
					}
					
					println "Checking file ${i+1} of ${filesObjs.size()}: ${f.absolutePath}, length: ${humanReadableByteCount(f.length())} ${uncompressedLengthString}"
					
					if(blockFileNameFilter.accept(f)) {
						
						BlockIterator iterator = null
						
						int c = 0
						
						try {
							
							iterator = BlockCompactStringSerializer.getBlocksIterator(openReader(f, verbose ? new UpdateProgressListener(totalLength) : null))
						
							for(VitalBlock block : iterator) {
								c++
							}
							
						} catch(Exception e) {
							error(e.localizedMessage)
						} finally {
							if(iterator != null) try {iterator.close()}catch(Exception e){}
						}
	
						println "${c} blocks"				
							
					} else if(ntripleFileFilter.accept(f)){
					
						VitalNTripleIterator iterator = null
						
						int c = 0
						
						try {
							
							iterator = new VitalNTripleIterator(f)
							
							for(GraphObject g : iterator) {
								c++
							}
							
						} catch(Exception e) {
							error(e.localizedMessage)
						} finally {
							IOUtils.closeQuietly(iterator)	
						}
					
						println "${c} graph objects"
							
					} else if(builderFileFilter.accept(f)) {
					
						def builder = new VitalBuilder()
					
						println "Parsing builder file..."
						String t = FileUtils.readFileToString(f, "UTF-8")
						List<VitalBlock> blocks = builder.queryString(t).toBlock()
						
						println "Blocks count: ${blocks.size()}"
					
					} else {
						error("unhandled file: ${f.absolutePath}")
					}
					
				}
			} else {
			
				println "Data check skipped"
				
			}
			
			if(check) {
				println "Data check complete - no errors"
				return 
			}
			
			if(bulkMode) {
				
				long total = t()
				
				println "Importing in bulk mode ... (without existing objects check)"
				
				for(int i = 0 ; i < filesObjs.size(); i++) {

					File f = filesObjs[i]
				
					if( ! blockFileNameFilter.accept(f) ) throw new Exception("Only block files may be imported in bulk mode")
				}
				
				for(int i = 0 ; i < filesObjs.size(); i++) {
					
					long s = t();
					
					File f = filesObjs[i]
					
					long totalLength = 0
					
					
					String uncompressedLengthString = ""
					if(f.name.endsWith('.gz')) {
						totalLength = getGZIPUncompressedLengthUnreliable(f)
						uncompressedLengthString = " / uncompressed length: " + humanReadableByteCount(totalLength)
					} else {
						totalLength = f.length()
					}
						
					println "Importing file ${i+1} of ${filesObjs.size()}: ${f.absolutePath}, file length: ${ humanReadableByteCount(f.length() ) } ${uncompressedLengthString}"
					
					InputStream inputStream = null;
					
					try {
						
						inputStream = new FileInputStream(f)
						
						if(f.name.endsWith(".gz")) {
							inputStream = new GZIPInputStream(inputStream)
						}
						
						inputStream = new BufferedInputStream(inputStream)
						
						
						if(verbose) {
							ProgressInputStream pis = new ProgressInputStream(inputStream)
							pis.addPropertyChangeListener(new UpdateProgressListener(totalLength))
							inputStream = pis
							
						}
						
						service.bulkImport(segmentObj, inputStream)
						
						println "Bulk import complete, time: ${t() - s}ms"
															
						
					} finally {
					
						IOUtils.closeQuietly(inputStream)
					
					}
					
				}
				
				if(filesObjs.size() > 1) {
					
					println "Total time: ${t() - total}ms"
					
				}
				
			} else {
			
				println "Importing in normal mode..."
				
				for(int i = 0 ; i < filesObjs.size(); i++) {

					File f = filesObjs[i]

					println "Importing file ${i+1} of ${filesObjs.size()}: ${f.absolutePath}"

					long s = t();

					List<VitalBlock> blocks = []
					
					int c = 0

					long stage = t()

					if(blockFileNameFilter.accept(f)) {

						BlockIterator iterator = null

						try {

							iterator = BlockCompactStringSerializer.getBlocksIterator(f)

							for(VitalBlock block : iterator) {
								if(blocks.size() >= blocksPerBatch) {

									//insert
									persist(segmentObj, blocks, service)

									if(verbose) {
										println"imported ${c} blocks (last batch ${t() - stage}ms) ..."
									}

									blocks.clear()

									stage = t()

								}

								blocks.add(block)

								c++
							}

						} finally {
							if(iterator != null) try {iterator.close()}catch(Exception e){}
						}

					} else if(ntripleFileFilter.accept(f)){

						VitalNTripleIterator iterator = null

						try {

							iterator = new VitalNTripleIterator(f)

							for(GraphObject g : iterator) {

								if(blocks.size() >= blocksPerBatch) {

									//insert
									persist(segmentObj, blocks, service)

									if(verbose) {
										println"imported ${c} blocks (last batch ${t() - stage}ms) ..."
									}

									blocks.clear()

									stage = t()

								}

								VitalBlock block = new VitalBlock()
								block.mainObject = g

								blocks.add(block)

								c++

							}

						} finally {
							IOUtils.closeQuietly(iterator)
						}
						
					} else if(builderFileFilter.accept(f)) {

						def builder = new VitalBuilder()
					
						println "Parsing builder file..."
						String t = FileUtils.readFileToString(f, "UTF-8")
						blocks = builder.queryString(t).toBlock()
						
						println "Blocks count: ${blocks.size()}"
						
					} else {
						error("unhandled file: ${f.absolutePath}")
					}


					if(blocks.size() > 0) {
						persist(segmentObj, blocks, service)
					}

					long stop = t()

					println "File ${i+1} ${f.absolutePath}: total ${c} blocks, ${stop-s}ms"


				}
			}

				
		}	
			
	}
	
	static persist(VitalSegment segment, List<VitalBlock> l, VitalService service) {
	
		List<GraphObject> gos = []
		
		for(VitalBlock b : l) {
			gos.add(b.mainObject)
			gos.addAll(b.dependentObjects)
		}
		
		if(gos.size() > 0) {
			service.save(segment, gos, true)
		}
			
	}
	
	//refresh every 100 miliseconds ? 
	static class UpdateProgressListener implements PropertyChangeListener {

		final long total
		
		DecimalFormat df = new DecimalFormat('0.0')
		
		String lastString = null;
		
		long lastUpdateTime = 0L
		
		public UpdateProgressListener(long totalLength) {
			this.total = totalLength
		}
		
		@Override
		public void propertyChange(PropertyChangeEvent evt) {

			long t = System.currentTimeMillis()
			
			long delta = Math.abs( t - lastUpdateTime )
			
			
			long readB = ((Long)evt.getNewValue()).longValue();
			
			boolean done = readB == total
			
			if(delta > 1000L || lastString == null || done) {
				
				double fraction = ((double)readB / (double)total)			
				
				String newString = df.format( fraction * 100d) + "%"
				
				if(lastString == null || !lastString.equals(newString) ) {
					
					
//					if(lastString != null) {
//						String b = ""
//						for(int i = 0; i < lastString.length() ; i++) {
//							b += "\b"
//						}
//						print b
//					}
//					
					String out = newString
					
					if(lastUpdateTime > 0L) {
						out += " delta ${t - lastUpdateTime}ms"
					}
					
					println out
					

					System.out.flush()
					
					lastString = newString
										
					lastUpdateTime = t
					
				}
					
			}
						
//			if(done) {
//				print "\n"
//				System.out.flush()
//			}
			
		}
		
	}

}
