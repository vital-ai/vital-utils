package ai.vital.vitalutilcommands

import org.apache.commons.io.IOUtils;

import ai.vital.endpoint.EndpointType;
import ai.vital.vitalsigns.datatype.VitalURI;
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.model.VITAL_Node;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.vitalsigns.rdf.VitalNTripleIterator
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.factory.Factory
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.segment.VitalSegment;

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
			f longOpt: "file", "input file or directory, supported extensions .vital[.gz], .nt[.gz]", args:1, required: true
			s longOpt: "segment", "target segment", args: 1, required: true
			b longOpt: "batch", "blocks per batch, default: ${DEFAULT_BLOCKS}", args: 1, required: false
			v longOpt: "verbose", "report import progress (only in non-big-files mode)", args: 0, required: false
			c longOpt: "check", "check input files - DOES NOT IMPORT", args: 0, required: false
			bf longOpt: "bigFiles", "[true|false] flag, force big files flag (only vitalprime), default true", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			//bf longOpt: 'bigfiles', "the flag to import the data in background, after they're uploaded", args: 0 , required: false
		}
		
		def options = cli.parse(args)

		if(!options || options.h) return
		
		List files = options.fs
		
		Integer blocksPerBatch = DEFAULT_BLOCKS
		
		if(options.b) {
			blocksPerBatch = Integer.parseInt(options.b)
		}
		
		String segment = options.s
		
		println "Segment: ${segment}"
		println "Input paths [${files.size()}]: ${files}"
		println "Blocks per batch: ${blocksPerBatch}"
		boolean verbose = options.v ? true : false
		println "Verbose: ${verbose}"
		boolean check = options.c ? true : false
		println "Check data only ? ${check}"

		List<File> filesObjs = []
		
		for(String f : files) {
		
			File fo = new File(f);
			
			if(!fo.exists()) {
				error "Path does not exist: ${fo.absolutePath}"
				return
			}
			
			if(fo.isFile()) {
				
				if(!blockFileNameFilter.accept(fo) && !ntripleFileFilter.accept(fo)) {
					error("Cannot accept file: ${fo.absolutePath} - name must end with .vital[.gz] or .nt[.gz]")
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
		
		
		String profile = options.prof ? options.prof : null
		if(profile != null) {
			println "Setting custom vital service profile: ${profile}"
			Factory.setServiceProfile(profile)
		} else {
			println "Using default vital service profile..."
		}
		
		VitalService service = Factory.getVitalService()
		println "Obtained vital service, type: ${service.getEndpointType()}"
		
		VitalSegment segmentObj = null;
		for(VitalSegment s : service.listSegments()) {
			if(s.id== segment) {
				segmentObj = s
				break
			}
		}
		
		if(!segmentObj) {
			error "Segment not found: ${segment}"
			return
		}
		
		Boolean bigFilesForced = options.bf ? Boolean.parseBoolean(options.bf) : null
		
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
					
					VitalStatus status = service.uploadFile(VitalURI.withString(VitalFtpCommand.FTP_URI), f.name, bis, true)
					
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
		
			println "Checking the data ..."
			
			for(int i = 0 ; i < filesObjs.size(); i++) {
				File f = filesObjs[i]
				
				println "Checking file ${i+1} of ${filesObjs.size()}: ${f.absolutePath}"
				
				if(blockFileNameFilter.accept(f)) {
					
					BlockIterator iterator = null
					
					int c = 0
					
					try {
						
						iterator = BlockCompactStringSerializer.getBlocksIterator(f)
					
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
						
				
				} else {
					error("unhandled file: ${f.absolutePath}")
				}
				
			}
			
			
			if(check) {
				println "Data check complete - no errors"
				return 
			}
			
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
	
	static persist(VitalSegment segment, List<VitalBlock> l, VitalService service) {
	
		List<GraphObject> gos = []
		
		for(VitalBlock b : l) {
			gos.add(b.mainObject)
			gos.addAll(b.dependentObjects)
		}
		
		if(gos.size() > 0) {
			service.save(segment, gos)
		}
			
	}

}
