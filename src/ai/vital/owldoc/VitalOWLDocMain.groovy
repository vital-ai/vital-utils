package ai.vital.owldoc

import org.apache.commons.io.FileUtils;

/**
 * Main script for vitalowldoc 
 * @author Derek
 *
 */
class VitalOWLDocMain {

	static main(args) {
	
		def cli = new CliBuilder(usage: 'vitalowldoc [options]')
		cli.with {
			h longOpt: "help", "Show usage information", args: 0, required: false
			d longOpt: "directory", "output directory", args:1, required: true
			ow longOpt: "overwrite", "overwrite output directory if exists", args: 0, required: false
			o longOpt: "owl", "input owl file", args: 1, required: true
		}
		
		def options = cli.parse(args)
		
		if(!options || options.h) return
	
		File directory = new File(options.d)
		
		println "Output directory: ${directory.absolutePath}"
		
		boolean overwrite = options.ow ? true : false
		
		println "Overwrite output directory ? ${overwrite}"
		
		File owlFile = new File(options.o)
		
		println "OWL file path: ${owlFile.absolutePath}"
		
		if(!owlFile.exists()) {
			err "OWL file does not exist: ${owlFile.absolutePath}"
		}
		
		if(!owlFile.isFile()) {
			err "OWL file path exists but is not a file: ${owlFile.absolutePath}"
		}
		
		if(directory.exists()) {
			
			if(!overwrite) {
				err "Output directory already exists, use --overwrite option: ${directory.absolutePath}"
			}
			
			if(!directory.isDirectory()) {
				err "Output path exists but is not a directory: ${directory.absolutePath}"
			}
			
			FileUtils.deleteDirectory(directory)
			
		}
		
		if(!directory.mkdirs()) {
			err "Couldn't create output directory: ${directory.absolutePath}"
		}
		
		
		OWLDocImpl.generateOWLDoc(directory, owlFile)
		
		println "DONE"
		
		
	}
	
	static void err(String err) {
		System.err.println err
		System.exit(1)
	}

}
