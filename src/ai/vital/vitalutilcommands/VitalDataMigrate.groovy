package ai.vital.vitalutilcommands

import org.apache.commons.io.FileUtils;

import ai.vital.vitalservice.BaseDowngradeUpgradeOptions;
import ai.vital.vitalservice.ServiceOperations;
import ai.vital.vitalservice.VitalStatus;
import ai.vital.vitalservice.impl.UpgradeDowngradeProcedure;

class VitalDataMigrate extends AbstractUtil {

	def static CMD = 'vitaldatamigrate'
	
	def static main(args) {
		
		String vitalHome = System.getenv('VITAL_HOME')
		if(!vitalHome) { error("VITAL_HOME not set!") }
		
		def cli = new CliBuilder(usage: CMD + ' [options]')
		cli.with {
			h longOpt: 'help', 'display usage', args: 0, required: false
			i longOpt: "input", "overrides path in a builder, .vital[.gz] file", args: 1, required: false
			o longOpt: "output", "overrides path in a builder, .vital[.gz] file", args: 1, required: false
			ow longOpt: "overwrite", "overwrite output file if exists", args:0, required: false
			b longOpt: "builder", "builder file, .groovy or .builder extension", args: 1, required: false
			owlfile longOpt: "owl-file", "overrides name in builder, older owl file path option", args: 1, required: false
			d longOpt: "direction", "[upgrade, dowgrade], required when no builder file provided", args:1, required: false
		}

		if(args.length < 1) {
			cli.usage()
			return
		}		
		
		def options = cli.parse(args)
		
		if(!options) {
			return
		}
		
		if( options.h ) {
			cli.usage()
			return
		}	
		
		File builderFile = options.b ? new File(options.b) : null
		
		File inputFile = options.i ? new File(options.i) : null
		File outputFile = options.o ? new File(options.o) : null
		
		String owlfile = options.owlfile ? options.owlfile : null
		
		boolean overwrite = options.ow ? true : false 
		
		println "builder file: ${builderFile?.absolutePath}"
		println "input file: ${inputFile?.absolutePath}"
		println "output file: ${outputFile?.absolutePath}"
		
		String direction = options.d ? options.d : null;
		
		ServiceOperations ops = null;
		
		if(builderFile) {
		
			println "using builder file"
		
			if(owlfile) println ("ignoring owlfile")
			if(direction) println ("ignoring direction param")
			
			if(!builderFileFilter.accept(builderFile)) {
				error("builder file name invalid: ${builderFile.absolutePath}, must be a .groovy or .builder file")
				return
			}
			
			if(!builderFile.isFile()) {
				error("builder file does not exist or not a file: ${builderFile.absolutePath}")
				return
			}
			
				
			ops = UpgradeDowngradeProcedure.parseUpgradeDowngradeBuilder(FileUtils.readFileToString(builderFile, "UTF-8"))
			
			BaseDowngradeUpgradeOptions opts = null;
			
			if( ops.getDowngradeOptions() ) {
				opts = ops.getDowngradeOptions()
			} else if(ops.getUpgradeOptions()) {
				opts = ops.getUpgradeOptions()
			} else {
				error("No UPGRADE/DOWNGRADE options")
				return;
			}
			
			if(opts.destinationSegment || opts.sourceSegment) {
				error("$CMD does not accept source/destination segment")
				return
			}
			
			if(inputFile) {
				println "overriding builder source path: ${inputFile.absolutePath}"
				opts.setSourcePath(inputFile.toURI().toString())
			} else {
				if(!opts.getSourcePath()) {
					error("no source path in builder nor cli param")
					return
				}
				inputFile = new File(URI.create(opts.getSourcePath()))
			}
			
			if(outputFile) {
				println "overriding builder destination path: ${outputFile.absolutePath}"
				opts.setDestinationPath(outputFile.toURI().toString())
			} else {
				if(!opts.getDestinationPath()) {
					error("no destination path in builder nor cli param")
					return
				}
				outputFile = new File(URI.create(opts.getDestinationPath()))
			}

		} else {
		
			println "no builder file, on-the-fly migration"

			if(inputFile == null) {
				error("no input file, required in builderless mode")
				return
			}
			
			if(outputFile == null) {
				error("no output file, required in builderless mode")
				return
			}
			
			if(!direction) {
				error("no direction set, required in builderless mode")
				return
			}
			
			if(!(direction == 'upgrade' || direction == 'downgrade')) {
				error("invalid direction: $direction, valid values: 'upgrade', 'downgrade'")
				return
			}
			
			if(!owlfile) {
				error("no owlfile set, required in builderless mode")
				return
			}
			
			String builderContent = """\

${direction.toUpperCase()} {

	value sourcePath: '${inputFile.toURI().toString()}'

	value destinationPath: '${outputFile.toURI().toString()}'

	value oldOntologyFileName: '${owlfile}'

}
"""

			ops = UpgradeDowngradeProcedure.parseUpgradeDowngradeBuilder(builderContent)
		
			
		}
		
		
		
		if(inputFile) {
			if(!blockFileNameFilter.accept(inputFile)) {
				error("input path must be a block file (.vital[.gz]): ${inputFile.absolutePath}")
				return
			}
			if(!inputFile.isFile()) {
				error("input file does not exist or not a file: ${inputFile.absolutePath}")
				return 
			}
		}
		
		if(outputFile) {
			if(!blockFileNameFilter.accept(outputFile)) {
				error("output path must be a block file (.vital[.gz]): ${outputFile.absolutePath}")
				return
			}
			
			if(outputFile.exists()) {
				if(!overwrite) {
					error("output file exists, use --overwrite option: ${outputFile.absolutePath}")
				}
				println ("output file will be overwritten: ${outputFile.absolutePath}")
			}
			
		}
		
		def procedure = new UpgradeDowngradeProcedure(null)
		
		VitalStatus status = procedure.execute(ops)
		
		println "" + status.status + " - " + status.message
		
	
	}
	
}
