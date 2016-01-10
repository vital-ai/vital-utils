package ai.vital.vitalutilcommands

import java.nio.file.Path
import org.apache.commons.io.FileUtils;
import org.asciidoctor.*

class VitalAsciiDoc extends AbstractUtil {

	static def main(args) {
		
		
		def cli = new CliBuilder(usage: 'vitalasciidoc [options]', stopAtNonOption: false)
		cli.with {
			h longOpt: 'help', 'display usage', args: 0, required: false
			i longOpt: 'input', "asciidoc input file/directory", args: 1, required: true
			o longOpt: 'output', "output html file/directory", args: 1, required: true
			ow longOpt: 'overwrite', "overwrite output file/directory if exists", args: 0, required: false
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
		
		File inputFile = new File( options.i )
		File outputFile = new File( options.o )
		
		Boolean overwrite = options.ow ? true : false
		
		println "input file: ${inputFile.absolutePath}"
		println "output file: ${outputFile.absolutePath}"
		println "overwrite output ? $overwrite"
		
		if(!inputFile.exists()) error("input file/directory not found: ${inputFile.absolutePath}")
		
		if(outputFile.exists()) {
			
			if(!overwrite) error("output location already exists, cannot convert: ${outputFile.absolutePath}")
			
			println "Deleting existing documentation: ${outputFile.absoluteFile}"
			
			FileUtils.deleteQuietly(outputFile)
			
		}
		
		def asciidoctor = Asciidoctor.Factory.create()

		
		if(inputFile.isFile()) {
			
			def opts = OptionsBuilder.options().toFile(outputFile).get()
			
			println "File -> file mode"
			
			asciidoctor.renderFile(inputFile, opts)
			
		} else {
		
			outputFile.mkdirs()	
		
			println "Directory -> directory mode"
			
			Collection<File> files = FileUtils.listFiles(inputFile, null, true)
			
			List<File> inputFiles = []
			
			for(File f : files) {

				if(!f.isFile()) continue
				inputFiles.add(f)
				
			}
			
			println "Input files count: ${inputFiles.size()}"
				
			int i = 0;				
			
			for(File f : inputFiles) {
				
				i++
				
				println "Processing file $i of ${inputFiles.size()} - ${f.absolutePath}"
				
				Path relativePath = inputFile.toPath().relativize(f.toPath())
				
				
				File outFile = outputFile.toPath().resolve(relativePath).toFile()
				
				File outParent = outFile.getParentFile();
				
				outParent.mkdirs()
				
				String n = outFile.name
				
				int lastDot = n.lastIndexOf('.')
				
				if(lastDot > 0 && lastDot < n.length() - 1) {
					n = n.substring(0, lastDot)
				}
				
				outFile = new File(outParent, n + ".html")
				
				def opts = OptionsBuilder.options().get()
				
				String out = asciidoctor.renderFile(f, opts)
				
				FileUtils.writeStringToFile(outFile, out, "UTF-8")
				
			}
			
		
		}
		
		println "DONE"		
				
	}
	
}
