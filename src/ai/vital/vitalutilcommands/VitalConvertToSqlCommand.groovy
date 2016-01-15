package ai.vital.vitalutilcommands

import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream

import com.typesafe.config.ConfigException.BugOrBroken

import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock
import ai.vital.vitalsigns.csv.ToCSVHandler
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.sql.ToSQLRowsHandler;;;;;

class VitalConvertToSqlCommand extends AbstractUtil {

	def static main(args) {
		
		def cli = new CliBuilder(usage: 'vitalconverttosql [options]', stopAtNonOption: false)
		cli.with {
			h longOpt: 'help', 'display usage', args: 0, required: false
			i longOpt: 'input', "input block .vital[.gz]", args: 1, required: true
			o longOpt: 'output', "output sql file, .csv[.gz] or .sql[.gz]", args: 1, required: true
			ow longOpt: 'overwrite', "overwrite output file if exists", args: 0, required: false
			soh longOpt: 'skip-output-header', "skip output csv header (block -> csv case)", args: 0, required: false
			tn longOpt: 'table-name', "required when .sql[.gz] output", args: 1, required: false
			bs longOpt: 'batch-size', "batch size, required when .sql.gz output", args: 1, required: false
		}
		
		boolean displayHelp = args.length == 0
		
		for(String arg : args) {
			if(arg == '-h' || arg == '--help') {
				displayHelp = true
			}
		}
		
		if(displayHelp == true) {
			cli.usage()
			return
		}
		
		def options = cli.parse(args)
		
		if(!options || options.h) {
			return
		}
				
		File inputFile = new File(options.i)
		File outputFile = new File(options.o)
		boolean overwrite = options.ow ? true : false
		boolean skipOutputHeader = options.soh ? true : false
		String tableName = options.tn ? options.tn : null
		
		Integer batchSize = options.bs ? Integer.parseInt(options.bs) : null
		
		
		println "Input file: ${inputFile.absolutePath}"
		
		if(!inputFile.isFile()) error("Input file ${inputFile.absolutePath} does not exist or is not a file")
		
		String iname = inputFile.name
		
		
		
		println "Output file: ${outputFile.absolutePath}"
		
		String oname = outputFile.name
		
		
		boolean inputBlock = false
//		boolean inputCSV = false
//		boolean inputNT = false
		
		boolean outputCSV = false
		boolean outputSQL = false
		
		if(iname.endsWith('.vital.gz') || iname.endsWith('.vital')) {
			inputBlock = false
		} else {
			error "input file must be a block file"
		}
		
		
		if(oname.endsWith('.csv.gz') || oname.endsWith('.csv')) {
			outputCSV = true
			if(tableName) {
				println "Ignoring --table-name param - CSV mode"
			}
			
			if(batchSize != null) {
				println "Ignoring --batch-size param - CSV mode"
			}
			
			println "Skip output header ? ${skipOutputHeader}"
			
		} else if(oname.endsWith('.sql.gz') || oname.endsWith('.sql')) {
			outputSQL = true
			
			if(!tableName) {
				error "No --table-name param, required with .sql output"
			}
			
			if(batchSize == null) {
				error("no --batch-size param, required with .sql output")
			}
			
			if(batchSize < 1) {
				error("batch-size must be > 0")
			}
			
			println "Output table name: ${tableName}"
			println "Batch size: ${batchSize}"
			
			if(skipOutputHeader) println "Ignoring --skip-output-header param - SQL mode"
			
		} else {
			error("Output file name extension invalid, expected .sql[.gz] or .csv[.gz] - ${oname}")
		}
		
		
		println "Overwrite: ${overwrite}"
		
		if(!inputFile.isFile()) error("Input file ${inputFile.absolutePath} does not exist or is not a file")
		
		if(outputFile.exists()) {
			
			if(!overwrite) error("Output file ${outputFile.absolutePath} already exists, but overwrite flag is not set")
			
			println "Output file will be overwritten: ${outputFile.absolutePath}"
			
		}
		
		OutputStream out = new FileOutputStream(outputFile);
		if(oname.endsWith(".gz")) {
			out = new GZIPOutputStream(out)
		}
		
		
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
		
		if(outputCSV && !skipOutputHeader) {
			writer.write(ToCSVHandler.getHeaders())
			writer.write("\n")
		}
		
		String insertInto = null;
		
		if(outputSQL) {
		
			List<String> columns = ToSQLRowsHandler.getColumns();
			
			StringBuilder sb = new StringBuilder("INSERT INTO `${tableName}`( ")
			for(int i = 0 ; i < columns.size(); i++) {
				if(i > 0) sb.append(", ")
				sb.append(columns.get(i))
			}
			
			sb.append(") VALUES \n")
			
			insertInto = sb.toString();
				
		}	

		int i = 0
		int r = 0
		
		VitalSigns.get()		
		
		long start = System.currentTimeMillis()
		
		int batchCounter = 0;
		
		for( BlockIterator iterator = BlockCompactStringSerializer.getBlocksIterator(inputFile); iterator.hasNext(); ) {
				
			VitalBlock block = iterator.next();
			
			for(GraphObject g : block.toList()) {
				
				i++
				
				if(outputSQL) {
					
					for( String s : g.toSQLRows() ) {
				
						if(batchCounter == 0) {
							
							writer.write(insertInto)
							
						} else {
						
							writer.write(",\n");	
						
						}
							
						batchCounter++;	
						r++
						
						writer.write(s)
						
						if(batchCounter % batchSize == 0) {
							//close the batch
							writer.append(";\n")
							batchCounter = 0
						}
						
						
					}
					
				}
				
				if(outputCSV) {
					
					for(String s : g.toCSV(false)) {
						
						r++
						writer.write(s)
						writer.write("\n")
						
					}
				}
				
			}	
			
		}
		
		if(batchCounter > 0) {
			writer.append(";\n")
		}
		
		if(outputSQL) {
			writer.append(";\n")
		}
		
		writer.close()
		
		println "Total time: ${System.currentTimeMillis() - start}ms"
		println "Graph objects iterated: ${i}"
		println "Output records count: ${r}"
		 

	}
	
}
