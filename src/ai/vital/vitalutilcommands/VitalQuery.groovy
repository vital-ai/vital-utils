package ai.vital.vitalutilcommands

import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.factory.Factory;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalGraphQuery;
import ai.vital.vitalservice.query.VitalSelectQuery
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.utils.BlockCompactStringSerializer;

import org.apache.commons.io.FileUtils;

/**
 * A script that queries vital service and persists results
 * @author Derek
 *
 */
class VitalQuery extends AbstractUtil {
	
	public static void main(String[] args) {
		
		def cli = new CliBuilder(usage: 'vitalquery [options]')
		cli.with {
			h longOpt: "help", "Show usage information", args: 0, required: false
			o longOpt: "output", "output block file, it prints to console otherwise", args:1, required: false
			ow longOpt: "overwrite", "overwrite output file", args: 0, required: false
			q longOpt: "query", "qurery file", args: 1, required: true
		}
		
		def options = cli.parse(args)
		
		if(!options || options.h) return
		
		File queryScriptFile = new File(options.q);
		
		
		
		if(!queryScriptFile.isFile()) {
			error "Query script file path does not exist or is not a file"
			return
		}
		
		File output = options.o ? new File(options.o) : null
				
		if(output != null) {
			
			if(!blockFileNameFilter.accept(output)) {
				error("Output file name must end with .vital[.gz]: ${output.absolutePath}")
				return
			}
			
			Boolean overwrite = Boolean.TRUE.equals(options.ow)
			
			println ("Output file: ${output.absolutePath}")
			println ("Overwrite ? " + overwrite)
		
			if(output.exists() && !overwrite) {
				error("Output file already exists - use '-ow' option to overwrite - ${output.absolutePath}")
				return
			}
			 
		}
		
		println "Query script file: ${queryScriptFile.absolutePath}"
		
		String queryScript = FileUtils.readFileToString(queryScriptFile, "UTF-8")
		
		queryScript =
		"import ai.vital.vitalservice.query.*;\n" +
		"import ai.vital.vitalservice.query.VitalQueryContainer.Type;\n" +
		queryScript;
		
		
		GroovyShell shell = new GroovyShell();
		Object _query = shell.evaluate(queryScript);

		if(!(_query instanceof VitalSelectQuery) && !(_query instanceof VitalGraphQuery)) throw new Exception("A script must return a select or a graph query variable.");
		
		
		VitalService service = Factory.getVitalService()
		println "Obtained vital service, type: ${service.class.canonicalName}"
		
		ResultList rl = null;
		
		if(_query instanceof VitalSelectQuery) {
			
			rl = service.selectQuery(_query)
			
		} else if(_query instanceof VitalGraphQuery) {
		
			rl = service.graphQuery(_query)
		
		}
		
		OutputStream os = null
		BlockCompactStringSerializer writer = null
		OutputStreamWriter osw = null
		if(output != null) {
			
			writer = new BlockCompactStringSerializer(output);
			 
		} else {
		
			osw = new OutputStreamWriter(System.out)
		
			writer = new BlockCompactStringSerializer(osw)
		
		}
		
		println ("Results: ${rl.results.size()}, total: ${rl.totalResults}")
		
		boolean started = false
		
		for(ResultElement r : rl.results) {
			
			if(!started) {
				writer.startBlock()
				started = true
			}	
			
			writer.writeGraphObject(r.graphObject)
			
		}
		
		if(started) {
			writer.endBlock()
		}
		
		if(output != null) {
			writer.close()
		}
		
		if(osw != null) {
			osw.flush()
		}
	}
	

}
