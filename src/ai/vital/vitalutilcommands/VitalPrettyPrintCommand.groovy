package ai.vital.vitalutilcommands

import groovy.json.JsonOutput;
import ai.vital.vitalservice.json.VitalServiceJSONMapper;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;
import ai.vital.vitalsigns.model.GraphObject;

class VitalPrettyPrintCommand extends AbstractUtil {
	
	static def BREAK_LINE = "--------------------"
	
	static String compact = 'compact'
	
	static String json = 'json'
	
	static String rdf = 'rdf'
	
	static def main(args) {
		
		def cli = new CliBuilder(usage: 'vitalprettyprint [options]', stopAtNonOption: false)
		cli.with {
			i longOpt: 'input', "input block file", args: 1, required: true
			s longOpt: 'style', "output style, one of: '${compact}' (default), '${rdf}, '${json}'", args: 1, required: false
			dbl longOpt: 'disable-breakline', 'disable break line which is enabled by default', args: 0, required: false
			l longOpt: 'limit', 'print max n blocks', args: 1, required: false
			off longOpt: 'offset', 'offset (skip) n blocks', args: 1, required: false
		}
	
		if(args.length == 0) {
			cli.usage()
			return
		}
		
		
		def options = cli.parse(args)
		
		if(!options) {
			return
		}
		
		String style = compact
		
		Integer limit = options.l ? Integer.parseInt(options.l) : 0 
		
		if(limit < 0) {
			error("Limit must be >= 0")
			return
		}
		
		Integer offset = options.off ? Integer.parseInt(options.off) : 0
		if(offset < 0) {
			error("offset must be >= 0")
			return
		} 
		
		if(options.s) {
			style = options.s
			if(compact.equalsIgnoreCase(style) || rdf.equalsIgnoreCase(style) || json.equalsIgnoreCase(style)) {
			} else {
				error("Invalid style, supported styles: ${compact}, ${rdf}, ${json}")
				return
			}
		}
			
		File inputFile = new File(options.i)
		
		if(!inputFile.exists()) {
			error("File not found: ${inputFile.absolutePath}")
			return
		}

		boolean disableBreakLine = options.dbl ? true : false		
		
		boolean first = true
		
		int c = 0
		
		int printed = 0
		
		BlockIterator iterator = BlockCompactStringSerializer.getBlocksIterator(inputFile)
		
		for( ; iterator.hasNext(); c++ ) {
			
			VitalBlock block = iterator.next()
			
			if(limit > 0 && limit <= printed) {
				//limit reached
				break
			}
			
			
			if(c < offset) {
				continue
			}
			
			printed++
			
			
			if(!first && !disableBreakLine) {
				
				o(BREAK_LINE)
				
			}
			
			first = false
			
			for(GraphObject g : block.toList()) {
				
				if(compact.equalsIgnoreCase(style)) {
				
					String s = g.toCompactString();
					
					String[] cols = s.split("\\t")
					
					o(cols[1])
					
					for(int i = 0 ; i < cols.length; i ++) {
						
						if(i == 1) continue
						
						o("\t" + cols[i])
						
					}
						
				} else if(rdf.equalsIgnoreCase(style)) {
				
					String ntripleString = g.toRDF()
				
					o(ntripleString)
					
				} else if(json.equalsIgnoreCase(style)) {
				
					o(JsonOutput.prettyPrint(g.toJSON()))
				
				}
				
			}
			
		}
		
		iterator.close()
		
		
		
		
	}

}
