package ai.vital.vitalutilcommands

import java.nio.charset.Charset;
import java.util.Map.Entry
import java.util.regex.Matcher
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import org.apache.commons.io.IOUtils;

import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.graph.Graph;
import ai.vital.vitalsigns.model.GraphObject;
import ai.vital.vitalsigns.model.PropertyInterface;
import ai.vital.vitalsigns.model.URIPropertyValue
import ai.vital.vitalsigns.utils.BlockCompactStringSerializer;
import ai.vital.vitalsigns.utils.BlockCompactStringSerializer.BlockIterator;
import ai.vital.vitalsigns.utils.BlockCompactStringSerializer.VitalBlock
import ai.vital.vitalsigns.utils.VitalNTripleIterator

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory;

class VitalConvertCommand extends AbstractUtil {

	static Pattern classPropPattern = Pattern.compile("^(.+)\\.([^.]+)\$")
	
	static Pattern classPropMappingPattern = Pattern.compile("^(.+)\\.(\\S+)\\s{0,}=\\s{0,}(\\S+)\$")
	
	static Pattern URIPropPattern = Pattern.compile("(^[^{}]{0,})(\\{(\\d+)\\})([^{}]{0,}\$)")
	
	static Pattern columnNumberPattern = Pattern.compile("^\\d+\$")
	
	public static void main(String[] args) {
		
		
		String vitalHome = System.getenv('VITAL_HOME')
		if(!vitalHome) { error("VITAL_HOME not set!") }
		
		File domainJarDir = new File(vitalHome, 'domain-groovy-jar')
		if(!domainJarDir.exists()) error "Domain jar \$VITAL_HOME/domain-groovy-jar directory does not exist: ${domainJarDir.absolutePath}"
		if(!domainJarDir.isDirectory()) error "Domain jar directory path \$VITAL_HOME/domain-groovy-jar exists but is not a directory: ${domainJarDir.absolutePath}"
		
		def cli = new CliBuilder(usage: 'vitalconvert [options]')
		cli.with {
			h longOpt: 'help', 'display usage', args: 0, required: false
			m longOpt: 'map', "map file, required with block -> csv conversion", args: 1, required: false
			i longOpt: 'input', "input block, n-triple or csv file, valid extensions .vital[.gz] .nt[.gz] .csv[.gz]", args: 1, required: true
			o longOpt: 'output', "output block, n-trilple or csv file, , valid extensions .vital[.gz] .nt[.gz] .csv[.gz]", args: 1, required: true
			ow longOpt: 'overwrite', "overwrite output file if exists", args: 0, required: false
			oh longOpt: 'outputHeader', "prepend csv header (block->csv case only)", args: 0, required: false
			sh longOpt: 'skipHeader', "skip input csv header (csv->block case only and map file specified)", args: 0, required: false
		}

		
		Boolean displayHelp = args.length == 0
		
		for(String arg : args) {
			if(arg == '-h' || arg == '--help') {
				displayHelp = true
			}
		}
		
		if(displayHelp == true) {
			println "displaying usage..."
			cli.usage()
			return
		}
		
		def options = cli.parse(args)
		
		if(!options) return

		displayHelp = options.h
		
		if(displayHelp != null && displayHelp == true) {
			println "displaying usage..."
			cli.usage()
			return
		}
				
		File inputFile = new File(options.i)
		
		println "Input file: ${inputFile.absolutePath}"
		
		if(!inputFile.isFile()) error("Input file ${inputFile.absolutePath} does not exist or is not a file")
		
		boolean inputBlock = false
		boolean inputCSV = false
		boolean inputNT = false
		boolean outputBlock = false
		boolean outputCSV = false
		boolean outputNT = false
		
		String iname = inputFile.name
		
		if(iname.endsWith('.vital.gz') || iname.endsWith('.vital')) {
			inputBlock = true
		} else if(iname.endsWith('.csv.gz') || iname.endsWith('.csv')) {
			inputCSV = true
		} else if(iname.endsWith('.nt.gz') || iname.endsWith('.nt')) {
			inputNT = true
		} else {
			error("Input file name extension invalid, expected .vital[.gz], .nt[.gz] or .csv[.gz] - ${iname}")
		}
		
		File outputFile = new File(options.o)
		
		println "Output file: ${outputFile.absolutePath}"
		
		String oname = outputFile.name
		
		if(oname.endsWith('.vital.gz') || oname.endsWith('.vital')) {
			outputBlock = true	
		} else if(oname.endsWith('.csv.gz') || oname.endsWith('.csv')) {
			outputCSV = true
		} else if(oname.endsWith('.nt.gz') || oname.endsWith('.nt')) {
			outputNT = true
		} else {
			error("Output file name extension invalid, expected .vital[.gz] or .csv[.gz] - ${oname}")
		}
		
		
		if(!( inputBlock || outputBlock)) {
			error("Either input or output file must be a block file")
		}  else if(inputBlock && outputBlock) {
			error("Either input or output file must be a block file - not both")
		}
		
		String inputType = inputBlock ? 'vital' : ( inputNT ? 'n-triples' : 'csv')
		String outputType = outputBlock ? 'vital' : ( outputNT ? 'n-triples' : 'csv')
		
		
		println "Conversion ${inputType} -> ${outputType}"
		
		
		boolean csvCase = inputCSV || outputCSV
		
		if(!csvCase) {
			
			if(options.m) error("Cannot use -m param in non-csv conversion")
			if(options.oh) error("Cannot use -oh param in non-csv conversion")
			if(options.sh) error("Cannot use -sh param in non-csv conversion")
			
		}
		

		Boolean overwrite = options.ow
		if(overwrite == null) overwrite = false
		
		println "Overwrite: ${overwrite}"
		
		if(!inputFile.isFile()) error("Input file ${inputFile.absolutePath} does not exist or is not a file")
		
		if(outputFile.exists()) {
			
			if(!overwrite) error("Output file ${outputFile.absolutePath} already exists, but overwrite flag is not set")
			
			println "Output file will be overwritten: ${outputFile.absolutePath}"
			
		}
		
		
		if(inputNT) {
			
			nt2Vital(inputFile, outputFile)
			return
			
		} else if(outputNT) {
		
			vital2NT(inputFile, outputFile)
			return
		
		}
		
		//the rest of it CSV!
		
		File mapFile = null;
		
		Map<String, Integer> propertyToColumn = new HashMap<String, Integer>()

		Map<String, PropertyInterface> propertyToInterface = new HashMap<String, Integer>()
				
		String mapURIPattern = null
		
		String URIPattern = null
		
		String URIPrefix = null
		String URISuffix = null
		
		Integer URIColumn = null
		
		String clazz = null
		
		Class clazzObj = null
		
		if(options.m) {
			
			mapFile = new File(options.m)
			println "Map file: ${mapFile.absolutePath}"
			if(!mapFile.isFile()) error("Map file does not exist or is not a file: ${mapFile.absolutePath}")
			
			BufferedReader reader = openReader(mapFile)
			
			int c = 0
			
			
			for(String l = reader.readLine(); l != null; l = reader.readLine()) {
				
				c++
				
				l = l.trim()
				
				if(l.isEmpty()) continue
				
				Matcher m1 = classPropMappingPattern.matcher(l)
				
				if(!m1.matches()) error("Map file line: ${c} - line does not match pattern: ${classPropMappingPattern.pattern()} - ${l}")
				
				String cls = m1.group(1)
				String property = m1.group(2)
				String mapping = m1.group(3)
				
				if(clazz != null && clazz != cls) error("Map file line: ${c} - class does not match the value in previous record ${clazz} - ${l}")
				
				clazz = cls
				
				if(clazzObj == null) {
					
					try {
												
						clazzObj = Class.forName(clazz)
												
						if( ! GraphObject.class.isAssignableFrom(clazzObj) ) error("Class is not a sublcass of ${GraphObject.class.canonicalName}")
												
					} catch(Exception e) {
						error(e.localizedMessage)
					}
																	
				}
				
				Integer col = null
				
				if(property == 'URI') {
					
					Matcher m = URIPropPattern.matcher(mapping)
					if(m.matches()) {
						
						URIPattern = mapping
						URIColumn = Integer.parseInt(m.group(3))
						URIPrefix = m.group(1)
						URISuffix = m.group(4)
					} else {
						Matcher m2 = columnNumberPattern.matcher(mapping)
						if(!m2.matches()) {
							error("URI property mapping value does not match any of the pattern ${URIPropPattern} or ${columnNumberPattern} - ${l}")
						}
						URIColumn = Integer.parseInt(mapping)
					}
					
					col = URIColumn
					
				} else {
				
				
					PropertyInterface pInt = VitalSigns.get().getClassProperty(clazzObj, property)
					
					
					if(pInt == null) error("Map file line: ${c} - Class ${clazz} property ${property} not found - ${l}")
				
					Matcher m = columnNumberPattern.matcher(mapping)
					
					if(!m.matches()) {
						error("Column mapping value does not match pattern ${columnNumberPattern} - ${l}")
					}
					
					col = Integer.parseInt(mapping)
					
					propertyToInterface.put(property, pInt)
				
				}
				
				
				if ( new HashSet<Integer>(propertyToColumn.values()).contains(col) ) error("Map file line: ${c} - column ${col} used more than once - ${l}")

				if(propertyToColumn.containsKey(property)) error("Map file line: ${c} - property ${property}, columns ${col} and ${propertyToColumn.get(property)} mapped twice - ${l}")
								
				propertyToColumn.put(property, col)
				
				
			}
			
			reader.close()
			
			if(propertyToColumn.size() < 1) error("No mappings in mapping file found: ${mapFile.absolutePath}")
			
			if(URIColumn == null) error("No required URI property mapping found in map file:  ${mapFile.absolutePath}")
			
			Set<Integer> cols = new HashSet<Integer>(propertyToColumn.values())
			
			println "mapped columns count ${cols.size()}"
			
			if(inputBlock && outputCSV) {
				
				println "Block->CSV case, checking columns index continuity..."
				
				for(int i = 0; i < cols.size(); i++) {
					if(!cols.contains(i)) error("Columns index continuity broken: no ${i} column defined")
				}
				
			} else {
			
				println "CSV->block case, columns index continuity not checked"	
			
			}
			
		}
		
		if( inputBlock && outputBlock ) {
			
			error("Cannot convert block-to-block")
			
		} else if( inputCSV && outputCSV ) {
		
			error("Cannot convert csv-to-csv")
			
		} else if( inputCSV && outputBlock ) {
		
			println "Converting CSV to VITAL block ..."
		
			InputStream is = new FileInputStream(inputFile)
			
			if(iname.endsWith(".gz")) {
				is = new GZIPInputStream(is)
			}
			
			CsvReader csvReader = new CsvReader(is, Charset.forName("UTF-8"))
			
			BlockCompactStringSerializer serializer = createSerializer(outputFile);
			
			int blocks = 0
			
			if(mapFile != null) {
				
				Boolean skipHeader = options.sh
				if(skipHeader == null) skipHeader = false
				
				if(skipHeader) {
					
					println "using map file, skip header flag set - ignoring first record"
					csvReader.readRecord()
					
				} else {
				
					println "using map file, assuming input csv does not have header"
					
				}
				
				
			} else {
			
				println "No optional map file, using csv first row (header) as mapping"
				
				if(!csvReader.readRecord()) error("Input file does not have any records ${inputFile.absolutePath}")
				
				String[] hv =  csvReader.getValues()
				
				for(int i = 0 ; i < hv.length; i++) {
				
					String h = hv[i]
					
					Matcher m = classPropPattern.matcher(h)
					
					if(!m.matches()) {
						error("Header column ${i} does not match pattern ${classPropPattern.pattern()}, file: ${inputFile.absolutePath}")
					}
					
					String cls = m.group(1)
					
					clazz = cls
					
					String property = m.group(2)
					
					if(clazz != null && cls != clazz) error("Only one class allowed in mapping header, two detected: ${clazz} vs. ${cls}, column ${i}")

					if(clazzObj == null) {

						try {
							
							clazzObj = Class.forName(clazz)
							
							if( ! GraphObject.class.isAssignableFrom(clazzObj) ) error("Class is not a sublcass of ${GraphObject.class.canonicalName}")
							
						} catch(Exception e) {
							error(e.localizedMessage)
						}
												
						
					}
					
					if(property == 'URI') {

						URIColumn = i
												
					} else {
					
						PropertyInterface pInt = VitalSigns.get().getClassProperty(clazzObj, property)
								
						if(pInt == null) error("Class ${clazz} property ${property} not found, column ${i}")
						
						if(propertyToColumn.containsKey(property)) error("Property ${property} mapped twice, columns ${i} and ${propertyToColumn.get(property)}")
								
								
						propertyToInterface.put(property, pInt)
					}
					
					
					propertyToColumn.put(property, i)
						
				}
				
				if(URIColumn == null) error("No column with URI defined")
				
				
			}
			
			Map<Integer, String> column2Prop = new HashMap<String, Integer>()
			
			for(Entry<String, Integer> e : propertyToColumn.entrySet()) {
				column2Prop.put(e.value, e.key)	
			}

			while(csvReader.readRecord()) {
				
				String[] vs = csvReader.getValues()

//				if(vs.length != propertyToColumn.size()) error("Columns count does not match mappings count: ${vs.length}, ${propertyToColumn}")
				
				if(URIColumn > vs.length - 1) error("Record ${csvReader.currentRecord}: columns count (${vs.length}) <= URIColumn index ${URIColumn}")
				
				String URI = vs[URIColumn]
				
				if(URI.isEmpty()) error("Record ${csvReader.currentRecord}: empty URI, column ${URIColumn}")
				
				if(URIPrefix != null) {
					URI = URIPrefix + URI
				}
				
				if(URISuffix != null) {
					URI = URI + URISuffix
				}
				
				GraphObject g = clazzObj.newInstance()
				
				g.URI = URI
				
				for( int i = 0 ; i < vs.length ; i++ ) {
					
					if(i == URIColumn) continue
					
					String propertyName = column2Prop.get(i)
					if(!propertyName) continue

					PropertyInterface pInt = propertyToInterface.get(propertyName)
					
					String v = vs[i]
					
					//string to value
					
					g."${propertyName}" = de(v, pInt.getPropertyClass())
					
					
				}
				
				serializer.startBlock()
				serializer.writeGraphObject(g)
				serializer.endBlock()
				
				blocks++
				
			}
						
			serializer.close()
			
			csvReader.close()
			
			println "Block written: ${blocks}"
			
		} else if( inputBlock && outputCSV ) {
		
			Map<Integer, String> column2Prop = new HashMap<String, Integer>()
			
			for(Entry<String, Integer> e : propertyToColumn.entrySet()) {
				column2Prop.put(e.value, e.key)
			}
		
			println "Converting VITAL block to CSV ..."
			
			if(mapFile == null) error("block -> csv conversion requires map file")
			
			BlockIterator iterator = BlockCompactStringSerializer.getBlocksIterator(openReader(inputFile));
			
			OutputStream os_ = new FileOutputStream(outputFile)
			
			if(oname.endsWith(".gz")) {
				os_ = new GZIPOutputStream(os_)
			}

			int blocks = 0
			
			CsvWriter writer = new CsvWriter(os_, (char)',', Charset.forName("UTF-8"))
			
			Boolean oh = options.oh
			if(oh == null) oh = false

			if(oh == true) {
				
				println "Writing header..."
				
				String[] head = new String[propertyToColumn.size()]
				
				for(int i = 0; i < propertyToColumn.size(); i++) {
					
					String property = column2Prop.get(i)
				
					head[i] = clazz + '.' + property
						
				}

				writer.writeRecord(head)
				
			} else {
				println "No csv header - flag not set"
			}
						
			for( ; iterator.hasNext(); ) {
				
				blocks ++
				
				VitalBlock block = iterator.next()
				
				GraphObject g = block.mainObject
				
				if( g.getClass() != clazzObj ) {
					error("Main block object class mismatch: ${g.getClass().canonicalName}, expected: ${clazzObj.canonicalName}")
				}
				
				
				String[] vals = new String[propertyToColumn.size()]

				Map<String, PropertyInterface> propsMap = g.properties
						
				for(int i = 0; i < propertyToColumn.size(); i++) {
					
					String property = column2Prop.get(i)
					
					String v = ''
					
					if(i == URIColumn) {
						
						v = g.URI
						
						if( URIPrefix != null && URIPrefix.length() > 0 ) {
							if( ! v.startsWith(URIPrefix) ) error("Object URI ${v} does not match URI mapping pattern: ${URIPattern}")
							v = v.substring(URIPrefix.length()) 
						}
						
						if( URISuffix != null && URISuffix.length() > 0 ) {
							if( !v.endsWith(URISuffix) ) error("Object URI ${v} does not match URI mapping pattern: ${URIPattern}")
							v = v.substring(0, v.length() - URISuffix.length())
						}
						
					} else {
					
						PropertyInterface pInt = propsMap.get(property)
						
						if(pInt != null && pInt.value != null) {
						
							v = es(pInt.value) 
							
						}
						
					}
					
					vals[i] = v
					
				}
				
				writer.writeRecord(vals)
				
				
			}
			
			writer.close()
			
			iterator.close()
			
			println "Records written: ${blocks}"
			
		} else {
			error("Unhandled case")
		}
		
	}

	
	//XXX copied and modified from compact string format!
	private static <T> T de(String deserialized, Class<T> clazz) {
		
		if(clazz == String) {
			return deserialized;
		} else if(clazz == URIPropertyValue) {
			return new URIPropertyValue(deserialized);
		} else if(deserialized.isEmpty()) {
			return null
		} else if(clazz == Boolean) {
			return Boolean.parseBoolean(deserialized);
		} else if(clazz == Integer) {
			return Integer.parseInt(deserialized);
		} else if(clazz == Long) {
			return Long.parseLong(deserialized);
		} else if(clazz == Double) {
			return Double.parseDouble(deserialized);
		} else if(clazz == Float) {
			return Float.parseFloat(deserialized);
		} else if(clazz == Date) {
			return new Date(Long.parseLong(deserialized));
		} else {
			throw new Exception("Unsupported data type: " + clazz.getCanonicalName());
		}
			
	}
	
	private static String es(Object input) {
		
		String s = null;
		
		if(input instanceof String) {
			s = (String)input;
		} else if(input instanceof URIPropertyValue) {
			s = ((URIPropertyValue) input).getURI();
		} else if(input instanceof Boolean) {
			s = "" + input;
		} else if(input instanceof Integer) {
			s = "" + input;
		} else if(input instanceof Long) {
			s = "" + input;
		} else if(input instanceof Double) {
			s = "" + input;
		} else if(input instanceof Float) {
			s = "" + input;
		} else if(input instanceof Date) {
			s = "" + ((Date)input).getTime();
		} else {
			throw new Exception("Unsupported data type: " + input.getClass().getCanonicalName());
		}
		return s;
		//all literal properties
		
	}
	
	static void nt2Vital(File inputNTFile, File outputBlockFile) {
		
		BlockCompactStringSerializer writer = null
		
		int objects = 0;
		
		Set<String> uris = new HashSet<>()
		
		VitalNTripleIterator iterator = null;
		
		try {
			
			writer = new BlockCompactStringSerializer(outputBlockFile)
			
			iterator = new VitalNTripleIterator(inputNTFile)
			
			for(GraphObject g : iterator) {
				
				if(!uris.add(g.URI)) error "Dupliacate URI in n-triple file: ${g.URI}"
				
				writer.startBlock()
				writer.writeGraphObject(g)
				writer.endBlock()
				
				objects++
				
			}
					
		} finally {
			if(writer != null) try {writer.close()} catch(Exception e){}
			IOUtils.closeQuietly(iterator)
		}
		
		println "DONE, graph objects count: ${objects}"
		
	}
	
	static void vital2NT(File inputFile, File outputFile) {
		
		BlockIterator iterator = null
		
		BufferedOutputStream bos = null
		
		int c = 0
		
		int b = 0
		
		Set<String> uris = new HashSet<>()
		
		try {
			
			OutputStream os = new FileOutputStream(outputFile)
			if(outputFile.getName().endsWith(".gz")) {
				os = new GZIPOutputStream(os)
			}
			
			bos = new BufferedOutputStream(os)
			
			iterator = BlockCompactStringSerializer.getBlocksIterator(inputFile)

			Model m = ModelFactory.createDefaultModel()
			
			for(VitalBlock block : iterator) {
				
				b++
				
				List<GraphObject> gos = []
				if( block.mainObject != null ) gos.add(block.mainObject)
				for(GraphObject g : block.dependentObjects) {
					if(g) gos.add(g)
				}
				
				for(GraphObject g : gos) {
					
					if(!uris.add(g.URI)) error "Dupliacate URI in input block file: ${g.URI}, block: ${b}"
					
					m.removeAll()
					
					g.toRDF(m)
					
					m.write(bos, "N-TRIPLE")
				
					c++
						
				}
				
			}
			
			println "Total objects converted to N-Triple: ${c}"
						
		} finally {
			IOUtils.closeQuietly(bos)
			if(iterator != null) iterator.close()
		}
		
	}
	
}
