package ai.vital.vitalutilcommands

import java.util.Map.Entry
import org.apache.commons.io.IOUtils;

import ai.vital.domain.ontology.VitalOntology;
import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.model.DomainOntology
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;

/**
 * a script that merges different vital block files
 * @author Derek
 *
 */
class VitalMerge extends AbstractUtil {

	public static void main(String[] args) {
		
		def cli = new CliBuilder(usage: 'vitalmerge [options]')
		cli.with {
			i longOpt: 'input', "input block file", args: 1, required: true
			o longOpt: 'output', "output block file", args: 1, required: true
			ow longOpt: 'overwrite', "overwrite output file if exists", args: 0, required: false
			or longOpt: 'override', "ignore ontology version conflicts and transform global annotations into block annotations", args: 0, required: false
		}
		
		if(args.length == 0) {
			cli.usage()
			return
		}
		
		def options = cli.parse(args)
		
		if(!options) {
			return
		}
		
		def inputs = options.is
		
		println "Input params count: ${inputs.size()}"
		
		File output = new File(options.o)
		
		Boolean overwrite = options.ow
		if(overwrite == null) overwrite = false
		println "Output file: ${output.absolutePath}"
		println "overwrite ? ${overwrite}"
		
		Boolean override = options.or
		if(override == null) override = false
		println "override ? ${override}"
		
		Set<File> inputF = []
		
		for(String f: inputs) {
			File inF = new File(f)
			if(!inF.exists()) {
				error "Input path does not exist: ${inF.absolutePath}"
			}
			
			if(inF.isDirectory()) {
				List<File> subList = getBlockFilesFromPath(inF)
				println "Found ${subList.size()} block files in input directory: ${inF.absolutePath}"
				inputF.addAll(subList)
			} else {
				if ( !isValidBlockFile(inF) ) {
					error "Input file name is invalid: ${inF.name}"
				}
				inputF.add(inF)
			}
		}
		
		println "Input files: ${inputF.size()}"
		
		if(inputF.size() < 1) {
			error "No input files."
		} else if(inputF.size() == 1) {
			error "At least 2 files required to merge"
		}
		
		
		//sanity check
		for(File f : inputF) {
			if(f.equals(output)) {
				error "Output file cannot be the input at the same time: ${output.absolutePath}"
			}
		}
		
		
		if(output.exists()) {
			
			if(!overwrite) {
				error "Output file already exists, use overwrite flag"
			} else {
				println "Overwriting existing file..."
			}
			
		}

		Map<File, List<DomainOntology>> file2Ontologies = [:]
		
		List<DomainOntology> ontologies = new ArrayList<DomainOntology>(VitalSigns.get().getDomainList())
		
		for( Iterator<DomainOntology> iter = ontologies.iterator(); iter.hasNext(); ) {
			DomainOntology d = iter.next()
			if(d.uri == VitalOntology.NS) iter.remove()
		}
		
		if(ontologies.size() > 1) error "Vitalsigns is expected to have exactly 0 or 1 domain ontology"
		
		println "Checking global ontologies..."
		
		//read in all
		for(File i : inputF) {
			
			List<DomainOntology> fileOnts = []
			
			file2Ontologies.put(i, fileOnts)
			
			int lc = 0
					
			BufferedReader reader = null
			
			try {
				
				reader = openReader(i)
				
				for(String l = reader.readLine(); l != null; l = reader.readLine()) {
					
					lc++
					
					if(l.startsWith(BlockCompactStringSerializer.BLOCK_SEPARATOR)) {
						break
					} else if(l.startsWith("@")) {
						String ontologyIRI = l.substring(1).trim()
						int spl = ontologyIRI.indexOf('$')
						if(spl <= 0) throw new Exception("Invalid ontology version IRI: ${ontologyIRI}")
						
						DomainOntology ont = new DomainOntology(ontologyIRI.substring(0, spl), ontologyIRI.substring(spl+1))
						
						DomainOntology match = null
						
						for(DomainOntology c : ontologies) {
							
							if(c.uri == ont.uri && c.major == ont.major && c.compareTo(ont) >= 0) {
								match = c
								break
							}
							
						}
						
						if(!match) {
							String m = "No matching ontology found: ${ont.toVersionIRI()}, loaded ontologies: "
							for(DomainOntology d : ontologies) {
								m += "${d.toVersionIRI()}"
							}
							throw new Exception(m)
						}
						
						if(fileOnts.size() > 0) error("More than one domain ontology detected - it's unsupported yet")
						
						fileOnts.add(ont)
					}
					
				}
			} catch(Exception e) {
				error "File: ${i.absolutePath}, line: ${lc} - ${e.localizedMessage}"
			} finally {
				IOUtils.closeQuietly(reader)
			}
			
			
		}
		
		
		println "Validating global ontologies..."
		
		DomainOntology lastOntology = null
		
		DomainOntology latestOntology = null
		
		//if override is fals
		
		if(!override) {
			
			for(Entry<File, List<DomainOntology>> e : file2Ontologies.entrySet()) {
				
				File f = e.getKey()
						
				List<DomainOntology> onts = e.getValue()
						
				if(onts.size() > 1) error("File: ${f.absolutePath} - more than one domain ontology detected - it's unsupported yet")
						
				if(onts.size() == 1) {
					DomainOntology current = onts[0]
					if(lastOntology != null) {
						if(lastOntology.uri != current.uri || lastOntology.compareTo(current) != 0) {
							error "Different ontologies versions found: ${current.toVersionIRI()} vs. ${lastOntology.toVersionIRI()}"
						} 
					}
					lastOntology = current
				}
				
			}
			
			//safe to insert current domain marker
			BufferedWriter writer = createWriter(output)
			
			if(ontologies.size() > 0) {
				DomainOntology ont = ontologies[0]
				writer.write("@${ont.toVersionIRI()}\n")
			}
			
			for(File f : inputF) {
				
				//copy everything except annotations
				BufferedReader r = openReader(f)
				
				for(String l = r.readLine(); l != null; l = r.readLine()) {
					
					if(l.startsWith("@")) continue
					
					writer.write(l)
					writer.write("\n")
					
					
				}
				
				r.close()
			}
			
			writer.close()
			
		} else {
		
			//transform global ontology markers into local

			//safe to insert current domain marker
			BufferedWriter writer = createWriter(output)
			
			DomainOntology ont = ontologies[0]
			if(ontologies.size() > 0) {
				writer.write("@${ont.toVersionIRI()}\n")
			}
			
			for(File f : inputF) {
				
				//copy everything except annotations
				BufferedReader r = openReader(f)
				
				for(String l = r.readLine(); l != null; l = r.readLine()) {
					
					if(l.startsWith("@")) continue
				
					
					List<DomainOntology> fileOnts = file2Ontologies.get(f)
					
					DomainOntology fOnt = fileOnts.size() > 0 ? fileOnts[0] : null
					
					writer.write(l)
					if(l == BlockCompactStringSerializer.BLOCK_SEPARATOR && fOnt != null) {
						writer.write('@')
						writer.write(fOnt.toVersionIRI())
					}
					
					writer.write("\n")
					
					
				}
				
				r.close()
				
			}
			
			writer.close()
		
		}

		println "DONE"		
	}
	
}
