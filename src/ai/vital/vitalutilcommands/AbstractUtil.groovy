package ai.vital.vitalutilcommands

import ai.vital.vitalsigns.utils.BlockCompactStringSerializer
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import org.apache.commons.io.FileUtils

class AbstractUtil {

	static String[] extensions = ['vital.gz', 'vital', 'gz'].toArray(new String[3]);
	
	static long t() { return System.currentTimeMillis()}
	
	public static List<File> getBlockFilesFromPath(File path) {
		
		List<File> files = new ArrayList<File>();
		
		if(!path.exists()) throw re("Path does not exist: " + path.getAbsolutePath());
		
		if(path.isDirectory()) {
			files.addAll(FileUtils.listFiles(path, extensions, true));
		} else {
			for(String ext : extensions) {
				if(path.getName().endsWith(ext)) {
					files.add(path);
					break;
				}
			}
		}
		
		return files;
	}
	
	public static isValidBlockFile(File inF) {
		for(String e : extensions) {
			if(inF.getName().endsWith(".${e}")) {
				return true
			}
		}
		
		return false
	} 
	

	static protected BufferedReader openReader(File f) throws IOException {
		
		InputStream inputStream = new FileInputStream(f);
				
		if(f.getName().endsWith(".gz")) {
			inputStream = new GZIPInputStream(inputStream);
		}
				
		return new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
	}
	
	static void error(String m) {
		System.err.println "ERROR: ${m}"
		System.exit(-1)
	}
	
	static protected BlockCompactStringSerializer createSerializer(File outputTestData) throws IOException {
		
		BlockCompactStringSerializer serializer = new BlockCompactStringSerializer(createWriter(outputTestData));
				
		return serializer;
	}
	
	static protected BufferedWriter createWriter(File output) {
		
		OutputStream os = new FileOutputStream(output);
		
		String name = output.getName();
		if(name.endsWith(".vital.gz")) {
			os = new GZIPOutputStream(os);
		} else if(name.endsWith(".vital")){
					
		} else {
			throw new IOException("Vital block file must end with .vital[.gz]");
		}
		
		return new BufferedWriter(new OutputStreamWriter(os, "UTF-8"))
		
	}
	
	static FileFilter blockFileNameFilter = new FileFilter(){
		public boolean accept(File f) {
			return f.name.endsWith(".vital.gz") || f.name.endsWith(".vital")
		}
	}
	
	static FileFilter ntripleFileFilter = new FileFilter() {
		public boolean accept(File f) {
			return f.name.endsWith(".nt.gz") || f.name.endsWith(".nt")
		}
	}
	
}
