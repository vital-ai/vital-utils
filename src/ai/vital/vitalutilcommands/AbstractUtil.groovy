package ai.vital.vitalutilcommands

import ai.vital.vitalsigns.block.BlockCompactStringSerializer
import ai.vital.vitalutilcommands.io.ProgressInputStream;
import groovy.transform.CompileStatic;

import java.beans.PropertyChangeListener;
import java.util.zip.GZIPInputStream

import java.util.zip.GZIPOutputStream

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils;

class AbstractUtil {

	static String[] extensions = ['vital.gz', 'vital', 'gz'].toArray(new String[3]);
	
	static long t() { return System.currentTimeMillis()}
	
	public static boolean exit_not_exception = true 
	
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
		return openReader(f, null)
	}
	
	static protected BufferedReader openReader(File f, PropertyChangeListener listener) {
		

		InputStream inputStream = new FileInputStream(f);
		
		if(f.getName().endsWith(".gz")) {
			inputStream = new GZIPInputStream(inputStream);
		}
		
		if(listener != null) {
			
			ProgressInputStream pis = new ProgressInputStream(inputStream)
			pis.addPropertyChangeListener(listener);
			inputStream = pis
			
		}
		
		return new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
		
	}
	
	static void error(String m) {
		System.err.println "ERROR: ${m}"
		if(exit_not_exception) {
			System.exit(-1)
		} else {
			throw new RuntimeException(m)
		}
	}
	
	protected static void o(String m) { 
		println m
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
	
	static FileFilter builderFileFilter = new FileFilter() {
		public boolean accept(File f) {
			return f.name.endsWith(".groovy") || f.name.endsWith(".builder")
		}
	}

	/**
	 * Fast but unreliable way of determining the uncompressed gzip stream length
	 * http://stackoverflow.com/questions/9715046/find-the-size-of-the-file-inside-a-gzip-file
	 */
	static long getGZIPUncompressedLengthUnreliable(File file) throws IOException {
		
		RandomAccessFile raf = null;
		
		try {
			raf = new RandomAccessFile(file, "r");
			raf.seek(raf.length() - 4);

			int b4 = raf.read();
			int b3 = raf.read();
			int b2 = raf.read();
			int b1 = raf.read();
			int val = (b1 << 24) | (b2 << 16) + (b3 << 8) + b4;

			return (long) val

		} finally {
			IOUtils.closeQuietly(raf)
		}
		
	}
	
	/**
	 * Slow but reliable way of determining the gzip uncompressed stream length 
	 */
	static long getGZIPUncompressedLength(File file) throws IOException {
		
		GZIPInputStream zis = null;
		
		try {
			zis = new GZIPInputStream(new FileInputStream(file));
			
			long val = 0;
		
			//1MiB buffer
			byte[] buf = new byte[1024*1024];
			
			while (zis.available() > 0) {
				int r = zis.read(buf);
				if (r > 0) val += r;
			}

			return val;
			
		} finally {
			IOUtils.closeQuietly(zis)
		}		
		
	}
	
	
	@CompileStatic
	public static String humanReadableByteCount(long bytes) {
		return humanReadableByteCount(bytes, false)
	}
	
	/**
	 * http://stackoverflow.com/questions/3758606/how-to-convert-byte-size-into-human-readable-format-in-java
	 */
	@CompileStatic
	public static String humanReadableByteCount(long bytes, boolean si) {
		int unit = si ? 1000 : 1024;
		if (bytes < unit) return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		String pre = "" + (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
		return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}
	
}
