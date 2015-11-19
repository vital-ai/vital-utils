package ai.vital.vitalutilcommands

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;

import com.csvreader.CsvWriter;
import com.vitalai.domain.nlp.Document
import com.vitalai.domain.nlp.Edge_hasEntity
import com.vitalai.domain.nlp.Entity

import ai.vital.vitalsigns.VitalSigns;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.BlockIterator;
import ai.vital.vitalsigns.block.BlockCompactStringSerializer.VitalBlock;
import ai.vital.vitalsigns.model.GraphObject
import ai.vital.vitalsigns.model.VitalApp;
import junit.framework.TestCase;

class VitalConvertCommandTest extends AbstractUtilTest {

	File tempDir = null
	VitalApp app = null

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		AbstractUtil.exit_not_exception = false
		tempDir = Files.createTempDirectory("vitalutil" ).toFile()
		app = VitalApp.withId('app')
	}
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
//		FileUtils.deleteQuietly(tempDir)
	}
	
	public void testCSVBlockPerformance() {
		
		File inputFile1 = new File(tempDir, "input1.csv")
		
		File outputFile1 = new File(tempDir, "output1.vital")

		CsvWriter writer1 = new CsvWriter(new FileOutputStream(inputFile1), (char)',', StandardCharsets.UTF_8)
		
		VitalSigns vs = VitalSigns.get()
		
		writer1.writeRecord([
//			'ai.vital.domain.Document.URI',
			'ai.vital.domain.Document.title',
			'ai.vital.domain.Document.body'
		] as String[])
		
		int max = 100000
		
		for(int i = 0 ; i < max; i++) {
			writer1.writeRecord([
				'title ' + i,
				'body ' + i
			] as String[])
		}
		
		writer1.close()
		
		long s = System.currentTimeMillis()
		
		
		VitalConvertCommand.main([
			"-i", inputFile1.absolutePath,
			"-o", outputFile1.absolutePath,
			"-ag",
			"-ow",
			"-sc"
			
		] as String[])
		
		s = System.currentTimeMillis() - s
		
		println "Total time: ${s}ms"
		
		int i = 0
		
		for( BlockIterator iter = BlockCompactStringSerializer.getBlocksIterator(outputFile1); iter.hasNext(); ) {
			
			List<GraphObject> block = iter.next().toList();
			assertEquals(1, block.size())
			i++
			
			Document doc = block.get(0)
			
			assertNotNull(doc.title)
			assertNotNull(doc.body)
			
		}
		
		assertEquals(max, i)
		
				
	}
	
	public void testCSVToBlockConversion() {

		//test input csv no headers autogenerate
//		Document d1 = new Document().generateURI(app)
		
		//no mapping, simple csv file, headers
		String csv1 = """\
"ai.vital.domain.Document.title","ai.vital.domain.Document.body"
"title 1","body 1"
"title 2","body 2"
"title 3","body 3"
"""
		File inputFile = new File(tempDir, "input1.csv")
		File outputFile = new File(tempDir, "output1.vital")
		
		FileUtils.writeStringToFile(inputFile, csv1, 'UTF-8')
		
		try {
			VitalConvertCommand.main([
				"-i", inputFile.absolutePath, 
				"-o", outputFile.absolutePath,
				"-sc"
			] as String[])
			fail("should fail, no -ag option")
		} catch(Exception e) {}
		
		VitalConvertCommand.main([
			"-i", inputFile.absolutePath,
			"-o", outputFile.absolutePath,
			"-ag",
			"-ow"
		] as String[])
		
		int i = 0
		for( BlockIterator iter = BlockCompactStringSerializer.getBlocksIterator(outputFile); iter.hasNext(); ) {
			
			List<GraphObject> block = iter.next().toList();
			assertEquals(1, block.size())
			i++
			
		}
		
		assertEquals(3, i)
		
		String mf2 = """\

ai.vital.domain.Document.title  = 0

ai.vital.domain.Document.body  = 1

ai.vital.domain.Document.name = 2

"""
		//no mapping, simple csv file, headers
		String csv2 = """\
"title 1","body 1","name 1"
"title 2","body 2","m"
"title 3","body 3",""
"title 4","body 4",
"""
	
		File inputFile2 = new File(tempDir, "input2.csv")
		FileUtils.writeStringToFile(inputFile2, csv2, 'UTF-8')
		File mappingFile2 = new File(tempDir, "mapping2.map")
		FileUtils.writeStringToFile(mappingFile2, mf2, 'UTF-8')
		File outputFile2 = new File(tempDir, "output2.vital")
		
		try {
			VitalConvertCommand.main([
				"-i", inputFile.absolutePath,
				"-o", outputFile.absolutePath
			] as String[])
			fail("should fail, no -ag option")
		} catch(Exception e) {}
		
		VitalConvertCommand.main([
			"-i", inputFile2.absolutePath,
			"-m", mappingFile2.absolutePath,
			"-o", outputFile2.absolutePath,
			"-ag",
			"-ow",
			"-nv"
		] as String[])
		
		i = 0
		
		for( BlockIterator iter = BlockCompactStringSerializer.getBlocksIterator(outputFile2); iter.hasNext(); ) {
			
			List<GraphObject> block = iter.next().toList();
			assertEquals(1, block.size())
			i++
			
		}
		
		assertEquals(4, i)
		
		
		//block -> csv
		VitalConvertCommand.main([
			"-i", outputFile2.absolutePath,
			"-m", mappingFile2.absolutePath,
			"-o", inputFile2.absolutePath,
			"-ow",
		] as String[])
		
		
		String outputCSV = FileUtils.readFileToString(inputFile2, 'UTF-8')
		
		println "input CSV: $csv2"
		
		println "output CSV: $outputCSV"
		
	}
	
	public void testBlockNTConversion() {

		
		Document doc = new Document().generateURI(app)
		doc.title = "Test doc"
		doc.publicationDate = new Date()
		doc.active = true
		doc.body = RandomStringUtils.randomAlphanumeric(256)
		
		Entity e1 = new Entity().generateURI(app)
		e1.name = "Test entity"
		
		Edge_hasEntity edge = new Edge_hasEntity().addSource(doc).addDestination(e1).generateURI(app)
		
		List<GraphObject> objs = [doc, edge, e1]
		
		File tempBlock = new File(tempDir, "test.vital")
		File tempBlockOut = new File(tempDir, "test2.vital")
		
		BlockCompactStringSerializer writer = new BlockCompactStringSerializer(tempBlock)
		
		Map<String, GraphObject> inputMap = [:]
		
		for(GraphObject g : objs) {
			writer.startBlock()
			writer.writeGraphObject(g)
			writer.endBlock()
			inputMap.put(g.URI, g)
		}
		writer.close()
		
		File outputNT = new File(tempDir, "test.nt")		
		
		VitalConvertCommand.main(['-i', tempBlock.absolutePath, '-o', outputNT.absolutePath] as String[])
		
		VitalConvertCommand.main(['-i', outputNT.absolutePath, '-o', tempBlockOut] as String[])
		
		Map<String, GraphObject> outputMap = [:]
		for( BlockIterator iter = BlockCompactStringSerializer.getBlocksIterator(tempBlockOut); iter.hasNext(); ) {
			VitalBlock block = iter.next()
			outputMap.put(block.mainObject.URI, block.mainObject)
			for(GraphObject g : block.dependentObjects) {
				outputMap.put(g.URI, g)
			}
		}
		
		assertEquals(inputMap.size(), outputMap.size())
		
		assertEquals(inputMap.keySet(), outputMap.keySet())
		
		for(GraphObject g : inputMap.values()) {
			GraphObject g2 = outputMap.get(g.URI)
			assertNotNull(g2)
			assertEquals(g, g2)
		}
		
		
		
	}

}
