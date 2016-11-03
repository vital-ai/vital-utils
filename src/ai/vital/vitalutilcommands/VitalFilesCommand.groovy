package ai.vital.vitalutilcommands

import java.util.Map;
import java.util.Map.Entry

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils

import ai.vital.domain.FileNode
import ai.vital.domain.FileNode_PropertiesHelper;
import ai.vital.query.querybuilder.VitalBuilder
import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalservice.VitalService
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.factory.VitalServiceFactory
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalservice.query.VitalSelectQuery
import ai.vital.vitalsigns.model.VitalServiceKey
import ai.vital.vitalsigns.model.property.URIProperty;

class VitalFilesCommand extends AbstractUtil {

static Map cmd2CLI = new LinkedHashMap()

	static String VF = "vitalfiles"
	
	static String CMD_HELP = 'help'
	
	static String CMD_PUT = "put"
	 
	static String CMD_GET = "get"
	 
	static String CMD_DEL = "del"
	 
	static String CMD_LS  = "ls"
	
	static String S3_API_DATASCRIPT = 'S3ApiDatascript.groovy'
	
	static {
		
		def putCLI = new CliBuilder(usage: "${VF} ${CMD_PUT} [options]", stopAtNonOption: false)
		putCLI.with {
			sk longOpt: 'service-key', "vital service key, default ${defaultServiceKey}", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
			f longOpt: "file", "local file to upload", args: 1, required: true
			n longOpt: "name", "file node name (label)", args: 1, required: true
//			fc longOpt: 'file-class', "FileNode class, default"
			s longOpt: "segment with file nodes", "files segment", args: 1, required: true
			au longOpt: "account-uri", "account-uri", args: 1, required: true
			pu longOpt: "profile-uri", "profile-uri (optional)", args: 1, required: false
			sc longOpt: 'scope', "scope: public/private", args: 1, required: true
			pa longOpt: 'path', 'target path', args: 1, required: true
			ow longOpt: "overwrite", "overwrite file if exists", args: 0, required: false
		}
		cmd2CLI.put(CMD_PUT, putCLI)
		
		def getCLI = new CliBuilder(usage: "${VF} ${CMD_GET} [options]", stopAtNonOption: false)
		getCLI.with {
			n longOpt: "name", "remote file name", args: 1, required: true
			d longOpt: "directory", "output directory to save the file", args: 1, required: true
			ow longOpt: "overwrite", "overwrite the output file if exists", args: 0, required: false
			sk longOpt: 'service-key', "vital service key, default ${defaultServiceKey}", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_GET, getCLI)
		
		def lsCLI = new CliBuilder(usage: "${VF} ${CMD_LS} [options]", stopAtNonOption: false)
		lsCLI.with {
			sk longOpt: 'service-key', "vital service key, default ${defaultServiceKey}", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
//			fc longOpt: 'file-class', "FileNode class, default"
			s longOpt: "segment with file nodes", "files segment", args: 1, required: true
			au longOpt: "account-uri", "account-uri (optional)", args: 1, required: false
			pu longOpt: "profile-uri", "profile-uri (optional)", args: 1, required: false
			sc longOpt: 'scope', "scope: public/private (optional)", args: 1, required: false
		}
		cmd2CLI.put(CMD_LS, lsCLI)
		
		def delCLI = new CliBuilder(usage: "${VF} ${CMD_DEL} [options]", stopAtNonOption: false)
		delCLI.with {
			sk longOpt: 'service-key', "vital service key, default ${defaultServiceKey}", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
//			fc longOpt: 'file-class', "FileNode class, default"
			s longOpt: "segment with file nodes", "files segment", args: 1, required: true
			au longOpt: "account-uri", "account-uri", args: 1, required: true
			pu longOpt: "profile-uri", "profile-uri (optional)", args: 1, required: false
			sc longOpt: 'scope', "scope: public/private", args: 1, required: true
			pa longOpt: 'path', 'target path', args: 1, required: true

		}
		cmd2CLI.put(CMD_DEL, delCLI)
	
	}
	
	static void usage() {
		
		println "usage: ${VF} <command> [options] ..."
		
		println "usage: ${VF} ${CMD_HELP} (prints usage)"
		
		for(Entry e : cmd2CLI.entrySet()) {
			e.value.usage()
		}
		
	}

	
	public static void main(String[] args) {
		
		
		String cmd = args.length > 0 ? args[0] : null
		
		boolean printHelp = args.length == 0 || cmd == CMD_HELP
			
		if(printHelp) {
			usage();
			return;
		}
			
			
		String[] params = args.length > 1 ? Arrays.copyOfRange(args, 1, args.length) : new String[0]
			
		def cli = cmd2CLI.get(cmd)
			
		if(!cli) {
			System.err.println "unknown command: ${cmd}"
			usage()
			return
		}
			
		def options = cli.parse(params)
		if(!options) {
			return
		}
		
		String profile = options.prof ? options.prof : null
		if(profile != null) {
			println "Setting custom vital service profile: ${profile}"
		} else {
			println "Using default vital service profile... ${VitalServiceFactory.DEFAULT_PROFILE}"
			profile = VitalServiceFactory.DEFAULT_PROFILE
		}
		
		VitalService service = null
		
		VitalServiceKey serviceKey = getVitalServiceKey(options)
		service = VitalServiceFactory.openService(serviceKey, profile)
		if(service.getEndpointType() != EndpointType.VITALPRIME) {
			error "${VF} only works with vitalprime"
		}
		
		println "appID: ${service.getApp()}"
		
		String filesSegment = options.s
		println "files segment: ${filesSegment}"
		
		String accountURI = options.au ? options.au : null
		println "accountURI: ${accountURI}"
		
		String profileURI = options.pu ? options.pu : null
		println "profileURI: ${profileURI}"
		
		String scope = options.sc ? options.sc : null
		println "scope: ${scope}"
		
		if(scope) {
			if(scope.equalsIgnoreCase('public')) {
				scope = 'Public'
			} else if(scope.equalsIgnoreCase('private')){
				scope = 'Private'
			} else {
				error("invalid scope: ${scope}")
			}
			
		}
		
		Class<? extends FileNode> fileClass = null
		//			if(options.fc != null) {
		//				fileClass = Class.forName(options.fc)
		//			} else {
						fileClass = FileNode.class
		//			}
		
		if(cmd == CMD_PUT) {

			String name = options.n
			println "name: ${name}"
			
			String path = options.pa
			println "path: ${path}"
			
			File srcFile = new File(options.f)

			boolean overwrite = options.ow ? true : false
			println "Overwrite ? ${overwrite}"
						
			if(!srcFile.exists()) error("Local file does not exist: ${srcFile.absolutePath}")
			if(!srcFile.isFile()) error("Local file path is not a file: ${srcFile.absolutePath}")
			
			String n = srcFile.name
	
			String tempFileURI = "urn:" + RandomStringUtils.randomAlphanumeric(16)
			String tempFileName = n
			
			try {
				
			FileInputStream fis = null
			println "Uploading file into temp location, ${tempFileURI}/${tempFileName}"
			try {
				fis = new FileInputStream(srcFile)
				VitalStatus status = service.uploadFile(URIProperty.withString(tempFileURI), n, fis, true)
				if(status.status != VitalStatus.Status.ok) error("Error when uploading file: ${status.message}")
			} finally {
				IOUtils.closeQuietly(fis)
			}
			
			println "Temp file uploaded, creating node and persisting..."


				
			FileNode prototype = new FileNode()
				
			//file uploaded successfully
			ResultList rl = service.callFunction(S3_API_DATASCRIPT, [
				action: 'create',
				fileClass: fileClass,
				filesSegment: filesSegment, 
				accountURI: accountURI,
				profileURI: profileURI,
				scope: scope,
				//desired path
				path: path,
				
				prototype: prototype,
				tempFileURI: tempFileURI,
				tempFileName: tempFileName,
				deleteOnSuccess: true,
				overwrite: overwrite,
				name: name
			])
			
			if(rl.status.status != VitalStatus.Status.ok) {
				error(rl.status.message)
			}
			
			FileNode newFile = rl.first()
			
			println "File created: ${newFile.URI}"
			
			} finally {
				try {
					service.deleteFile(URIProperty.withString(tempFileURI), tempFileName)	
				} catch(Exception e) {}
			}
			
		} else if(cmd == CMD_DEL) {
		
			String path = options.pa
			println "path: ${path}"
		
			//file uploaded successfully
			ResultList rl = service.callFunction(S3_API_DATASCRIPT, [
				action: 'delete',
				fileClass: fileClass,
				filesSegment: filesSegment,
				accountURI: accountURI,
				profileURI: profileURI,
				scope: scope,
				//desired path
				path: path,
				
			])
			
			if(rl.status.status != VitalStatus.Status.ok) {
				error(rl.status.message)
			}
			
			println "File deleted"
			
		} else if(cmd == CMD_LS) {
		
			VitalSelectQuery sq = new VitalBuilder().query {
				
				SELECT {
					value offset: 0
					value limit: 1000
					value segments: [filesSegment]
					
					node_constraint { FileNode.class }
					
					if(scope) {
						node_constraint { ((FileNode_PropertiesHelper)FileNode.props()).fileScope.equalTo(scope) }
					}
					
					if(accountURI) {
						node_constraint { ((FileNode_PropertiesHelper)FileNode.props()).accountURI.equalTo(URIProperty.withString(accountURI)) }
					}
					
					if(profileURI) {
						node_constraint { ((FileNode_PropertiesHelper)FileNode.props()).profileURI.equalTo(URIProperty.withString(profileURI)) }
					}
					
				}
				
			}.toQuery()
			
			ResultList rl = service.query(sq)
			if(rl.status.status != VitalStatus.Status.ok) {
				error("Error when querying for the file nodes: ${rl.status.message}")
			}	
			
			List<FileNode> nodes = rl.iterator(FileNode.class, false).toList()
			println "Nodes found: ${rl.totalResults}, returned: ${nodes.size()}"
			
			int i = 0
			for(FileNode node : nodes) {
				i++
				println "${i}. ${node.fileURL} name: ${node.name} scope: ${node.fileScope} length: ${node.fileLength} bytes"
				
			}
		
		} else {
			error("Unhandled command: ${cmd}")
		}
	}
	
}
