package ai.vital.vitalutilcommands

import java.util.Map.Entry

import org.apache.commons.io.IOUtils;

import ai.vital.vitalservice.EndpointType;
import ai.vital.vitalsigns.model.property.URIProperty;
import ai.vital.vitalservice.VitalService;
import ai.vital.vitalservice.VitalStatus
import ai.vital.vitalservice.admin.VitalServiceAdmin;
import ai.vital.vitalservice.factory.VitalServiceFactory;
import ai.vital.vitalservice.query.ResultElement;
import ai.vital.vitalservice.query.ResultList;
import ai.vital.vitalsigns.model.VITAL_Node
import ai.vital.vitalsigns.model.VitalApp;
import ai.vital.vitalsigns.model.VitalServiceAdminKey;
import ai.vital.vitalsigns.model.VitalServiceKey;

class VitalFtpCommand extends AbstractUtil {

	static Map cmd2CLI = new LinkedHashMap()

	static String VF = "vitalftp"
	
	static String CMD_HELP = 'help'
	
	static String CMD_PUT = "put"
	 
	static String CMD_GET = "get"
	 
	static String CMD_DEL = "del"
	 
	static String CMD_LS  = "ls"

	static String CMD_PURGE = "purge"
	
	static String FTP_URI = "FTP";	 
	
	static {
		
		def putCLI = new CliBuilder(usage: "${VF} ${CMD_PUT} [options]", stopAtNonOption: false)
		putCLI.with {
			f longOpt: "file", "local file to upload", args: 1, required: true
			c longOpt: "commons", "put file into commons area, --admin-key required", args:0, required: false
			ow longOpt: "overwrite", "overwrite remote file if exists", args: 0, required: false
			sk longOpt: 'service-key', "vital service key, default ${defaultServiceKey}", args: 1, required: false
			ak longOpt: 'admin-key', "vital admin key, used with --commons only, default ${defaultServiceKey}", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
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
		}
		cmd2CLI.put(CMD_LS, lsCLI)
		
		def delCLI = new CliBuilder(usage: "${VF} ${CMD_DEL} [options]", stopAtNonOption: false)
		delCLI.with {
			n longOpt: "name", "remote file name", args: 1, required: true
			sk longOpt: 'service-key', "vital service key, default ${defaultServiceKey}", args: 1, required: false
			ak longOpt: 'admin-key', "vital admin key, used with commons/* files, required if default ${defaultServiceKey}", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_DEL, delCLI)
	
		def purgeCLI = new CliBuilder(usage: "${VF} ${CMD_PURGE} (no options)", stopAtNonOption: false)
		purgeCLI.with {
			sk longOpt: 'service-key', "vital service key, default ${defaultServiceKey}", args: 1, required: false
			prof longOpt: 'profile', 'vitalservice profile, default: default', args: 1, required: false
		}
		cmd2CLI.put(CMD_PURGE, purgeCLI)
			
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
		
		
		boolean commons = false
		
		if( cmd == CMD_PUT && options.c ) commons = true
		
		if( cmd == CMD_DEL && options.n.startsWith("commons/") ) commons = true 
		
		VitalServiceAdmin adminService = null
		VitalService service = null
		if(commons) {

			println "commons area, using admin key"			
			VitalServiceAdminKey adminKey = getVitalServiceAdminKey(options)
			adminService = VitalServiceFactory.openAdminService(adminKey, profile)
			
			if(adminService.getEndpointType() != EndpointType.VITALPRIME) {
				error "${VF} only works with vitalprime"
			}
			
		} else {
		
			println "Putting into app area"
		
			VitalServiceKey serviceKey = getVitalServiceKey(options)
			service = VitalServiceFactory.openService(serviceKey, profile)
			if(service.getEndpointType() != EndpointType.VITALPRIME) {
				error "${VF} only works with vitalprime"
			}
			
		}
		
		
		if(cmd == CMD_LS) {
			
			ResultList res = null
			try {
				res = service.callFunction("commons/scripts/VitalFTP_ListFiles.groovy", [:])
				if(res.status.status != VitalStatus.Status.ok) {
					error res.status.message 
				}
			} catch(Exception e) {
				error "Couldn't list files: ${e.localizedMessage}"
			}
			
			println "Files count: ${res.results.size()}"
			
			int i = 0
			
			for(ResultElement re : res.results) {
				
				VITAL_Node node = re.graphObject
				
				println "${++i}. ${node.name}\t\t${node.active ? '[IN USE]' : '\t'} ${new Date(node.timestamp.rawValue())}"
				
			}
			
		} else if(cmd == CMD_GET) {
		
			String n = options.n
			File dir = new File(options.d)
			boolean overwrite = options.ow ? true : false
			
			File targetFile = new File(dir, n)
			
			
			println "File name: ${n}"
			println "Target directory: ${dir.absolutePath}"
			println "Target location: ${targetFile.absolutePath}"
			
			if(!dir.exists()) error "Target directory does not exist: ${dir.absolutePath}"
			
			if(!dir.isDirectory()) error "Target directory path is not a directory: ${dir.absolutePath}"
			
			if(targetFile.exists()) {
				if(!overwrite) error("Target file already exists, use --overwrite option: ${targetFile.absolutePath}")
				println "File will be overwritten: ${targetFile.absolutePath}"
			}

			
			VITAL_Node node = getFile(service, n)
			
			if(node == null) error("File not found: ${n}")
			
			if(node.active) {
				error "File in use: ${n}"
			}
			
			BufferedOutputStream fos = null
			try {
				fos = new BufferedOutputStream(new FileOutputStream(targetFile))
				VitalStatus status = service.downloadFile(URIProperty.withString(FTP_URI), n, fos, true)
				println "download status: ${status}"
			} catch(Exception e) {
				error e.localizedMessage
			} finally {
				IOUtils.closeQuietly(fos)
			}
		
		} else if(cmd == CMD_PUT) {
		
			File srcFile = new File(options.f)
			
			if(!srcFile.exists()) error("Local file does not exist: ${srcFile.absolutePath}")
			if(!srcFile.isFile()) error("Local file path is not a file: ${srcFile.absolutePath}")
			
			String n = srcFile.name
			
			println "Uploading file remote file ${srcFile.absolutePath}${commons ? ' [commons]' : ''}"
			
			boolean overwrite = options.ow ? true : false
			
			VITAL_Node node = null
			if(commons) {
				node = getCommonsFile(adminService, n)
			} else {
				node = getFile(service, n)
			}
			
			if(node != null) {
				
				if(!overwrite) error "Remote file already exists: ${n}, use --overwrite option"

				if(node.active) error("Remote file in use: ${n}")
				
				println "Remote file exists, overwriting ..."
				
			}
			
			BufferedInputStream inputStream = null
			
			try {
				
				inputStream = new BufferedInputStream(new FileInputStream(srcFile))
				
				VitalStatus status = null
				if(commons) {
					status = adminService.uploadFile(VitalApp.withId('commons'), URIProperty.withString(FTP_URI), n, inputStream, overwrite)
				} else {
					status = service.uploadFile(URIProperty.withString(FTP_URI), n, inputStream, overwrite)
				}
				
				println "Status: ${status}"
			} catch(Exception e) {
				error e.localizedMessage
			} finally {
				IOUtils.closeQuietly(inputStream)
			}
			
		} else if(cmd == CMD_DEL) {
		
			String n = options.n
			
			println "Deleting remote file ${n}${commons ? ' [commons]' : ''}"
		
			VITAL_Node node = null
			if(commons) {
				node = getCommonsFile(adminService, n)
			} else {
				node = getFile(service, n)
			}
			if(node == null) error("File not found: ${n}")
			
			if(node.active) {
				error "File in use: ${n}"
			}
		
			try {
				VitalStatus status = null
				if(commons) {
					status = adminService.deleteFile(VitalApp.withId('commons'), URIProperty.withString(FTP_URI), n)
				} else {
					status = service.deleteFile(URIProperty.withString(FTP_URI), n)
				}
				println "Status: ${status}"
			} catch(Exception e) {
				error e.localizedMessage
			} 
			
		} else if(cmd == CMD_PURGE) {
		
			println "Purging all files not in use ..."
			
			ResultList res = null
			try {
				res = service.callFunction("commons/scripts/VitalFTP_PurgeFiles.groovy", [:])
				if(res.status.status != VitalStatus.Status.ok) {
					error res.status.message
				}
				println "${res.status.message}"
				
			} catch(Exception e) {
				error "Couldn't purge files: ${e.localizedMessage}"
			}
			
		} else {
		
			error "Unhandled command: ${cmd}"
		
		}		
		
	}
	
	static VITAL_Node getFile(VitalService service, String n) {
		
		ResultList res = null
		try {
			res = service.callFunction("commons/scripts/VitalFTP_ListFiles.groovy", [name: n])
			if(res.status.status != VitalStatus.Status.ok) {
				error res.status.message
			}
		} catch(Exception e) {
			error "Couldn't list files: ${e.localizedMessage}"
		}

		VITAL_Node node = res.results.size() > 0 ? (VITAL_Node) res.results[0].graphObject : null

		return node
		
	}
	
	static VITAL_Node getCommonsFile(VitalServiceAdmin adminService, String n) {
		
		ResultList res = null
		try {
			res = adminService.callFunction(VitalApp.withId('commons'), "commons/scripts/VitalFTP_ListFiles.groovy", [name: n])
			if(res.status.status != VitalStatus.Status.ok) {
				error res.status.message
			}
		} catch(Exception e) {
			error "Couldn't list files: ${e.localizedMessage}"
		}

		VITAL_Node node = res.results.size() > 0 ? (VITAL_Node) res.results[0].graphObject : null

		return node
		
	}
	
}
