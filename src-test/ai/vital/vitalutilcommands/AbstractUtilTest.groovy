package ai.vital.vitalutilcommands

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import junit.framework.TestCase

class AbstractUtilTest extends TestCase {

	File tempVitalHome
	
	@Override
	protected void setUp() throws Exception {
		
		super.setUp()
		
//		tempVitalHome = Files.createTempDirectory("vitalhome").toFile()
		
//		File vitalConf = new File(tempVitalHome, "")
//		File vitalServiceConf = new File(tempVitalHome, "vitalservi")
		
	}
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown()
		FileUtils.deleteQuietly(tempVitalHome)
	}
	
	//http://stackoverflow.com/questions/318239/how-do-i-set-environment-variables-from-java
	protected static void setEnv(Map<String, String> newenv)
	{
	  try
		{
			Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
			Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
			theEnvironmentField.setAccessible(true);
			Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
			env.putAll(newenv);
			Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
			theCaseInsensitiveEnvironmentField.setAccessible(true);
			Map<String, String> cienv = (Map<String, String>)     theCaseInsensitiveEnvironmentField.get(null);
			cienv.putAll(newenv);
		}
		catch (NoSuchFieldException e)
		{
		  try {
			Class[] classes = Collections.class.getDeclaredClasses();
			Map<String, String> env = System.getenv();
			for(Class cl : classes) {
				if("java.util.Collections\$UnmodifiableMap".equals(cl.getName())) {
					Field field = cl.getDeclaredField("m");
					field.setAccessible(true);
					Object obj = field.get(env);
					Map<String, String> map = (Map<String, String>) obj;
					map.clear();
					map.putAll(newenv);
				}
			}
		  } catch (Exception e2) {
			e2.printStackTrace();
		  }
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
	
}
