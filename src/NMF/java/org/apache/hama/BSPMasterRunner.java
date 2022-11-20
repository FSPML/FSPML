/**
 * Termite-beta-0.1 
 */
package org.apache.hama;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hama.bsp.BSPMaster;
import org.apache.hama.myhama.util.VersionInfo;

/**
 * This class starts and runs the BSPMaster.
 */
public class BSPMasterRunner extends Configured implements Tool {

	public static final Log LOG = LogFactory.getLog(BSPMasterRunner.class);
	
	/**
	 * StartupShutdownPretreatment
	 * 
	 * This class is used to do some prepare work before starting BSPController
	 * and cleanup after shutting down BSPController.
	 * 
	 * @author root
	 * @version 0.1
	 */
	public class StartupShutdownPretreatment extends Thread {

		private final Log LOG;
		private final String hostName;
		private final String className;
		private BSPMaster master;

		public StartupShutdownPretreatment(Class<?> clazz,
				final org.apache.commons.logging.Log LOG) {
			this.LOG = LOG;
			this.hostName = getHostname();
			this.className = clazz.getSimpleName();

			this.LOG.info(toStartupShutdownMessage("STARTUP_MSG: ", new String[] {
					"Starting " + this.className, "  host = " + this.hostName,
					"  version = " + VersionInfo.getVersionInfo(),
					"  source = " + VersionInfo.getSourceCodeInfo(),
					"  compiler = " + VersionInfo.getCompilerInfo(),
					"  workplace = " + VersionInfo.getWorkPlaceInfo()}));
		}

		public void setHandler(BSPMaster master) {
			this.master = master;
		}

		@Override
		public void run() {
			try {
				this.master.shutdown();

				this.LOG.info(toStartupShutdownMessage("SHUTDOWN_MSG: ",
						new String[] { "Shutting down " + this.className + " at "
								+ this.hostName }));
			} catch (Exception e) {
				this.LOG.error("Fail to SHUTDOWN", e);
			}
		}

		private String getHostname() {
			try {
				return "" + InetAddress.getLocalHost();
			} catch (UnknownHostException uhe) {
				return "" + uhe;
			}
		}

		private String toStartupShutdownMessage(String prefix, String[] msg) {
			StringBuffer b = new StringBuffer(prefix);

			b.append("\n/************************************************************");
			for (String s : msg) {
				b.append("\n" + prefix + s);
			}
			b.append("\n************************************************************/");

			return b.toString();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		StartupShutdownPretreatment pretreatment = 
            new StartupShutdownPretreatment(BSPMaster.class, LOG);

		if (args.length != 0) {
			System.out.println("usage: BSPMasterRunner");
			System.exit(-1);
		}

		try {
			HamaConfiguration conf = new HamaConfiguration(getConf());
			BSPMaster master = BSPMaster.startMaster(conf);
			pretreatment.setHandler(master);
            Runtime.getRuntime().addShutdownHook(pretreatment);
			master.offerService();
		} catch (Throwable e) {
			LOG.fatal(StringUtils.stringifyException(e));
			return -1;
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new BSPMasterRunner(), args);
		System.exit(exitCode);
	}
}
