package org.apache.hama.myhama.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hama.bsp.BSPJobID;

public class JobLog {
	
	private static final File LOG_DIR = new File(
			System.getProperty("termite.log.dir"), "joblogs").getAbsoluteFile();
	
	static {
		if (!LOG_DIR.exists()) {
			LOG_DIR.mkdirs();
		}
	}
	
	private String jobId;
	private BufferedWriter bw;
	private SimpleDateFormat sdf;
	
	public JobLog(BSPJobID jobId) {
		try {
			this.jobId = jobId.toString();
			this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			File file = new File(LOG_DIR, this.jobId);
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fw = new FileWriter(file, true);
			bw = new BufferedWriter(fw);
		} catch (IOException ioe) {
			// TODO
		}
	}
	
	public void close() {
		try {
			bw.close();
		} catch (IOException ioe) {
			// TODO Auto-generated catch block
		}
	}
	
	public void debug(String debug) {
		try {
			bw.write(sdf.format(new Date()) + ":" + debug);
			bw.newLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
	}
	
	public void info(String info) {
		try {
			bw.write(sdf.format(new Date()) + ":" + info);
			bw.newLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
	}
	
	public void error(String error, Exception e) {
		try {
			bw.write(sdf.format(new Date()) + ":" + error);
			bw.newLine();
			bw.write(e.toString());
			bw.newLine();
			bw.flush();
		} catch (IOException ioe) {
			// TODO Auto-generated catch block
		}
	}
	
	public void error(String error) {
		try {
			bw.write(sdf.format(new Date()) + ":" + error);
			bw.newLine();
			bw.flush();
		} catch (IOException ioe) {
			// TODO Auto-generated catch block
		}
	}
	
	public void fatal(String fatal) {
		try {
			bw.write(sdf.format(new Date()) + ":" + fatal);
			bw.newLine();
			bw.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
	}
}
