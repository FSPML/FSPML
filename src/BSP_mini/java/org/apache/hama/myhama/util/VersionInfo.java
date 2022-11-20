package org.apache.hama.myhama.util;

/**
 * Termite version information.
 * 
 * A version information class.
 * 
 * @author zhigang wang
 * @version 0.1
 */
public class VersionInfo {

    public static String getVersionInfo() {
        return "beta-0.1";
    }
    
    public static String getSourceCodeInfo() {
        return "https://github.com/HybridGraph";
    }
    
    public static String getCompilerInfo() {
        return "zhigang wang on " + getCompilerDateInfo();
    }
    
    public static String getWorkPlaceInfo() {
    	return "Northeastern University, China";
    }
    
    private static String getCompilerDateInfo() {
        return "12/23/2015";
    }
}
