package org.plugbase.analysis.func.format;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

public class MyErrDomainTextOutputFormat extends MultipleTextOutputFormat<String, String>{
	protected String generateFileNameForKeyValue(String key, String value, String name) {
		return "domain";
	}
	
	@Override
	protected String generateActualKey(String key, String value) {
		return null;
	}
	
	@Override
	public void checkOutputSpecs(FileSystem ignored, JobConf job)
			throws FileAlreadyExistsException, InvalidJobConfException,
			IOException {
		
	    Path outDir = getOutputPath(job);
	    if (outDir == null && job.getNumReduceTasks() != 0) {
	      throw new InvalidJobConfException("Output directory not set in JobConf.");
	    }
	    if (outDir != null) {
	      FileSystem fs = outDir.getFileSystem(job);
	      // normalize the output directory
	      outDir = fs.makeQualified(outDir);
	      setOutputPath(job, outDir);
	      
	      // get delegation token for the outDir's file system
	      TokenCache.obtainTokensForNamenodes(job.getCredentials(), 
	                                          new Path[] {outDir}, job);
	      
	      // check its existence
	      if (fs.exists(outDir)) {
	        //throw new FileAlreadyExistsException("Output directory " + outDir + " already exists");
	      }
	    }
	}
}
