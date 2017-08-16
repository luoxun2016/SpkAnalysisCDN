package org.plugbase.analysis.func.format;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

public class MyFluxLogTextOutputFormat extends MultipleTextOutputFormat<String, String>{
	protected String generateFileNameForKeyValue(String key, String value, String name) {
		return key;
	}
	
	@Override
	protected String generateActualKey(String key, String value) {
		return null;
	}
	
	@Override
	public RecordWriter<String, String> getRecordWriter(FileSystem fs,
			JobConf job, String name, Progressable arg3) throws IOException {
	    final FileSystem myFS = fs;
	    final String myName = generateLeafFileName(name);
	    final JobConf myJob = job;
	    final Progressable myProgressable = arg3;

	    return new RecordWriter<String, String>() {

	      // a cache storing the record writers for different output files.
	      TreeMap<String, RecordWriter<String, String>> recordWriters = new TreeMap<String, RecordWriter<String, String>>();

	      public void write(String key, String value) throws IOException {

	        // get the file name based on the key
	        String keyBasedPath = generateFileNameForKeyValue(key, value, myName);

	        // get the file name based on the input file name
	        String finalPath = getInputFileBasedOutputFileName(myJob, keyBasedPath);

	        // get the actual key
	        String actualKey = generateActualKey(key, value);
	        String actualValue = generateActualValue(key, value);

	        RecordWriter<String, String> rw = this.recordWriters.get(finalPath);
	        if (rw == null) {
	          // if we don't have the record writer yet for the final path, create
	          // one
	          // and add it to the cache
	          rw = getBaseRecordWriter(myFS, myJob, finalPath, myProgressable);
	          this.recordWriters.put(finalPath, rw);
	        }
	        rw.write(actualKey, actualValue);
	      };

	      public void close(Reporter reporter) throws IOException {
	        Iterator<String> keys = this.recordWriters.keySet().iterator();
	        while (keys.hasNext()) {
	          RecordWriter<String, String> rw = this.recordWriters.get(keys.next());
	          rw.close(reporter);
	        }
	        this.recordWriters.clear();
	      };
	    };
	}
	
	  protected static class LineRecordWriter<K, V>
	    implements RecordWriter<K, V> {
	    private static final String utf8 = "UTF-8";
	    private static final byte[] newline;
	    static {
	      try {
	        newline = "\n".getBytes(utf8);
	      } catch (UnsupportedEncodingException uee) {
	        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
	      }
	    }

	    protected DataOutputStream out;
	    private final byte[] keyValueSeparator;

	    public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
	      this.out = out;
	      try {
	        this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
	      } catch (UnsupportedEncodingException uee) {
	        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
	      }
	    }

	    public LineRecordWriter(DataOutputStream out) {
	      this(out, "\t");
	    }

	    /**
	     * Write the object to the byte stream, handling Text as a special
	     * case.
	     * @param o the object to print
	     * @throws IOException if the write throws, we pass it on
	     */
	    private void writeObject(Object o) throws IOException {
	      if (o instanceof Text) {
	        Text to = (Text) o;
	        out.write(to.getBytes(), 0, to.getLength());
	      } else {
	        out.write(o.toString().getBytes(utf8));
	      }
	    }

	    public synchronized void write(K key, V value)
	      throws IOException {

	      boolean nullKey = key == null || key instanceof NullWritable;
	      boolean nullValue = value == null || value instanceof NullWritable;
	      if (nullKey && nullValue) {
	        return;
	      }
	      if (!nullKey) {
	        writeObject(key);
	      }
	      if (!(nullKey || nullValue)) {
	        out.write(keyValueSeparator);
	      }
	      if (!nullValue) {
	        writeObject(value);
	      }
	      out.write(newline);
	    }

	    public synchronized void close(Reporter reporter) throws IOException {
	      out.close();
	    }
	  }
	
	public RecordWriter<String, String> getBaseRecordWriter(FileSystem ignored,
              JobConf job,
              String name,
              Progressable progress)
		throws IOException {
		boolean isCompressed = getCompressOutput(job);
		String keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator", "\t");
		if (!isCompressed) {
			Path file = FileOutputFormat.getTaskOutputPath(job, name);
			FileSystem fs = file.getFileSystem(job);
			if(!fs.exists(file)){
				fs.createNewFile(file);
			}
			FSDataOutputStream fileOut = fs.append(file, fs.getConf().getInt("io.file.buffer.size", 4096), progress);
			return new LineRecordWriter<String, String>(fileOut, keyValueSeparator);
		} else {
			Class<? extends CompressionCodec> codecClass =
			getOutputCompressorClass(job, GzipCodec.class);
			// create the named codec
			CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
			// build the filename including the extension
			Path file = FileOutputFormat.getTaskOutputPath(job, name + codec.getDefaultExtension());
			FileSystem fs = file.getFileSystem(job);
			if(!fs.exists(file)){
				fs.createNewFile(file);
			}
			FSDataOutputStream fileOut = fs.append(file, fs.getConf().getInt("io.file.buffer.size", 4096), progress);
			return new LineRecordWriter<String, String>(new DataOutputStream
			    (codec.createOutputStream(fileOut)),
			    keyValueSeparator);
		}
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