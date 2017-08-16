package org.plugbase.analysis;
import static org.plugbase.analysis.config.Configuration.DO_FINSH;
import static org.plugbase.analysis.config.Configuration.DO_REMOVE;
import static org.plugbase.analysis.config.Configuration.DO_WRITE;
import static org.plugbase.analysis.config.Configuration.FLUX_LOG;
import static org.plugbase.analysis.config.Configuration.INPUT_PATH;
import static org.plugbase.analysis.config.Configuration.ROOT_PATH;

import java.util.List;

import org.plugbase.analysis.collect.CollectFluxLog;
import org.plugbase.analysis.config.Configuration;
import org.plugbase.analysis.util.HadoopFileSystem;

public class BootStrap2 {
	
	public static void main(String[] args) {
		Configuration.setDebug(true);
		Configuration.setAppName("CDN WriteFluxLog");
		
		String hdfsLogPath			= INPUT_PATH + "/*.finsh";
		String hdfsLogSavePath		= ROOT_PATH + "/" + FLUX_LOG + "/";
		
		// 批量修改将要汇总的文件名
		List<String> filelist = HadoopFileSystem.replaceAllFileName(hdfsLogPath, DO_FINSH, DO_WRITE);
		
		if(filelist == null || filelist.isEmpty()) return;
		
		hdfsLogPath = String.join(",", filelist);
		
		CollectFluxLog.collect(hdfsLogPath, hdfsLogSavePath);
		
		HadoopFileSystem.replaceAllFileName(hdfsLogPath, DO_WRITE, DO_REMOVE);
	}
	
}
