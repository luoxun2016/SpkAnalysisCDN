package org.plugbase.analysis.util;




public class SpiderUtil {
	
	public static final int OTHER = 0x0 ;
	public static final int BAIDU = 0x1 ;
	public static final int GOOGLE = 0x2 ;
	public static final int YAHOO = 0x3 ;
	public static final int SOGOU = 0x4 ;
	public static final int YODAO = 0x5 ;
	public static final int YOUDAO = 0x6 ;
	public static final int QIHU = 0x7 ;
	public static final int SOSO = 0x8 ;
	public static final int BING = 0x9 ;
	
	
	private static String[] addrs = {
		"baiduspider",//baidu
		"googlebot",//google
		"slurp",//yahoo
		"sogou",//sogou
		"yodaobot",//yodao
		"youdaobot",//youdao
		"qihoobot",//qihu
		"sosospider",//soso
		"bingbot"//bing
	};
	
	private static int[] types = {
		BAIDU,
		GOOGLE,
		YAHOO,
		SOGOU,
		YODAO,
		YOUDAO,
		QIHU,
		SOSO,
		BING
	};
	public static int getSpiderType(String spiderstr){
		if(spiderstr == null || spiderstr.isEmpty()) return OTHER;
		spiderstr = spiderstr.toLowerCase();
		for(int i=0;i<addrs.length;i++){
			if( spiderstr.contains(addrs[i])) return types[i] ;
		}
		return OTHER ;
	}
}
