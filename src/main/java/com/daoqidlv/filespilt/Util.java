package com.daoqidlv.filespilt;

import java.io.File;

public class Util {
	
	/**
	 * 判断是否存在null或者空字符串
	 * 
	 * @param params
	 * @return
	 */
	public static boolean isNull(String... params) {
		if (params == null)
			return true;

		for (String param : params) {
			if (param == null || "".equals(param.trim()))
				return true;
		}

		return false;
	}
	
	/**
	 * 判断是否是null对象
	 * 
	 * @param params
	 * @return
	 */
	public static boolean isNull(Object... params) {
		if (params == null)
			return true;

		for (Object param : params) {
			if (param == null)
				return true;
		}

		return false;
	}
	
	public static String genFullFileName(String fileDir, String fileName) {
		
		if(fileDir.endsWith(File.separator)) {
			return fileDir+fileName;
		} else {
			return fileDir + File.separator + fileName;
		}
	}

}
