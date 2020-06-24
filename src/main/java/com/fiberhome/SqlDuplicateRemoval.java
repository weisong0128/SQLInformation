package com.fiberhome;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlDuplicateRemoval {
	private static final String SEPARATOR = String.valueOf((char)0x13);
	public static Boolean testLogOpen = false;
	
	/**
	 * 对输入的字符串str中类似 xxx as field的语法进行替换
	 * (?i)不区分大小写
	 * @param str
	 * @return
	 */
	public static String rpByRegex1(String str) {
		String regex = "\\s+\\d*[0-9] (?i)as";
		Pattern pat = Pattern.compile(regex);
		Matcher matcher = pat.matcher(str);
		while(matcher.find()) {
			if(testLogOpen) {
				System.out.println("1-group:" + matcher.group());
			}
			str = str.replace(matcher.group()," *** AS");
		}
		return str;
	}

	/**
	 * 对输入的字符串str中类似 有''的内容进行替换
	 * @param str
	 * @return
	 */
//	public static String rpByRegex2(String str) {
//		String regex = "(?<=').*?(?=')";
//		Pattern pat = Pattern.compile(regex);
//		Matcher matcher = pat.matcher(str);
//		while(matcher.find()) {
//			//System.out.println("2group:" + matcher.group());
//			String cstr = matcher.group().toUpperCase();
//			if(cstr.contains("AND") || cstr.contains("LIKE") || cstr.contains("IN")) {
//				continue;
//			} else {
//				str = str.replace("'" + matcher.group() + "'","'***'");
//			}
//		}
//		return str;
//	}
	
	/**
	 * 匹配 match（***）场景
	 * @param str
	 * @return
	 */
	public static String rpByRegex3(String str,String match) {
		String regex = "(?i)"+match + "\\s*.*?\\(.*?\\)\\s*";
		Pattern pat = Pattern.compile(regex);
		Matcher matcher = pat.matcher(str);
		while(matcher.find()) {
			if(testLogOpen) {
				System.out.println(match + "3-group:" + matcher.group());
			}
			str = str.replace(matcher.group(),match+"(***)");
		}
		return str;
	}
	
	/**
	 * 匹配 match *** 场景 /OVERWRITE,INSERT INTO
	 * @param str
	 * @param match
	 * @return
	 */
	public static String rpByRegex4(String str,String match) {
		String regex = "(?i)"+match + "\\s+.*?\\s+";
		Pattern pat = Pattern.compile(regex);
		Matcher matcher = pat.matcher(str);
		while(matcher.find()) {
			if(testLogOpen) {
				System.out.println(match + "4-group:" + matcher.group());
			}
			str = str.replace(matcher.group(),match+" *** ");
		}
		return str;
	}
	
	/**
	 * 匹配 BETWEEN AND
	 * @param str
	 * @return
	 */
	public static String rpByRegex5(String str) {
		String regex = "(?i)between\\s+.*? (?i)and\\s+.*? ";
		Pattern pat = Pattern.compile(regex);
		Matcher matcher = pat.matcher(str);
		while(matcher.find()) {
			if(testLogOpen) {
				System.out.println("5-group:" + matcher.group());
			}
			str = str.replace(matcher.group(),"BETWEEN *** AND *** ");
		}
		return str;
	}
	
	/**
	 * 匹配 LIKE ' '
	 * @param str
	 * @return
	 */
	public static String rpByRegex6(String str) {
		String regex = "(?i)like\\s+'.*?' ";
		Pattern pat = Pattern.compile(regex);
		Matcher matcher = pat.matcher(str);
		while(matcher.find()) {
			if(testLogOpen) {
				System.out.println("6-group:" + matcher.group());
			}
			str = str.replace(matcher.group(),"LIKE *** ");
		}
		return str;
	}
	
	/**
	 * 匹配 = > < >= <=
	 * \\s* 0个或多个空格
	 * \\s+ 1个或以上空格
	 * @param str
	 * @return
	 */
	public static String rpByRegex7(String str,String tmp) {
		//String regex = "//s+(=|>|<|>=|<=|<>)//s+.*?(//s+|\\))";
		String regex = "\\s*"+tmp+"(\\s*(.*?)\\s+)";
		Pattern pat = Pattern.compile(regex);
		Matcher matcher = pat.matcher(str);
		while(matcher.find()) {
			if(testLogOpen) {
				System.out.println("7-group:" + matcher.group());
			}
			str = str.replace(matcher.group(),tmp+"*** ");
		}
		return str;
	}
	
	/**
	 * 匹配 IN场景
	 * @param str
	 * @return
	 */
	public static String rpByRegex8(String str) {
		String regex = " (?i)in\\s+\\(.*?\\)\\s*";
		Pattern pat = Pattern.compile(regex);
		Matcher matcher = pat.matcher(str);
		while(matcher.find()) {
			if(testLogOpen) {
				System.out.println("3-group:" + matcher.group());
			}
			str = str.replace(matcher.group()," IN (***) ");
		}
		return str;
	}

	/**
	 * LOAD场景
	 * @param str
	 * @return
	 */
	public static String rpByRegex9(String str,String match) {
		if (str.startsWith("load")||str.startsWith("LOAD")){
			String regex = match+"\\s*.*?\",";
			Pattern pat = Pattern.compile(regex);
			Matcher matcher = pat.matcher(str);
			while(matcher.find()) {
				if(testLogOpen) {
					System.out.println("9-group:" + matcher.group());
				}
				str = str.replace(matcher.group(),match+" \"***\",");
			}
		}
		return str;
	}

	/**
	 * 匹配 LIMIT场景
	 * @param str
	 * @return
	 */
	public static String rpByRegex10(String str) {
		String regex = " (?i)limit\\s+\\d+.*\\s*";
		Pattern pat = Pattern.compile(regex);
		Matcher matcher = pat.matcher(str);
		while(matcher.find()) {
			if(testLogOpen) {
				System.out.println("10-group:" + matcher.group());
			}
			str = str.replace(matcher.group()," LIMIT *** ");
		}
		return str;
	}

	/**
	 * 对Str进行正则表达式匹配和过滤
	 * @param str
	 * @return
	 */
	public static String rpByRegex(String str) {
		str += " ";
		str = rpByRegex1(str);
		str = rpByRegex3(str,"CLLOCATION");
		str = rpByRegex3(str,"CL_MAKE_LUCERNE_INDEX");
		str = rpByRegex3(str,"CL_MERGER_LUCENE_INDEX");	
		str = rpByRegex4(str,"INSERT INTO");
		str = rpByRegex4(str,"OVERWRITE");
		str = rpByRegex5(str);
		str = rpByRegex6(str);
		str = rpByRegex7(str,"=");
		str = rpByRegex7(str,">");
		str = rpByRegex7(str,"<");
		str = rpByRegex7(str,">=");
		str = rpByRegex7(str,"<=");
		str = rpByRegex7(str,"!=");
		str = rpByRegex7(str,"<>");
		str = rpByRegex8(str);
		str = rpByRegex9(str," f\",");
		str = rpByRegex9(str," p\",");
		str = rpByRegex10(str);
		return str.replaceAll(" \\( ", "\\(").replaceAll(" \\) ", "\\)");
	}
	
	/**
	 * 对输入的字符串str中的where条件中的内容进行***替换
	 * @param str
	 * @return
	 */
	public static String doStrReplace(String str) {
		if(str ==null || str.trim().isEmpty()) {
			return null;
		} else {
			str = str.replaceAll("\\(", " \\( ").replaceAll("\\)", " \\) ");
			return rpByRegex(str);
		}
	}
	
	//对待处理的文件或文件夹进行sql去重处理
	public static void doSqlFileDuplicateRemoval(File file) {
//		Map<String,Integer> sqlMap = new liHashMap<>();
		Map<String,String> sqlMap = new LinkedHashMap<>();
		if(file.isDirectory()) {
			//如果文件是文件目录，需要遍历目录下所有的文件
			System.out.println("输入的文件是目录，暂不支持对目录的遍历处理");
		} else {
			//如果文件是文件，则直接处理文件
			try (InputStreamReader isr = new InputStreamReader(new FileInputStream(file),"utf-8");
				 BufferedReader bf = new BufferedReader(isr)){
				String lineStr;
				String line;
				String repStr;
				while((lineStr = bf.readLine()) != null) {
					line = lineStr.split(SEPARATOR)[2];
					repStr = doStrReplace(line);
					if(null != repStr) {
						/*if(sqlMap.containsKey(repStr)) {
							int num = sqlMap.get(repStr) + 1;
							sqlMap.put(repStr, num);//记录sql的重复次数
 						} else {
							sqlMap.put(repStr, 1);
						}*/
						sqlMap.put(lineStr,repStr);
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			doFileWrite(sqlMap,file);
		}
	}
	
	/**
	 * 把遍历后的sql写入到filename文件中,文件名是在原文件名后面加后置.replace
	 * @param sqlMap
	 */
	public static void doFileWrite(Map<String,String> sqlMap,File file)  {
		if(!sqlMap.isEmpty()) {
			File newFile = new File(file.getAbsolutePath()+".replace");
			if(newFile.exists()) {
				newFile.delete();
			}
			try (FileOutputStream out = new FileOutputStream(newFile,true)){
				Iterator<String> it = sqlMap.keySet().iterator();
				while(it.hasNext()) {
					String keyValue = (String) it.next();
					StringBuffer sb = new StringBuffer();
					sb.append(keyValue + SEPARATOR + sqlMap.get(keyValue) + "\r\n");
					out.write(sb.toString().getBytes("utf-8"));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws FileNotFoundException {
		if(null == args[0]) {
			throw new FileNotFoundException();
		}
		System.out.println(args[0]);
		File file = new File(args[0]);
		if(!file.exists()) {
			//如果文件不存在，抛异常
			throw new FileNotFoundException();
		} else {
			doSqlFileDuplicateRemoval(file);
		}

	}

	/*public static void main(String[] args) {
		SqlDuplicateRemoval.testLogOpen = true;
		String testStr = "select mapping_name as FH_TABLE_NAME, mapping_content as FH_SOURCE_STRUCTED,FH_SOURCE_y_wildcard4_sm as FH_SOURCE_UNSTRUCTED,FH_TEXT_y_wildcard4_s as FH_TEXT,FH_ATTACHTEXT_y_wildcard4_s as FH_ATTACHTEXT, content_y_text_hanlp_ismp as FH_QUERY_CONTENT from PHYSICAL_TABLE_BIGDATA where mapping_name in ('NB_TAB_HTTP','NB_TAB_WEBCHAT','NB_TAB_WEBSHARE','NB_TAB_BLOG','NB_TAB_WEBBBS','NB_TAB_MULTIMEDIAIMAGE','NB_TAB_MBLOG') and partition in (20200525,20200524,20200523,20200522) and TEXTTYPE_LABEL_y_string_i like '%label%' limit 500 , 10" +
				"" ;
		System.out.println(testStr);
		testStr = testStr.replaceAll("\\(", " \\( ").replaceAll("\\)", " \\) ").replaceAll(",", ", ");
		System.out.println(SqlDuplicateRemoval.rpByRegex(testStr));
	}*/
}
