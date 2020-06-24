package com.fiberhome;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author wcc
 * @Date 2020/5/29 8:59
 */
public class SqlFieldsExtraction {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlFieldsExtraction.class);

    private static final String SEPARATOR = String.valueOf((char)0x13); //16进制分隔符
    private static final String FIELD = "showField";
    private static final String PARTITION = "partition";
    private static final String MAPPING_NAME = "mapping_name";
    private static final String EQUAL = "equal";
    private static final String LIKE = "like";
    private static final String RANGE = "range";
    private static final String GROUP = "group";
    private static final String SORT = "sort";
    private static final String LIMIT = "limit";
    private static final String FUNCTION = "function";
    private static final String JOIN = "join";
    private static final String CHARSET = "utf-8";

    public static void main(String[] args) throws FileNotFoundException {
       /*String sql = "with label_src as (select MULTIMEDIA_ID,TAGSLABEL from ZDR_MEDIA_RESULT_PIC where syskv='sys.multival.join:spacejson' and partition in ('2019') group by MULTIMEDIA_ID,TAGSLABEL )," +
                " label as (select case when MULTIMEDIA_ID = '' then round(rand(100)*10000000) else MULTIMEDIA_ID end MULTIMEDIA_ID,TAGSLABEL from label_src), " +
                " pic as (SELECT DOC_ID,FILEMD5,MAINFILE,FLAG,CAPTURE_TIME,FILESIZE FROM ZDR_PICTURE WHERE ENTITY_ID in ('1110021') AND FILEMD5<>'' AND partition in ('2019')), " +
                " ptp as (select DOC_ID,FILEMD5,MAINFILE,FLAG,CAPTURE_TIME,FILESIZE,MULTIMEDIA_ID,TAGSLABEL from pic left join label on pic.DOC_ID = label.MULTIMEDIA_ID), " +
                " rowdata as (select FILEMD5,MAINFILE,FLAG,CAPTURE_TIME,FILESIZE,MULTIMEDIA_ID,TAGSLABEL,cnt,row_number() over(order by cnt desc, CAPTURE_TIME desc) as rownum from (select FILEMD5,MAINFILE,FLAG,CAPTURE_TIME,FILESIZE,MULTIMEDIA_ID,TAGSLABEL,count(*) over(partition by FILEMD5) as cnt, row_number() over(partition by FILEMD5 ORDER BY CAPTURE_TIME desc ) as rn from ptp)t where t.rn=1)" +
                " select count(*) as count from rowdata limit 1";*/

//       File file = new File(args[0]);
//       LOGGER.info("FileName: {}",args[0]);
        File file = new File("test01");
       long beginTime = System.currentTimeMillis();
       if (!file.exists()){
           throw new FileNotFoundException("input file doesn't exists");
       } else {
           doSqlFileAnalyze(file);
       }
       long endTime = System.currentTimeMillis();
       LOGGER.info("耗时: {}",(endTime-beginTime)+"ms");

        /*if (sql.startsWith("RamIndexFilter")){
            return;
        }*/
//        sqlAnalyze(sql,0,null,null);
    }

    /**
     * 按行处理读入的文件
     * @param file
     */
    private static void doSqlFileAnalyze(File file) {
        //解析后每行内容
        StringBuilder content = new StringBuilder(1024);
        try (InputStreamReader isr = new InputStreamReader(new FileInputStream(file),CHARSET);
             BufferedReader br = new BufferedReader(isr)){
            //读入文件中每行的内容
            String lineStr;
            //时间
            long time = 0;
            //毫秒数
            String millisecond;
            //标识
            String identifier;
            //每行的sql
            String line;
            String str;
            while ((lineStr=br.readLine()) != null){
                //读取一行获取这一行的分隔符的个数
                int count = 0;
                //正则表达式Pattern、Matcher类
                Pattern p = Pattern.compile(SEPARATOR);     //以 SEPARATOR 作为分隔符
                Matcher m = p.matcher(lineStr); // Matcher类提供对正则表达式的分组支持，以及对正则表达式的多次匹配支持
                while(m.find()) {   //find():对字符串(0x13)进行匹配，匹配到的字符串可以在任何位置，0x13可以匹配到8次
                    count++;
                }
                //文件的标准格式中一行应有8个分割符,若某行分隔符格式不为8,跳过此行
                if(count != 8) {
                    continue;
                }
                time = Long.parseLong(lineStr.split(SEPARATOR)[0].trim());  //得到时间戳字段值
                millisecond = lineStr.split(SEPARATOR)[6];  //得到耗时字段值
                identifier = lineStr.split(SEPARATOR)[7];   //得到业务标识
                line = lineStr.split(SEPARATOR)[2] .trim(); //得到sql语句
                //过滤掉文件中包含的CREATE |INSERT |EXPORT |DELETE等语句
                String regexStr = "CREATE |INSERT |EXPORT |DELETE |RamIndexFilter";
                Pattern pattern = Pattern.compile(regexStr);
                Matcher matcher = pattern.matcher(line);
                if(!matcher.find()) {   //如果sql语句不是上述regexStr类型
                    if (line.contains(" from ") || line.contains(" FROM ")){    //判断是否为select语句？
                        str = sqlAnalyze(line, time, millisecond, identifier);
                        content.append(str);
                    }
                }
            }

        } catch (IOException e) {
            LOGGER.error("Read Exception: {} ",e);
        }
        doFileWrite(content.toString(),file);
    }

    /**
     * 将处理后的结果写入到文件中
     * @param content
     * @param file
     */
    private static void doFileWrite(String content,File file) {
        File result= new File(file.getAbsolutePath()+".result");
        if (result.exists()){
            result.delete();
        }
        writeFile(content, result);
    }

    /**
     * 写数据到文件
     */
    private static void writeFile(String content, File file) {
        try (OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(file,true),CHARSET)){
            out.write(content);
            out.flush();
        } catch (IOException e) {
            LOGGER.error("Write File Exception: {} ",e);
        }
    }

    /**
     * 分析每一条sql语句
     * @param sql
     * @param time
     * @param millisecond
     * @param identifier
     * @return
     */
    private static String sqlAnalyze(String sql, long time, String millisecond, String identifier) {
        StringBuilder builder = new StringBuilder();
        try {
           //使用jsqlparser解析sql语句
           Statement stmt = CCJSqlParserUtil.parse(new StringReader(sql));
           if(stmt instanceof Select) {
               TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
               List<String> tableList =tablesNamesFinder.getTableList(stmt);    //得到表名对象tableList
               //返回第一行结果：table + 分隔符 + 时间戳 + 分隔符 + " " + 分隔符 + 表名 + 分隔符 + 耗时 + 分隔符 + 业务标识
               builder.append("table" + SEPARATOR + time + SEPARATOR +" "+ SEPARATOR + tableList.get(0) + SEPARATOR + millisecond + SEPARATOR + identifier + "\r\n");

               Select select = (Select) stmt;
               //获取with子句列表
               List<WithItem> withItems = select.getWithItemsList();
               //判断withItems是否为空,若不为空则获取with字句的查询语句并验证.
               if (withItems != null) {
                   for (WithItem withItem : withItems) {
                       SelectBody withSelectBody = withItem.getSelectBody();
                       String str = analyzeFromItem(withSelectBody, time, millisecond, identifier);
                       builder.append(str);
                   }
               }

               //sql的主查询体
               SelectBody selectBody = select.getSelectBody();
               //若有UNION操作,解析union的每一条语句
               if (selectBody instanceof SetOperationList) {
                   List<SelectBody> selectBodyList = ((SetOperationList) selectBody).getSelects();
                   for (SelectBody body : selectBodyList){
                       String str  = analyzeFromItem(body, time, millisecond, identifier);
                       builder.append(str);
                   }
               }else {
                   String str  = analyzeFromItem(selectBody, time, millisecond, identifier);
                   builder.append(str);
               }
           }
        } catch (JSQLParserException e) {
            LOGGER.error("JSQLParserException: {}",e.getCause().getMessage());
            try {
                String jarpath = URLDecoder.decode(SqlFieldsExtraction.class.getProtectionDomain().getCodeSource().getLocation().getPath(),CHARSET);
                if (jarpath.endsWith(".jar")){
                    jarpath = jarpath.substring(0,jarpath.lastIndexOf(File.separator)+1);
                }
                File badSqlFile = new File(jarpath);
                writeFile(sql,badSqlFile);
            } catch (UnsupportedEncodingException ex) {
                LOGGER.error("UnsupportedEncodingException: {}",ex);
            }
        }
        return builder.toString();
    }

    /**
     * 分析查询sql的fromItem，针对表或子查询分别进行解析
     * @param selectBody
     * @return
     */
    private static String analyzeFromItem(SelectBody selectBody, long time, String millisecond, String identifier) {
        StringBuilder builder = new StringBuilder();
        if(selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;
            FromItem fromItem = plainSelect.getFromItem();
            Stack<FromItem> stack = new Stack<>();
            stack.push(fromItem);
            while(!stack.isEmpty()) {
                fromItem = stack.pop();
                if(fromItem instanceof SubSelect) {
                    SubSelect subSelect = (SubSelect) fromItem;
                    PlainSelect ps = (PlainSelect) subSelect.getSelectBody();
                    FromItem from = ps.getFromItem();
                    //子查询的表名和表别名
                    String tableName = null;
                    if (from instanceof Table){
                        tableName = ((Table) from).getFullyQualifiedName();
                    }
                    String subSelectAlias = subSelect.getAlias().getName();
                    //解析子查询sql，保存对应的字段
                    Map<String,Map<String,String>> mapMap = parse(ps,tableName,subSelectAlias);
                    //拼接结果
                    String str = buildResult(mapMap, time, millisecond, identifier);
                    builder.append(str);
                    stack.push(ps.getFromItem());
                }else if (fromItem instanceof Table) {
                    Table table = (Table)fromItem;
                    //查询的表名和表别名
                    String tableName = table.getFullyQualifiedName();
                    String alias = table.getAlias()!=null?fromItem.getAlias().getName():tableName;

                    //解析查询sql，保存相应的字段和表
                    Map<String,Map<String,String>> mapMap = parse(plainSelect,tableName,alias);
                    //按规则拼接结果
                    String str = buildResult(mapMap, time, millisecond, identifier);
                    builder.append(str);
                }
            }
        }
        return builder.toString();
    }

    /**
     * 遍历每一条sql解析的不同类型的结果map，并按规则进行拼接
     * @param mapMap
     * @param time
     * @param millisecond
     * @param identifier
     * @return
     */
    private static String buildResult(Map<String,Map<String,String>> mapMap, long time, String millisecond, String identifier) {
        StringBuilder buffer = new StringBuilder();
        for (Map.Entry<String, Map<String,String>> outMap : mapMap.entrySet()) {
            String key = outMap.getKey();
            //比较key，然后遍历里层的map,按规则拼接结果
            Map<String,String> innerMap = outMap.getValue();
            switch (key) {
                case FIELD:
                    buildBuf(innerMap, time, millisecond, identifier, buffer, FIELD);
                    break;
                case FUNCTION:
                    buildBuf(innerMap, time, millisecond, identifier, buffer, FUNCTION);
                    break;
                case PARTITION:
                    buildBuf(innerMap, time, millisecond, identifier, buffer, PARTITION);
                    break;
                case MAPPING_NAME:
                    buildBuf(innerMap, time, millisecond, identifier, buffer, MAPPING_NAME);
                    break;
                case EQUAL:
                    buildBuf(innerMap, time, millisecond, identifier, buffer, EQUAL);
                    break;
                case LIKE:
                    buildBuf(innerMap, time, millisecond, identifier, buffer, LIKE);
                    break;
                case RANGE:
                    buildBuf(innerMap, time, millisecond, identifier, buffer, RANGE);
                    break;
                case GROUP:
                    buildBuf(innerMap, time, millisecond, identifier, buffer, GROUP);
                    break;
                case SORT:
                    buildBuf(innerMap, time, millisecond, identifier, buffer, SORT);
                    break;
                case LIMIT:
                    buildBuf(innerMap, time, millisecond, identifier, buffer, LIMIT);
                    break;
                case JOIN:
                    buildBuf(innerMap, time, millisecond, identifier, buffer, JOIN);
                    break;
                default:
                    break;
            }
        }
        return buffer.toString();
    }

    /**
     * 拼接字段、表名等信息
     */
    private static void buildBuf(Map<String, String> innerMap, long time, String millisec, String identifier, StringBuilder buffer, String field) {
        for (Map.Entry<String, String> map : innerMap.entrySet()) {
            buffer.append(field + SEPARATOR + time + SEPARATOR + map.getKey() + SEPARATOR + map.getValue() + SEPARATOR + millisec + SEPARATOR + identifier + "\r\n");
        }
    }

    /**
     * 解析sql，分别获取查询展示字段、where过滤字段、group by的字段以及order by的字段等信息。
     * @param plainSelect
     * @param tableName
     * @param alias
     * @return
     */
    private static Map<String,Map<String,String>> parse(PlainSelect plainSelect, String tableName, String alias) {
        Map<String,Map<String,String>> mapMap = new HashMap<>(1024);

        //join关联语句
        List<Join> joins = plainSelect.getJoins();
        String joinTable = null;
        String joinTableAlias;
        if (joins!=null){
            for (Join join:joins){
                //保存join字段
                Map<String,String> joinMap = new HashMap<>();
                //关联的表名和表别名
                FromItem rightItem =join.getRightItem();
                joinTable = ((Table) rightItem).getFullyQualifiedName();
//                joinMap.put(joinTable,tableName);
                joinTableAlias = join.getRightItem().getAlias()==null?joinTable:join.getRightItem().getAlias().getName();
                //关联的on表达式，并提取出关联字段
                Expression on =join.getOnExpression();
                List<Bean> list = new ArrayList<>();
                FPWhere.getConditions(on,list);
                for (Bean bean:list){
                    Expression left = bean.getLeft();
                    String operator = bean.getOperator();
                    String right = bean.getRight();
                    //对比on表达式中关联字段"=右"侧表达式"."之前的字符串与两表的表别名，得到关联字段
                    String t = right.contains(".")?right.substring(0,right.lastIndexOf('.')):"";
                    if ((t.equals(joinTableAlias)||t.equals(alias)) && "=".equals(operator)){
                        /*String tab1 = ((Column)left).getTable().getFullyQualifiedName().equals(alias)?tableName:joinTable;
                        String tab2 = tabAlias.equals(joinTableAlias)?joinTable:tableName;*/
                        joinMap.put(((Column)left).getColumnName(),tableName);
                        joinMap.put(right,joinTable);
                    }
                }
                mapMap.put(JOIN,joinMap);
            }
        }

        //查询字段列表
        List<SelectItem> selectItems = plainSelect.getSelectItems();
        SelectExpressionItem sei;
        Expression exp;
        //保存展示字段
        Map<String,String> colMap = new HashMap<>();
        //保存函数
        Map<String,String> funcMap = new HashMap<>();
        //遍历字段列表
        for (SelectItem selectItem:selectItems){
            if((selectItem.toString().matches(".*\\*"))) {
                continue;
            } else if(selectItem instanceof SelectExpressionItem){//判断查询字段对象为表达式
                //若selectItem为表达式，将其强转为表达式类型，获取对应的表达式
                sei = (SelectExpressionItem)selectItem;
                exp = sei.getExpression();
                /*System.out.println(sei.getAlias().getName());*/
                //判断表达式对象是列名还是函数
                if (exp instanceof Column) {
                    //判断查询字段名是否有表名或表别名，以得到字段的来源
                    if (((Column) exp).getTable()!=null){
                        colMap.put(((Column) exp).getColumnName(),
                                ((Column) exp).getTable().toString().equals(alias)?tableName:joinTable);
                    } else {
                        colMap.put(((Column) exp).getColumnName(),tableName);
                    }
                } else if (exp.toString().contains("(") && exp.toString().contains(")")){//保存函数
                    funcMap.put(exp.toString(),tableName);
                }
            }
        }
        mapMap.put(FIELD,colMap);
        mapMap.put(FUNCTION,funcMap);

        //获取where条件，并得到过滤条件字段相关信息
        Expression where = plainSelect.getWhere();
        if (where != null){
            getFilterFields(mapMap, where, tableName);
        }
        //获取group by的字段信息
        GroupByElement groupByElement = plainSelect.getGroupBy();
        if (groupByElement != null){
            List<Expression> groupBy = groupByElement.getGroupByExpressions();
            //保存分组字段
            Map<String,String> groupMap = new HashMap<>();
            for (Expression group : groupBy){
                groupMap.put(((Column)group).getColumnName(),tableName);
            }
            mapMap.put(GROUP,groupMap);
        }

        //获取order by的字段信息
        List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
        if (orderByElements != null){
            //保存排序字段
            Map<String,String> sortMap = new HashMap<>();
            for (OrderByElement orderBy : orderByElements){
                sortMap.put(((Column)orderBy.getExpression()).getColumnName(),tableName);
            }
            mapMap.put(SORT,sortMap);
        }

        //获取limit字句中的offset和rowCount
        Limit limit = plainSelect.getLimit();
        if (limit != null){
            Expression offset = limit.getOffset();
            Expression rowCount = limit.getRowCount();
            //保存limit的值
            Map<String,String> limitMap = new HashMap<>();
            limitMap.put(null!=offset?offset+","+rowCount:rowCount.toString(),tableName);
            mapMap.put(LIMIT,limitMap);
        }
        return mapMap;
    }

    /**
     * where条件中非函数的字段信息
     */
    private static void getFilterFields(Map<String, Map<String, String>> mapMap, Expression where, String tableName) {
        List<Bean> list = new ArrayList<>();
        FPWhere.getConditions(where,list);
        //保存分区
        Map<String,String> partMap = new HashMap<>();
        //保存映射表名
        Map<String,String> mappingMap = new HashMap<>();
        //保存等值查询字段
        Map<String,String> equalMap = new HashMap<>();
        //保存范围查找字段
        Map<String,String> rangeMap = new HashMap<>();
        //保存模糊检索字段
        Map<String,String> likeMap = new HashMap<>();
        //遍历where后的条件
        for (Bean bean :list){
            Expression left = bean.getLeft();
            String operator = bean.getOperator();
            String value = bean.getRight();
            //where条件后的字段名
            String filterField = ((Column)left).getColumnName();
            if (PARTITION.equalsIgnoreCase(left.toString())){
                partMap.put(value.replaceAll("[(]|[)]|\\'",""),tableName);
            }else if (MAPPING_NAME.equalsIgnoreCase(left.toString())){
                mappingMap.put(value.replaceAll("[(]|[)]|\\'",""),tableName);
            } else if (("=".equalsIgnoreCase(operator)||"in".equalsIgnoreCase(operator))){
                equalMap.put(filterField,tableName);
            }else if (!"=".equals(operator) && ">、<、<=、>=".contains(operator)){
                rangeMap.put(filterField,tableName);
            }else if ("between".equalsIgnoreCase(operator)){
                rangeMap.put(filterField,tableName);
            }else if (LIKE.equalsIgnoreCase(operator)){
                likeMap.put(filterField,tableName);
            }
        }
        mapMap.put(PARTITION,partMap);
        mapMap.put(MAPPING_NAME,mappingMap);
        mapMap.put(EQUAL,equalMap);
        mapMap.put(RANGE,rangeMap);
        mapMap.put(LIKE,likeMap);
    }
}