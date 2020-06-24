package test;

import com.fiberhome.Bean;
import com.fiberhome.FPWhere;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.*;

/**
 * @description: 描述
 * @author: ws
 * @time: 2020/6/18 14:02
 */
public class Demo01 {

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

    public static void main(String[] args) {
//        String sql = "select name from (select * from ws_table) tmp group by name limit 1";
        String sql = "SELECT COUNT(*) FROM NB_APP_SKE_DELIVERY WHERE PARTITION='2020' AND (RECEIVER_ADDRESS LIKE '%太原市%' OR RECEIVER_DISTRICT_NAME='太原市' OR RECEIVER_DISTRICT_NAME='太原') AND ACTION_TIME BETWEEN 1590854400 AND 1590940800";
        long time = 1592132980L;
        String millisecond = "100";
        String identifier = "其他";

        try {
            Statement stmt = CCJSqlParserUtil.parse(sql);
            StringBuilder builder = new StringBuilder();
            if(stmt instanceof Select) {
                System.out.println("为select语句");
                //1.获取表名
                TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                List<String> tableList =tablesNamesFinder.getTableList(stmt);
                System.out.println("表名为：" + tableList.get(0));
                builder.append("table" + SEPARATOR + time + SEPARATOR +" "+ SEPARATOR + tableList.get(0) + SEPARATOR + millisecond + SEPARATOR + identifier + "\r\n");
                
                //2.获取字段名
                Select select = (Select) stmt;
                SelectBody selectBody = select.getSelectBody(); //sql的主查询体
                //analyzeFromItem()方法测试
                if(selectBody instanceof PlainSelect) {
                    PlainSelect plainSelect = (PlainSelect) selectBody;
                    //3.fromItem(查询来源)，即fromItem为from后()内的语句
                    FromItem fromItem = plainSelect.getFromItem();      //得到from后的内容，fromItem=(SELECT * FROM ws_table)
                    Stack<FromItem> stack = new Stack<>();  //stack=[(SELECT * FROM ws_table)]
                    stack.push(fromItem);
                    while(!stack.isEmpty()) {
                        fromItem = stack.pop();
                        System.out.println("fromItem=" + fromItem);
                        if (fromItem instanceof Table) {    //如果from后跟着是表名
                            Table table = (Table) fromItem;
                            //查询的表名和表别名
                            String tableName = table.getFullyQualifiedName();
                            String alias = table.getAlias()!=null?fromItem.getAlias().getName():tableName;

                            //解析查询sql，保存相应的字段和表
                            Map<String,Map<String,String>> mapMap = parse(plainSelect,tableName,alias);
                            //按规则拼接结果
                            String str = buildResult(mapMap, time, millisecond, identifier);
                            builder.append(str);
                        } else if(fromItem instanceof SubSelect) {  //如果from后包含一个子查询，如from (select * from ws_table) ...
                            System.out.println("包含子查询");
                            SubSelect subSelect = (SubSelect) fromItem;
                            PlainSelect ps = (PlainSelect) subSelect.getSelectBody();
                            FromItem from = ps.getFromItem();   //得到子查询中from后的内容
                            //子查询的表名和表别名
                            String tableName = null;
                            if (from instanceof Table){
                                tableName = ((Table) from).getFullyQualifiedName();
                            }
                            System.out.println("tableName=" + tableName);
                            String subSelectAlias = subSelect.getAlias().getName(); //获取到表别名tmp,即：from (select * from ws_table) tmp
                            //解析子查询sql，保存对应的字段
                            Map<String,Map<String,String>> mapMap = parse(ps,tableName,subSelectAlias);
                            //按规则拼接结果
                            String str = buildResult(mapMap, time, millisecond, identifier);
                            builder.append(str);
                            stack.push(ps.getFromItem());
                        }

                    }

                    System.out.println(builder.toString());
                }

            } else {
                System.out.println("不是select语句");
            }

        } catch (JSQLParserException e) {
            e.printStackTrace();
        }

    }

    /**
     * 遍历每一条sql解析的不同类型的结果map，并按规则进行拼接
     * @param mapMap
     * @param time
     * @param millisecond
     * @param identifier
     * @return
     */
    private static String buildResult(Map<String, Map<String, String>> mapMap, long time, String millisecond, String identifier) {
        StringBuilder buffer = new StringBuilder();
        for (Map.Entry<String, Map<String, String>> outMap : mapMap.entrySet()) {
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
     * @param innerMap
     * @param time
     * @param millisec
     * @param identifier
     * @param buffer
     * @param field
     */
    private static void buildBuf(Map<String, String> innerMap, long time, String millisec, String identifier, StringBuilder buffer, String field) {
        for (Map.Entry<String, String> map : innerMap.entrySet()) {
            buffer.append(field + SEPARATOR + time + SEPARATOR + map.getKey() + SEPARATOR + map.getValue() + SEPARATOR + millisec + SEPARATOR + identifier + "\r\n");
        }

    }

    /**
     * 解析sql，分别获取查询展示字段、where过滤字段、group by的字段以及order by的字段等信息。
     * @param plainSelect :select语句模板
     * @param tableName :表名
     * @param alias :表别名
     * @return
     */
    private static Map<String,Map<String,String>> parse(PlainSelect plainSelect, String tableName, String alias) {
        Map<String, Map<String, String>> mapMap = new HashMap<>(1024);

        //1.是否包含join关联语句
        List<Join> joins = plainSelect.getJoins();
        String joinTable = null;
        String joinTableAlias;
        if (joins!=null){

        }
        //开始正常解析
        //2.获取select后字段名或函数列表，并存入mapMap中
        List<SelectItem> selectItems = plainSelect.getSelectItems();//得到select后的内容
        SelectExpressionItem sei;
        Expression exp;
        //保存展示字段
        Map<String, String> colMap = new HashMap<>();
        //保存函数
        Map<String,String> funcMap = new HashMap<>();
        //遍历字段列表
        for (SelectItem selectItem : selectItems) {
            if(selectItem.toString().matches(".*\\*")) {    //匹配正则表达式，select后出现 *
                continue;
            }else if(selectItem instanceof SelectExpressionItem){   //判断查询字段对象为表达式
                //若selectItem为表达式，将其强转为表达式类型，获取对应的表达式
                sei = (SelectExpressionItem)selectItem;
                exp = sei.getExpression();
                //判断表达式对象是列名还是函数
                if (exp instanceof Column) {
                    //判断查询字段名是否有表名或表别名，以得到字段的来源
                    if (((Column) exp).getTable()!=null){
                        colMap.put(((Column) exp).getColumnName(),
                                ((Column) exp).getTable().toString().equals(alias)?tableName:joinTable);
                    } else {
                        colMap.put(((Column) exp).getColumnName(),tableName);
                    }
                }else if (exp.toString().contains("(") && exp.toString().contains(")")){//保存函数
                    funcMap.put(exp.toString(),tableName);
                }

            }

        }
        //将字段名和函数放入map
        mapMap.put(FIELD,colMap);
        mapMap.put(FUNCTION,funcMap);

        //3.获取where后条件，得到过滤条件字段相关信息，并存入mapMap中
        Expression where = plainSelect.getWhere();
        if (null != where) {
            getFilterFields(mapMap, where, tableName);
        }

        //4.获取group by的字段信息，并存入mapMap中
        GroupByElement groupByElement = plainSelect.getGroupBy();
        if (null != groupByElement) {
            List<Expression> groupByExpressions = groupByElement.getGroupByExpressions();
            //保存分组字段
            Map<String,String> groupMap = new HashMap<>();
            for (Expression group : groupByExpressions) {
                groupMap.put(((Column)group).getColumnName(), tableName);
            }
            mapMap.put(GROUP,groupMap);

        }

        //5.获取order by的字段信息，并存入mapMap中
        List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
        if (null != orderByElements) {
            //保存排序字段
            Map<String,String> sortMap = new HashMap<>();
            for (OrderByElement order : orderByElements) {
                sortMap.put(((Column)order.getExpression()).getColumnName(), tableName);
            }
            mapMap.put(SORT, sortMap);
        }

        //6.获取limit字句中的offset和rowCount，并存入mapMap中
        Limit limit = plainSelect.getLimit();
        if (null != limit) {
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
     * 获取where条件中非函数的字段信息
     * @param mapMap
     * @param where
     * @param tableName
     */
    private static void getFilterFields(Map<String, Map<String, String>> mapMap, Expression where, String tableName) {
        //bean :  name = "ws"   三个属性
        List<Bean> list = new ArrayList<>();
        FPWhere.getConditions(where,list);  //解析where后表达式，并添加到list中
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
        for (Bean bean : list) {
            Expression left = bean.getLeft();
            String operator = bean.getOperator();
            String value = bean.getRight();
            //获取where后的字段名
            String columnName = ((Column) left).getColumnName();
            if (PARTITION.equalsIgnoreCase(left.toString())){
                partMap.put(value.replaceAll("[(]|[)]|\\'",""),tableName);
            }else if (MAPPING_NAME.equalsIgnoreCase(left.toString())){
                mappingMap.put(value.replaceAll("[(]|[)]|\\'",""),tableName);
            }else if (("=".equalsIgnoreCase(operator)||"in".equalsIgnoreCase(operator))){
                equalMap.put(columnName,tableName);
            }else if (!"=".equals(operator) && ">、<、<=、>=".contains(operator)){
                rangeMap.put(columnName,tableName);
            }else if ("between".equalsIgnoreCase(operator)){
                rangeMap.put(columnName,tableName);
            }else if (LIKE.equalsIgnoreCase(operator)){
                likeMap.put(columnName,tableName);
            }
        }
        mapMap.put(PARTITION,partMap);
        mapMap.put(MAPPING_NAME,mappingMap);
        mapMap.put(EQUAL,equalMap);
        mapMap.put(RANGE,rangeMap);
        mapMap.put(LIKE,likeMap);
    }



}
