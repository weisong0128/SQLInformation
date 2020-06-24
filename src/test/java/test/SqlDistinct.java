package test;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

/**
 * @Author Administrator
 * @Date 2020/6/4 11:00
 */
public class SqlDistinct {
    public static void main(String[] args) {
        String sql = "select kkbh,dldm,12 as a,count(*) as sl from (select * from veh_passrec_jczhpt  where partition like  '2019%' and (gcsj >= '2019-06-01' and gcsj<'2019-07-01')) m" +
                " where substr(m.dldm,1,1) in ('5','6','7','8') and substr(xzqh,1,4)='4401' group by  kkbh,dldm order by dldm,kkbh;";
        StringBuilder builder = new StringBuilder();
        try {
            Statement stmt = CCJSqlParserUtil.parse(new StringReader(sql));
            if(stmt instanceof Select) {
                Select select = (Select) stmt;
                //获取with字句列表
                List<WithItem> withItems = select.getWithItemsList();
                //判断withItems是否为空,若不为空则获取with字句的查询语句并验证.
                if (withItems != null) {
                    for (WithItem withItem : withItems) {
//                        System.out.println(withItem.getName());
                        SelectBody withSelectBody = withItem.getSelectBody();
                        parse(withSelectBody);
                    }
                }
                SelectBody selectBody = select.getSelectBody();

//                System.out.println(((PlainSelect)selectBody).getJoins());

                List<FromItem> fromList = getFromClause(selectBody);
                /*if (fromList.size() == 1){
                    parse(selectBody);
                } else if (fromList.size()>1) {
                    for (FromItem fromItem:fromList){
                        if (fromItem instanceof SubSelect){
                            SubSelect subSelect = (SubSelect) fromItem;
                            parse(subSelect.getSelectBody());
                        }else if (fromItem instanceof Table){
                            parse(selectBody);
                        }
                    }
                }*/
            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
    }

    /**
     * 解析sql，分别获取查询展示字段、where过滤字段、group by的字段以及order by的字段等信息。
     * @param selectBody
     */
    private static void parse(SelectBody selectBody) {
        PlainSelect plain = (PlainSelect)selectBody;
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        //查询字段列表
        List<SelectItem> selectItems = plain.getSelectItems();
        SelectExpressionItem sei = null;
        Expression exp = null;
        //遍历字段列表
        for (int i=0;i<selectItems.size();i++){
            SelectItem selectItem = selectItems.get(i);
            if((selectItem.toString().matches(".*\\*"))) {
                builder.append(selectItem);
            } else if(selectItem instanceof SelectExpressionItem){//判断查询字段对象为表达式
                if (i!=0){
                    builder.append(",");
                }
               sei = ((SelectExpressionItem) selectItem);
               exp = sei.getExpression();
               if (exp.toString().replaceAll("\'","").matches("\\d+")){
                   builder.append(exp.toString().replaceAll(exp.toString().replaceAll("\'",""),"***")+sei.getAlias());
               }else {
                   builder.append(sei);
               }
            }
        }
        //表名
        FromItem fromItem = plain.getFromItem();
        if (fromItem instanceof Table){
            builder.append(" FROM ").append(fromItem);
        } else if (fromItem instanceof SubSelect){
            System.out.println(((SubSelect) fromItem).getSelectBody());
        }
        //where字句
        Expression where = plain.getWhere();
        if (where != null){
           StringBuilder buffer = new StringBuilder();
           getConditions(buffer,where);
           builder.append(" WHERE ").append(buffer);
        }
        System.out.println(builder);
    }

    /**
     * 获取查询来源(表或其他子查询),添加到fromList结合中。若为子查询,则判断子查询是否有别名.
     * @param selectBody
     * @return
     */
    private static List<FromItem> getFromClause(SelectBody selectBody) {
        List<FromItem> list = new ArrayList<>();
        try {
            if(selectBody instanceof PlainSelect) {
                PlainSelect plain = (PlainSelect) selectBody;
                FromItem fromItem = plain.getFromItem();
                Stack<FromItem> stack = new Stack<>();
                stack.push(fromItem);
                while(!stack.isEmpty()) {
                    fromItem = stack.pop();
                    if(fromItem instanceof SubSelect) {
                        SubSelect subSelect = (SubSelect) fromItem;
                        list.add(subSelect);
                        PlainSelect ps = (PlainSelect) subSelect.getSelectBody();
                        stack.push(ps.getFromItem());
                    }else if (fromItem instanceof Table) {
                        Table tab = (Table)fromItem;
                        list.add(tab);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * getConditions方法用来获取where条件之后的查询的字段名和每个字段对应的查询的条件
     * @param expression
     * @return
     * @throws ParseException
     */
    private static StringBuilder getConditions(StringBuilder builder,Expression expression) {
        if (expression == null){
            return null;
        }
        if(expression instanceof BinaryExpression){
            //获取左边的表达式
            Expression leftExpression = ((BinaryExpression) expression).getLeftExpression();
            /*
             * 以下操作是用来判断leftExpression的具体类型，并将leftExpression转换为对应的类型
             * 另外创建一个HashMap来接收每个具体表达式类型操作符左右两侧的值
             */
            if(leftExpression instanceof Column) {
                String column = ((Column) leftExpression).getColumnName();
                String value = ((BinaryExpression) expression).getRightExpression().toString();
                String operator = ((BinaryExpression) expression).getStringExpression();

                builder.append(column).append(" ").append(operator).append("\'\'");

            }
            String s1 = getFilterFields(builder,leftExpression);
            builder.append(s1);
            //拼接条件关系表达式and或or
            builder.append(((BinaryExpression) expression).getStringExpression()).append(" ");

            //获取右边的表达式
            Expression rightExpression = ((BinaryExpression) expression).getRightExpression();
            String s2 = getFilterFields(builder,rightExpression);
            builder.append(s2);
        }else if(expression instanceof Parenthesis){//如果包含(),则递归获取括号中的内容
            Expression parenthesis = ((Parenthesis) expression).getExpression();
            builder.append("(");
            getConditions(builder,parenthesis);
            builder.append(") ");
        }else if(expression instanceof IsNullExpression){
            IsNullExpression isNull = (IsNullExpression)expression;
            builder.append(isNull.toString());

        }else if(expression instanceof InExpression){
            InExpression in = (InExpression) expression;
            //拼接in的字段名和对应的类型操作符
            builder.append(in.getLeftExpression()+" IN (' ') ");

        } else if (expression instanceof Between) {
            Between between = (Between)expression;
            //拼接between表达式的字段名和对应的类型操作符
            builder.append(between.getLeftExpression()).append(" ").append("BETWEEN").append(" \'\'").append(" AND ").append("\'\' ");
        }
        return builder;
    }

    /**
     * 以下操作是用来判断的左右Expression具体类型，并将expression转换为对应的类型
     * 处理相应的表达式并保存字段名、操作符和对应的值
     * @param expression
     */
    private static String getFilterFields(StringBuilder builder,Expression expression) {
        StringBuilder buffer = new StringBuilder();
        if (expression == null){
            return null;
        }

        if (expression instanceof Between){
            Between between = (Between)expression;
            //拼接between表达式的字段名和对应的类型操作符
            buffer.append(between.getLeftExpression()).append(" ").append("BETWEEN").append(" \'\'").append(" AND ").append("\'\' ");
        } else if (expression instanceof LikeExpression) {
            LikeExpression likeExp = (LikeExpression) expression;
            //拼接where过滤条件的字段名和对应的类型操作符
            buffer.append(likeExp.getLeftExpression()).append(" ").append(likeExp.getStringExpression()).append(" \'\' ");
        } else if (expression instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) expression;
            buffer.append(equalsTo.getLeftExpression()).append(" ").append(equalsTo.getStringExpression()).append(" \'\' ");
        } else if (expression instanceof NotEqualsTo) {
            NotEqualsTo notEqualsTo = (NotEqualsTo) expression;
            buffer.append(notEqualsTo.getLeftExpression()).append(" ").append(notEqualsTo.getStringExpression()).append(" \'\' ");
        } else if (expression instanceof GreaterThan) {
            GreaterThan greaterThan = (GreaterThan) expression;
            buffer.append(greaterThan.getLeftExpression()).append(" ").append(greaterThan.getStringExpression()).append(" \'\' ");
        } else if (expression instanceof GreaterThanEquals) {
            GreaterThanEquals greaterThanEquals = (GreaterThanEquals) expression;
            buffer.append(greaterThanEquals.getLeftExpression()).append(" ").append(greaterThanEquals.getStringExpression()).append(" \'\' ");
        } else if (expression instanceof MinorThan) {
            MinorThan minorThan = (MinorThan) expression;
            buffer.append(minorThan.getLeftExpression()).append(" ").append(minorThan.getStringExpression()).append(" \'\' ");
        } else if (expression instanceof MinorThanEquals) {
            MinorThanEquals minorThanEquals = (MinorThanEquals) expression;
            buffer.append(minorThanEquals.getLeftExpression()).append(" ").append(minorThanEquals.getStringExpression()).append(" \'\' ");
        } else {
            //递归调用,获取leftExpression完整的查询条件
            getConditions(builder,expression);
        }
        return buffer.toString();
    }

}
