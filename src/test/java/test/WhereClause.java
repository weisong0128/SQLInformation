package test;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

/**
 * @Author Administrator
 * @Date 2020/6/1 9:36
 */
public class WhereClause {
    /**
     * getConditions方法用来获取where条件之后的查询的字段名和每个字段对应的查询的条件
     * @param expression
     * @return
     * @throws ParseException
     */
    public static void conditions(Expression expression) {
        if (expression == null){
            return;
        }
        List<Expression> queue = new LinkedList<>();
        queue.add(expression);
        while (!queue.isEmpty()){
            Expression head = queue.remove(0);
            if (head instanceof BinaryExpression){
                //获取左边的表达式
                Expression leftExpression = ((BinaryExpression) head).getLeftExpression();
//                System.out.println(leftExpression);
                if(leftExpression instanceof Column) {
                    String column = ((Column) leftExpression).getColumnName();
                    String value = ((BinaryExpression) leftExpression).getRightExpression().toString();
                    System.out.println(((BinaryExpression) leftExpression).getLeftExpression()+"#"+((BinaryExpression) leftExpression).getStringExpression()+"#"+((BinaryExpression) leftExpression).getRightExpression());
                }
                if (leftExpression instanceof Between){
                    Between between = (Between)leftExpression;
                    System.out.println(between.getLeftExpression()+"#"+between.getBetweenExpressionStart()+"#"+between.getBetweenExpressionEnd());
                } else if (leftExpression instanceof LikeExpression) {
                    LikeExpression likeExp = ((LikeExpression) leftExpression);
                    System.out.println(likeExp.getLeftExpression()+"#"+likeExp.getStringExpression()+"#"+likeExp.getRightExpression());
                } else if (leftExpression instanceof EqualsTo) {
                    EqualsTo equalsTo = (EqualsTo) leftExpression;
                    System.out.println(equalsTo.getLeftExpression()+"#"+equalsTo.getStringExpression()+"#"+equalsTo.getRightExpression());
                } else if (leftExpression instanceof NotEqualsTo) {
                    NotEqualsTo notEqualsTo = (NotEqualsTo) leftExpression;
                    System.out.println(notEqualsTo.getLeftExpression()+"#"+notEqualsTo.getStringExpression()+"#"+notEqualsTo.getRightExpression());
                } else if (leftExpression instanceof GreaterThan) {
                    GreaterThan greaterThan = (GreaterThan) leftExpression;
                    System.out.println(greaterThan.getLeftExpression()+"#"+greaterThan.getStringExpression()+"#"+greaterThan.getRightExpression());
                } else if (leftExpression instanceof GreaterThanEquals) {
                    GreaterThanEquals greaterThanEquals = (GreaterThanEquals) leftExpression;
                    System.out.println(greaterThanEquals.getLeftExpression()+"#"+greaterThanEquals.getStringExpression()+"#"+greaterThanEquals.getRightExpression());
                } else if (leftExpression instanceof MinorThan) {
                    MinorThan minorThan = (MinorThan) leftExpression;
                    System.out.println(minorThan.getLeftExpression()+"#"+minorThan.getStringExpression()+"#"+minorThan.getRightExpression());
                } else if (leftExpression instanceof MinorThanEquals) {
                    MinorThanEquals minorThanEquals = (MinorThanEquals) leftExpression;
                    System.out.println(minorThanEquals.getLeftExpression()+"#"+minorThanEquals.getStringExpression()+"#"+minorThanEquals.getRightExpression());
                } else if(leftExpression instanceof Parenthesis){//如果包含(),则递归获取括号中的内容
                    Expression parenthesis = ((Parenthesis) leftExpression).getExpression();
                    queue.add(parenthesis);
                } else {
                    queue.add(leftExpression);
                }

                //获取右边的表达式
                Expression rightExpression = ((BinaryExpression) head).getRightExpression();
//                System.out.println(rightExpression);
                if (rightExpression instanceof Between){
                    Between between = (Between)rightExpression;
                    System.out.println(between.getLeftExpression()+"#"+between.getBetweenExpressionStart()+"#"+between.getBetweenExpressionEnd());
                } else if (rightExpression instanceof LikeExpression) {
                    LikeExpression likeExp = ((LikeExpression) rightExpression);
                    System.out.println(likeExp.getLeftExpression()+"#"+likeExp.getStringExpression()+"#"+likeExp.getRightExpression());
                } else if (rightExpression instanceof EqualsTo) {
                    EqualsTo equalsTo = (EqualsTo) rightExpression;
                    System.out.println(equalsTo.getLeftExpression()+"#"+equalsTo.getStringExpression()+"#"+equalsTo.getRightExpression());
                } else if (rightExpression instanceof NotEqualsTo) {
                    NotEqualsTo notEqualsTo = (NotEqualsTo) rightExpression;
                    System.out.println(notEqualsTo.getLeftExpression()+"#"+notEqualsTo.getStringExpression()+"#"+notEqualsTo.getRightExpression());
                } else if (rightExpression instanceof GreaterThan) {
                    GreaterThan greaterThan = (GreaterThan) rightExpression;
                    System.out.println(greaterThan.getLeftExpression()+"#"+greaterThan.getStringExpression()+"#"+greaterThan.getRightExpression());
                } else if (rightExpression instanceof GreaterThanEquals) {
                    GreaterThanEquals greaterThanEquals = (GreaterThanEquals) rightExpression;
                    System.out.println(greaterThanEquals.getLeftExpression()+"#"+greaterThanEquals.getStringExpression()+"#"+greaterThanEquals.getRightExpression());
                } else if (rightExpression instanceof MinorThan) {
                    MinorThan minorThan = (MinorThan) rightExpression;
                    System.out.println(minorThan.getLeftExpression()+"#"+minorThan.getStringExpression()+"#"+minorThan.getRightExpression());
                } else if (rightExpression instanceof MinorThanEquals) {
                    MinorThanEquals minorThanEquals = (MinorThanEquals) rightExpression;
                    System.out.println(minorThanEquals.getLeftExpression()+"#"+minorThanEquals.getStringExpression()+"#"+minorThanEquals.getRightExpression());
                } else if(rightExpression instanceof Parenthesis){//如果包含(),则递归获取括号中的内容
                    Expression parenthesis = ((Parenthesis) rightExpression).getExpression();
                    queue.add(parenthesis);
                } else {
//                    queue.add(rightExpression);
                }
            } else if(expression instanceof IsNullExpression){
                IsNullExpression isNull = (IsNullExpression)expression;
                System.out.println(isNull.getLeftExpression()+"#");
            }else if(expression instanceof InExpression){
                InExpression in = (InExpression) expression;
                System.out.println(in.getLeftExpression().toString()+"#"+in.getRightItemsList().toString());
            } else if (expression instanceof Between) {
                Between between = (Between)expression;
                System.out.println(between.getLeftExpression()+"#"+between.getBetweenExpressionStart()+"#"+between.getBetweenExpressionEnd());
            } else if(expression instanceof Parenthesis){//如果包含(),则递归获取括号中的内容
                Expression parenthesis = ((Parenthesis) expression).getExpression();
                queue.add(parenthesis);
            }
        }
    }
}
