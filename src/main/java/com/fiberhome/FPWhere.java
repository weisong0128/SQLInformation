package com.fiberhome;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;

import java.util.List;

/**
 * @Author wcc
 * @Date 2020/5/29 10:35
 */
public class FPWhere {
	private FPWhere() {
	}

	/**
	 * getConditions方法用来获取where条件之后的查询的字段名和每个字段对应的查询的条件
	 * @param expression
	 * @return
	 * @throws ParseException
	 */
//	public static List<Map<String, String>> getConditions(Expression expression,List<Map<String, String>> list) {
	public static List<Bean> getConditions(Expression expression,List<Bean> list) {
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
				//创建一个Bean对象来保存字段名、操作符和对应的值
				Bean bean = new Bean();
				bean.setLeft(leftExpression);
				bean.setOperator(operator);
				bean.setRight(value);
				list.add(bean);
			}
			getFilterFields(leftExpression,list);

			//获取右边的表达式
			Expression rightExpression = ((BinaryExpression) expression).getRightExpression();
			getFilterFields(rightExpression,list);
		}else if(expression instanceof Parenthesis){//如果包含(),则递归获取括号中的内容
			Expression parenthesis = ((Parenthesis) expression).getExpression();
			getConditions(parenthesis,list);
		}else if(expression instanceof IsNullExpression){
			IsNullExpression isNull = (IsNullExpression)expression;

		}else if(expression instanceof InExpression){
			InExpression in = (InExpression) expression;
            //创建一个Bean对象来保存字段名、操作符和对应的值
            Bean bean = new Bean();
            bean.setLeft(in.getLeftExpression());
            bean.setOperator("IN");
            bean.setRight(in.getRightItemsList().toString());
            list.add(bean);
		} else if (expression instanceof Between) {
			Between between = (Between)expression;
            treatBetween(list, between);
        }
		return list;
	}

    /**
     * 处理between表达式
     * @param list
     * @param between
     */
    private static void treatBetween(List<Bean> list, Between between) {
        //创建一个Bean对象来保存字段名、操作符和对应的值符
        Bean bean = new Bean();
        bean.setLeft(between.getLeftExpression());
        bean.setOperator("BETWEEN");
        bean.setRight(between.getBetweenExpressionStart() + " " + between.getBetweenExpressionEnd());
        list.add(bean);
    }

    /**
     * 以下操作是用来判断的左右Expression具体类型，并将expression转换为对应的类型
     * 处理相应的表达式并保存字段名、操作符和对应的值
     * @param list
     * @param expression
     */
    private static void getFilterFields(Expression expression,List<Bean> list) {
        if (expression instanceof Between){
            Between between = (Between)expression;
            treatBetween(list, between);
        } else if (expression instanceof LikeExpression) {
            LikeExpression likeExp = (LikeExpression) expression;
            //处理表达式并保存字段名、操作符和对应的值
            filterFieldsTreat(list, likeExp);
        } else if (expression instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) expression;
            filterFieldsTreat(list, equalsTo);
        } else if (expression instanceof NotEqualsTo) {
            NotEqualsTo notEqualsTo = (NotEqualsTo) expression;
            filterFieldsTreat(list, notEqualsTo);
        } else if (expression instanceof GreaterThan) {
            GreaterThan greaterThan = (GreaterThan) expression;
           filterFieldsTreat(list, greaterThan);
        } else if (expression instanceof GreaterThanEquals) {
            GreaterThanEquals greaterThanEquals = (GreaterThanEquals) expression;
            filterFieldsTreat(list, greaterThanEquals);
        } else if (expression instanceof MinorThan) {
            MinorThan minorThan = (MinorThan) expression;
           filterFieldsTreat(list, minorThan);
        } else if (expression instanceof MinorThanEquals) {
            MinorThanEquals minorThanEquals = (MinorThanEquals) expression;
            filterFieldsTreat(list, minorThanEquals);
        } else {
            //递归调用,获取leftExpression完整的查询条件
            getConditions(expression,list);
        }
    }

    /**
     * 处理where后相应的关系表达式并保存字段名、操作符和对应的值
     * @param list
     * @param expression
     */
    private static void filterFieldsTreat(List<Bean> list, BinaryExpression expression) {
        Expression left = expression.getLeftExpression();
        String operator = expression.getStringExpression();
        String right = expression.getRightExpression().toString();

        Bean bean = new Bean();
        bean.setLeft(left);
        bean.setOperator(operator);
        bean.setRight(right);
        list.add(bean);
    }
}