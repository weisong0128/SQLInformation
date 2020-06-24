package test;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author wcc
 * @Date 2020/5/29 10:35
 */
public class FPWhere2 {
	public static final StringBuilder builder = new StringBuilder();

	private FPWhere2() {
	}

	/**
	 * getConditions方法用来获取where条件之后的查询的字段名和每个字段对应的查询的条件
	 * @param expression
	 * @return
	 * @throws ParseException
	 */
	public static List<Map<String, String>> getConditions(Expression expression,List<Map<String, String>> list) {
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
				Map<String, String> colMap = new HashMap<>();
				colMap.put(column, operator+","+value);
				list.add(colMap);
			}
			getFilterFields(leftExpression,list);
			//拼接条件关系表达式and或or
			builder.append(((BinaryExpression) expression).getStringExpression()).append(" ");

			//获取右边的表达式
			Expression rightExpression = ((BinaryExpression) expression).getRightExpression();
			getFilterFields(rightExpression,list);
		}else if(expression instanceof Parenthesis){//如果包含(),则递归获取括号中的内容
			Expression parenthesis = ((Parenthesis) expression).getExpression();
			builder.append("(");
			getConditions(parenthesis,list);
			builder.append(") ");
		}else if(expression instanceof IsNullExpression){
			IsNullExpression isNull = (IsNullExpression)expression;
			builder.append(isNull.toString());
           /* Map<String, String> isNullMap = getMap(isNull.getLeftExpression(), "is null/is not null");
            list.add(isNullMap);*/
		}else if(expression instanceof InExpression){
			InExpression in = (InExpression) expression;
            //拼接in的字段名和对应的类型操作符
			builder.append(in.getLeftExpression()+" IN (' ') ");

            Map<String, String> inMap = new HashMap<>();
            inMap.put(in.getLeftExpression().toString(),"IN|"+in.getRightItemsList());
            list.add(inMap);
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
    private static void treatBetween(List<Map<String, String>> list, Between between) {
        //拼接between表达式的字段名和对应的类型操作符
        builder.append(between.getLeftExpression()).append(" ").append("BETWEEN").append(" \'\'").append(" AND ").append("\'\' ");
        //创建一个map保存between表达式的字段名和对应的类型操作符
        Map<String, String> betweenMap = new HashMap<>();
        betweenMap.put(between.getLeftExpression().toString(), "BETWEEN|" + between.getBetweenExpressionStart() + " " + between.getBetweenExpressionEnd());
        list.add(betweenMap);
    }

    /**
	 * 以下操作是用来判断的左右Expression具体类型，并将expression转换为对应的类型
	 * 另外创建一个HashMap来接收每个具体表达式类型操作符左右两侧的值
	 * @param list
	 * @param expression
	 */
	private static void getFilterFields(Expression expression,List<Map<String, String>> list) {
		if (expression instanceof Between){
			Between between = (Between)expression;
            treatBetween(list, between);
        } else if (expression instanceof LikeExpression) {
			LikeExpression likeExp = (LikeExpression) expression;
			//拼接过滤条件的字段名和对应的类型操作符
			buildBuf(likeExp.getLeftExpression(), likeExp.getStringExpression());
            //创建一个map保存where过滤条件的字段名和对应的操作符
            filterFieldsTreat(list, likeExp.getLeftExpression(), likeExp.getStringExpression(), likeExp.getRightExpression());
        } else if (expression instanceof EqualsTo) {
			EqualsTo equalsTo = (EqualsTo) expression;
			buildBuf(equalsTo.getLeftExpression(), equalsTo.getStringExpression());

            filterFieldsTreat(list, equalsTo.getLeftExpression(), equalsTo.getStringExpression(), equalsTo.getRightExpression());
        } else if (expression instanceof NotEqualsTo) {
			NotEqualsTo notEqualsTo = (NotEqualsTo) expression;
			buildBuf(notEqualsTo.getLeftExpression(), notEqualsTo.getStringExpression());

            filterFieldsTreat(list, notEqualsTo.getLeftExpression(), notEqualsTo.getStringExpression(), notEqualsTo.getRightExpression());
        } else if (expression instanceof GreaterThan) {
			GreaterThan greaterThan = (GreaterThan) expression;
			buildBuf(greaterThan.getLeftExpression(), greaterThan.getStringExpression());

            filterFieldsTreat(list, greaterThan.getLeftExpression(), greaterThan.getStringExpression(), greaterThan.getRightExpression());
        } else if (expression instanceof GreaterThanEquals) {
			GreaterThanEquals greaterThanEquals = (GreaterThanEquals) expression;
			buildBuf(greaterThanEquals.getLeftExpression(),greaterThanEquals.getStringExpression());

            filterFieldsTreat(list, greaterThanEquals.getLeftExpression(), greaterThanEquals.getStringExpression(), greaterThanEquals.getRightExpression());
        } else if (expression instanceof MinorThan) {
			MinorThan minorThan = (MinorThan) expression;
			buildBuf(minorThan.getLeftExpression(), minorThan.getStringExpression());

            filterFieldsTreat(list, minorThan.getLeftExpression(), minorThan.getStringExpression(), minorThan.getRightExpression());
        } else if (expression instanceof MinorThanEquals) {
			MinorThanEquals minorThanEquals = (MinorThanEquals) expression;
			buildBuf(minorThanEquals.getLeftExpression(), minorThanEquals.getStringExpression());

            filterFieldsTreat(list, minorThanEquals.getLeftExpression(), minorThanEquals.getStringExpression(), minorThanEquals.getRightExpression());
        } else {
			//递归调用,获取leftExpression完整的查询条件
			getConditions(expression,list);
		}
	}

    /**
     * 保存where过滤条件的字段名和对应的类型操作符
     * @param list
     * @param leftExpression
     * @param stringExpression
     * @param rightExpression
     */
    private static void filterFieldsTreat(List<Map<String,String>> list, Expression leftExpression, String stringExpression, Expression rightExpression) {
        Map<String, String> map = new HashMap<>();
        map.put(leftExpression.toString(), stringExpression + "|" + rightExpression);
        list.add(map);
    }

    /**
	 * 拼接where条件字段名和对应的类型操作符
	 * @param leftExpression
	 * @param stringExpression
	 */
	private static void buildBuf(Expression leftExpression, String stringExpression) {
		builder.append(leftExpression).append(" ").append(stringExpression).append(" \'\' ");
	}

	/**
	 * 查询展示字段中的函数名和字段名
	 * @param exp
	 */
	private static String getSelectFunction(Function exp) {
		StringBuilder funcBuf = new StringBuilder();
		//获取函数名
		String function = exp.getName();
		funcBuf.append("函数：").append(function).append("\n");
		//获取函数中的参数列表，并从中获取得到列名
		if (exp.getParameters()!=null){
			List<Expression> params = exp.getParameters().getExpressions();
			for (Expression param:params){
				if (param instanceof Column){
					funcBuf.append("查询字段：").append(param).append("\n");
				}
			}
		}
		return funcBuf.toString();
	}

	/**
	 * 获取where条件中的函数名和字段名
	 * @param left
	 * @param operator
	 */
	private static String getWhereFunction(Expression left, String operator) {
		StringBuilder buf = new StringBuilder();
		String function = ((Function) left).getName();
		buf.append("函数：").append(function).append("\n");
		List<Expression> params = ((Function) left).getParameters().getExpressions();
		for (Expression param:params){
			if (param instanceof Column){
				if ("=".equalsIgnoreCase(operator)||"in".equalsIgnoreCase(operator) && !"partition".equalsIgnoreCase(left.toString())){
					buf.append("EQUAL: ").append(param).append("\n");
				}else if (!"=".equals(operator) && ">、<、<=、>=".contains(operator)){
					buf.append("SIZE: ").append(param).append("\n");
				}else if ("between".equalsIgnoreCase(operator)){
					buf.append("SIZE: ").append(param).append("\n");
				}else if ("like".equalsIgnoreCase(operator) && !"partition".equalsIgnoreCase(left.toString())){
					buf.append("LIKE: ").append(param).append("\n");
				}
			}
		}
		return buf.toString();
	}

}