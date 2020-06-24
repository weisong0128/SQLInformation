package com.fiberhome;

import net.sf.jsqlparser.expression.Expression;

import java.io.Serializable;

/**
 * 保存字段名、操作符和对应的值
 * @Author wcc
 * @Date 2020/6/2 19:29
 */
public class Bean {
    private Expression left;
    private String operator;
    private String right;

    public Expression getLeft() {
        return left;
    }

    public void setLeft(Expression left) {
        this.left = left;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getRight() {
        return right;
    }

    public void setRight(String right) {
        this.right = right;
    }

    @Override
    public String toString() {
        return "Bean{" +
                "left=" + left +
                ", operator='" + operator + '\'' +
                ", right='" + right + '\'' +
                '}';
    }
}
