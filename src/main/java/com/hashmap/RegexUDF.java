/*
 * Class: RegexUDF
 *
 * Created on 16-07-2019
 *
 * (c) Copyright Lam Research Corporation, unpublished work, created 2019
 * All use, disclosure, and/or reproduction of this material is prohibited
 * unless authorized in writing.  All Rights Reserved.
 * Rights in this program belong to:
 * Lam Research Corporation
 * 4000 N. First Street
 * San Jose, CA
 */
package com.hashmap;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;

@BuiltInFunction(name = RegexUDF.NAME, args = {
        @Argument(allowedTypes = {PVarchar.class}),
        @Argument(allowedTypes = {PVarchar.class})})
public class RegexUDF extends ScalarFunction {

    public static final String NAME = "Prefix_A";

    public RegexUDF() {
    }

    public RegexUDF(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
//        Expression arg = getChildren().get(0);
//        if (!arg.evaluate(tuple, ptr)) return false;
//        String regex = new String(ptr.copyBytes());
//        arg = getChildren().get(1);
//        if (!arg.evaluate(tuple, ptr))
//            return false;
//
//        String str=new String(ptr.copyBytes());
//        String[] token=str.split(",");
//        int count=0;
//        StringBuilder stringBuilder=new StringBuilder();
//        for (String fileName:token) {
//            boolean matches = Pattern.matches(regex,fileName.trim());
//            if(matches){
//                if(count==0){
//                    stringBuilder.append(fileName);
//                }
//                else {
//                    stringBuilder.append(",");
//                    stringBuilder.append(fileName);
//                }
//                count++;
//            }
//        }
//
//        byte[] target = stringBuilder.toString().getBytes();
//        ptr.set(target);
//        return true;


        Expression strExpression = getChildren().get(0);
        if (!strExpression.evaluate(tuple, ptr)) {
            return false;
        }

        String sourceStr = (String)PVarchar.INSTANCE.toObject(ptr, strExpression.getSortOrder());

        if (sourceStr == null) {
            return true;
        }

        Expression typeExpression = getChildren().get(1);
        if (!typeExpression.evaluate(tuple, ptr)) {
            return false;
        }

        String regex = (String)PVarchar.INSTANCE.toObject(ptr, typeExpression.getSortOrder());

        //String str=new String(ptr.copyBytes());
        String[] token=sourceStr.split(",");
        int count=0;
        StringBuilder stringBuilder=new StringBuilder();
        for (String fileName:token) {
            boolean matches = Pattern.matches(regex,fileName.trim());
            if(matches){
                if(count==0){
                    stringBuilder.append(fileName);
                }
                else {
                    stringBuilder.append(",");
                    stringBuilder.append(fileName);
                }
                count++;
            }
        }

        byte[] target = stringBuilder.toString().getBytes();
        ptr.set(target);
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PVarchar.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
