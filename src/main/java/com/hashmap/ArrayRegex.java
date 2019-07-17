/*
 * Class: ArrayRegex
 *
 * Created on 17-07-2019
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
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;

@BuiltInFunction(name = ArrayRegex.NAME, args = { @Argument(allowedTypes = { PVarchar.class }),
        @Argument(allowedTypes = { PVarchar.class }) })
public class ArrayRegex extends ScalarFunction
{

    public static final String NAME = "ArrayRegex";

    public ArrayRegex()
    {
    }

    public ArrayRegex(List<Expression> children) throws SQLException
    {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr)
    {

        Expression regexExpression = getChildren().get(1);
        Expression colExpression = getChildren().get(0);

        if (!regexExpression.evaluate(tuple, ptr))
        {
            return false;
        }

        String regex = (String) PVarchar.INSTANCE.toObject(ptr);

        if (!colExpression.evaluate(tuple, ptr))
        {
            return false;
        }

        String intermediate = (String) PVarchar.INSTANCE.toObject(ptr);
        String[] arrayElements =intermediate.split(",");

        if (null != regex & null != arrayElements)
        {
            Pattern pattern = Pattern.compile(regex);
            List<String> matchingElements = Arrays.stream(arrayElements)
                    .filter(element -> pattern.matcher(element).matches()).collect(Collectors.toList());

            ptr.set(StringUtils.join(matchingElements, ",").getBytes());

            return true;
        }
        return false;
    }

    @Override
    public SortOrder getSortOrder()
    {
        return getChildren().get(0).getSortOrder();
    }

    @Override
    public PDataType getDataType()
    {
        return PVarchar.INSTANCE;
    }

    @Override
    public String getName()
    {
        return NAME;
    }
}