package com.hashmap;
import java.sql.SQLException;
import java.util.List;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.StringUtil;

@BuiltInFunction(name = PaymentAmountUDF.NAME, args = {
        @Argument(allowedTypes = {PVarchar.class})})
public class PaymentAmountUDF extends ScalarFunction {

    public static final String NAME = "Prefix_A";

    public PaymentAmountUDF() {
    }

    public PaymentAmountUDF(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression arg = getChildren().get(0);
        if (!arg.evaluate(tuple, ptr)) {
            return false;
        }

        int targetOffset = ptr.getLength();
        if (targetOffset == 0) {
            return true;
        }

        byte[] source = ptr.get();
        // prefix A is one byte
        byte[] target = new byte[targetOffset + 1];
        int sourceOffset = ptr.getOffset();
        int endOffset = sourceOffset + ptr.getLength();
        SortOrder sortOrder = arg.getSortOrder();

        int tmp = 1;

        // needed
        while (sourceOffset < endOffset) {
            int nBytes = StringUtil.getBytesInChar(source[sourceOffset], sortOrder);
            System.arraycopy(source, sourceOffset, target, tmp, nBytes);
            sourceOffset += nBytes;
            tmp += nBytes;
        }
        // add prefix A
        target[0] = (byte) 0x41;

        ptr.set(target);
        return true;
    }

    @Override
    public SortOrder getSortOrder() {
        return getChildren().get(0).getSortOrder();
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