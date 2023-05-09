package sqlancer.presto.ast;

import sqlancer.presto.PrestoSchema;

public interface PrestoExpression {

    default PrestoSchema.PrestoDataType getExpectedType() {
        return null;
    }

    default PrestoConstant getExpectedValue() {
        return null;
    }

}
