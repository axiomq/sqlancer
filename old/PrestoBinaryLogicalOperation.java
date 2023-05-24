package sqlancer.presto.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoDataType;

public class PrestoBinaryLogicalOperation extends NewBinaryOperatorNode<PrestoExpression>
        implements Node<PrestoExpression> {

    public PrestoBinaryLogicalOperation(Node<PrestoExpression> left, Node<PrestoExpression> right,
                                        PrestoBinaryLogicalOperator op) {
        super(left, right, op);
    }

    public PrestoConstant getExpectedValue() {
        PrestoConstant leftValue = getLeft().getExpectedValue();
        PrestoConstant rightValue = getRight().getExpectedValue();
        if (leftValue == null || rightValue == null) {
            return null;
        }
        return getOp().apply(leftValue, rightValue);
    }


    public PrestoSchema.PrestoDataType getExpectedType() {
        return PrestoSchema.PrestoDataType.BOOLEAN;
    }

    public enum PrestoBinaryLogicalOperator implements BinaryOperatorNode.Operator {
        AND("AND", "and") {
            @Override
            public PrestoConstant apply(PrestoConstant left, PrestoConstant right) {
                PrestoConstant leftVal = left.cast(PrestoDataType.BOOLEAN);
                PrestoConstant rightVal = right.cast(PrestoDataType.BOOLEAN);
                assert leftVal.isNull() || leftVal.isBoolean() : leftVal + "不是NULL也不是Boolean类型";
                assert rightVal.isNull() || rightVal.isBoolean() : rightVal + "不是NULL也不是Boolean类型";
                if (leftVal.isNull()) {
                    if (rightVal.isNull()) {
                        return PrestoConstant.createNullConstant();
                    } else {
                        if (rightVal.asBoolean()) {
                            return PrestoConstant.createNullConstant();
                        } else {
                            return PrestoConstant.createBooleanConstant(false);
                        }
                    }
                } else if (!leftVal.asBoolean()) {
                    return PrestoConstant.createBooleanConstant(false);
                }
                assert leftVal.asBoolean();
                if (rightVal.isNull()) {
                    return PrestoConstant.createNullConstant();
                } else {
                    return PrestoConstant.createBooleanConstant(rightVal.asBoolean());
                }
            }
        },
        OR("OR", "or") {
            @Override
            public PrestoConstant apply(PrestoConstant left, PrestoConstant right) {
                PrestoConstant leftVal = left.cast(PrestoDataType.BOOLEAN);
                PrestoConstant rightVal = right.cast(PrestoDataType.BOOLEAN);
                assert leftVal.isNull() || leftVal.isBoolean() : leftVal + "不是NULL也不是Boolean类型";
                assert rightVal.isNull() || rightVal.isBoolean() : rightVal + "不是NULL也不是Boolean类型";
                if (leftVal.isBoolean() && leftVal.asBoolean()) {
                    return PrestoConstant.createBooleanConstant(true);
                }
                if (rightVal.isBoolean() && rightVal.asBoolean()) {
                    return PrestoConstant.createBooleanConstant(true);
                }
                if (leftVal.isNull() || rightVal.isNull()) {
                    return PrestoConstant.createNullConstant();
                }
                return PrestoConstant.createBooleanConstant(false);
            }
        };

        private final String[] textRepresentations;

        PrestoBinaryLogicalOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        @Override
        public String getTextRepresentation() {
            return Randomly.fromOptions(textRepresentations);
        }

        public PrestoBinaryLogicalOperator getRandomOp() {
            return Randomly.fromOptions(values());
        }

        public static PrestoBinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        public abstract PrestoConstant apply(PrestoConstant left, PrestoConstant right);

    }

}
