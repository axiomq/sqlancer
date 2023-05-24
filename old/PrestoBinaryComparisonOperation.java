package sqlancer.presto.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.PrestoSchema;

public class PrestoBinaryComparisonOperation extends NewBinaryOperatorNode<PrestoExpression>
        implements Node<PrestoExpression>, PrestoExpression {

    public PrestoBinaryComparisonOperation(Node<PrestoExpression>  left, Node<PrestoExpression>  right,
                                           PrestoBinaryComparisonOperator op) {
        super(left, right, op);
    }

    public PrestoExpression getLeftExpression() {
        return (PrestoExpression) super.getLeft();
    }

    public PrestoExpression getRightExpression() {
        return (PrestoExpression) super.getRight();
    }

    public PrestoBinaryComparisonOperator getOp() {
        return (PrestoBinaryComparisonOperator) op;
    }

    @Override
    public PrestoSchema.PrestoDataType getExpectedType() {
        return PrestoSchema.PrestoDataType.BOOLEAN;
    }

    @Override
    public PrestoConstant getExpectedValue() {
        PrestoConstant leftExpectedValue = getLeftExpression().getExpectedValue();
        PrestoConstant rightExpectedValue = getRightExpression().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().apply(leftExpectedValue, rightExpectedValue);
    }

    public enum PrestoBinaryComparisonOperator implements BinaryOperatorNode.Operator {
        EQUALS("=") {
            @Override
            public PrestoConstant apply(PrestoConstant left, PrestoConstant right) {
                return left.isEquals(right);
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public PrestoConstant apply(PrestoConstant left, PrestoConstant right) {
                PrestoConstant isEquals = left.isEquals(right);
                if (isEquals.isBoolean()) {
                    return PrestoConstant.createBooleanConstant(!isEquals.asBoolean());
                }
                return isEquals;
            }
        },
        IS_DISTINCT("IS DISTINCT FROM") {
            @Override
            public PrestoConstant apply(PrestoConstant left, PrestoConstant right) {
                return PrestoConstant.createBooleanConstant(!IS_NOT_DISTINCT.apply(left, right).asBoolean());
            }
        },
        IS_NOT_DISTINCT("IS NOT DISTINCT FROM") {
            @Override
            public PrestoConstant apply(PrestoConstant left, PrestoConstant right) {
                if (left.isNull()) {
                    return PrestoConstant.createBooleanConstant(right.isNull());
                } else if (right.isNull()) {
                    return PrestoConstant.createBooleanConstant(false);
                } else {
                    return left.isEquals(right);
                }
            }
        },
        LESS("<") {
            @Override
            public PrestoConstant apply(PrestoConstant left, PrestoConstant right) {
                return left.isLessThan(right);
            }
        },
        LESS_EQUALS("<=") {
            @Override
            public PrestoConstant apply(PrestoConstant left, PrestoConstant right) {
                PrestoConstant isLessThan = left.isLessThan(right);
                if (isLessThan.isBoolean() && !isLessThan.asBoolean()) {
                    return left.isEquals(right);
                } else {
                    return isLessThan;
                }
            }
        },
        GREATER(">") {
            @Override
            public PrestoConstant apply(PrestoConstant left, PrestoConstant right) {
                PrestoConstant isEquals = left.isEquals(right);
                if (isEquals.isBoolean() && isEquals.asBoolean()) {
                    return PrestoConstant.createBooleanConstant(false);
                } else {
                    PrestoConstant less = left.isLessThan(right);
                    if (less.isNull()) {
                        return PrestoConstant.createNullConstant();
                    }
                    return PrestoConstant.createBooleanConstant(!less.asBoolean());
                }
            }
        },
        GREATER_EQUALS(">=") {
            @Override
            public PrestoConstant apply(PrestoConstant left, PrestoConstant right) {
                PrestoConstant isEquals = left.isEquals(right);
                if (isEquals.isBoolean() && isEquals.asBoolean()) {
                    return PrestoConstant.createBooleanConstant(true);
                } else {
                    PrestoConstant less = left.isLessThan(right);
                    if (less.isNull()) {
                        return PrestoConstant.createNullConstant();
                    }
                    return PrestoConstant.createBooleanConstant(!less.asBoolean());
                }
            }
        };

        private final String textRepresentation;

        PrestoBinaryComparisonOperator(String text) {
            textRepresentation = text;
        }

        public abstract PrestoConstant apply(PrestoConstant left, PrestoConstant right);

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

}
