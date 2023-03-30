package sqlancer.presto.ast;

import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.PrestoSchema;

public class PrestoUnaryPrefixOperation extends NewUnaryPrefixOperatorNode<PrestoExpression> {

    public PrestoUnaryPrefixOperation(PrestoUnaryPrefixOperator operation, Node<PrestoExpression> expression) {
        super(expression, operation);
    }

    @Override
    public String getOperatorRepresentation() {
        return this.op.getTextRepresentation();
    }

    public enum PrestoUnaryPrefixOperator implements BinaryOperatorNode.Operator {
        NOT("NOT", PrestoSchema.PrestoDataType.BOOLEAN, PrestoSchema.PrestoDataType.INT) {
            @Override
            public PrestoSchema.PrestoDataType getExpressionType() {
                return PrestoSchema.PrestoDataType.BOOLEAN;
            }
        },

        UNARY_PLUS("+", PrestoSchema.PrestoDataType.INT) {
            @Override
            public PrestoSchema.PrestoDataType getExpressionType() {
                return PrestoSchema.PrestoDataType.INT;
            }
        },
        UNARY_MINUS("-", PrestoSchema.PrestoDataType.INT) {
            @Override
            public PrestoSchema.PrestoDataType getExpressionType() {
                return PrestoSchema.PrestoDataType.INT;
            }
        };

        private final String textRepresentation;

        PrestoUnaryPrefixOperator(String textRepresentation, PrestoSchema.PrestoDataType... dataTypes) {
            this.textRepresentation = textRepresentation;
        }

        public abstract PrestoSchema.PrestoDataType getExpressionType();

        @Override
        public String getTextRepresentation() {
            return this.textRepresentation;
        }
    }

}
