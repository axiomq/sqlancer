package sqlancer.presto.ast;

import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.PrestoSchema;

public class PrestoBetweenOperation extends NewBetweenOperatorNode<PrestoExpression> implements Node<PrestoExpression>, PrestoExpression {

    public PrestoBetweenOperation(Node<PrestoExpression> left, Node<PrestoExpression> middle, Node<PrestoExpression> right,
                                  boolean isTrue) {
        super(left, middle, right, isTrue);
    }

    @Override
    public PrestoConstant getExpectedValue() {
        PrestoBinaryComparisonOperation leftComparison = new PrestoBinaryComparisonOperation(getMiddle(),
                getLeft(), PrestoBinaryComparisonOperation.PrestoBinaryComparisonOperator.LESS_EQUALS);
        PrestoBinaryComparisonOperation rightComparison = new PrestoBinaryComparisonOperation(getLeft(),
                getRight(), PrestoBinaryComparisonOperation.PrestoBinaryComparisonOperator.LESS_EQUALS);
        return new PrestoBinaryLogicalOperation(leftComparison, rightComparison,
                PrestoBinaryLogicalOperation.PrestoBinaryLogicalOperator.AND).getExpectedValue();
    }

    @Override
    public PrestoSchema.PrestoDataType getExpectedType() {
        return PrestoSchema.PrestoDataType.BOOLEAN;
    }

}
