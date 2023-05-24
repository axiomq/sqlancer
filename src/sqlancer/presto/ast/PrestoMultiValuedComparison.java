package sqlancer.presto.ast;

import sqlancer.common.ast.newast.Node;

import java.util.List;

public class PrestoMultiValuedComparison implements Node<PrestoExpression> {

    private final Node<PrestoExpression> left;
    private final List<Node<PrestoExpression>> right;
    private final PrestoMultiValuedComparisonType type;
    private final PrestoMultiValuedComparisonOperator op;

    public PrestoMultiValuedComparison(Node<PrestoExpression> left, List<Node<PrestoExpression>> right,
                                       PrestoMultiValuedComparisonType type, PrestoMultiValuedComparisonOperator op) {
        this.left = left;
        this.right = right;
        this.type = type;
        this.op = op;
    }

    public Node<PrestoExpression> getLeft() {
        return left;
    }

    public PrestoMultiValuedComparisonOperator getOp() {
        return op;
    }

    public List<Node<PrestoExpression>> getRight() {
        return right;
    }

    public PrestoMultiValuedComparisonType getType() {
        return type;
    }

}
