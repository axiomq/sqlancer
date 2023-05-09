package sqlancer.presto;

import sqlancer.common.ast.newast.Node;
import sqlancer.presto.ast.PrestoExpression;

import java.util.List;
import java.util.stream.Collectors;

public final class PrestoExpressionToNode {

    private PrestoExpressionToNode() {

    }

    @SuppressWarnings("unchecked")
    public static Node<PrestoExpression> cast(PrestoExpression expression) {
        return (Node<PrestoExpression>) expression;
    }

    @SuppressWarnings("unchecked")
    public static List<Node<PrestoExpression>> casts(List<PrestoExpression> expressions) {
        return expressions.stream().map(e -> (Node<PrestoExpression>) e).collect(Collectors.toList());
    }

}
