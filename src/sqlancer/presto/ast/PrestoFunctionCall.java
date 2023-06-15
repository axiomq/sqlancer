package sqlancer.presto.ast;

import java.util.List;

import sqlancer.common.ast.newast.Node;

public class PrestoFunctionCall implements Node<PrestoExpression> {

    private final PrestoDefaultFunction function;
    private final List<Node<PrestoExpression>> arguments;

    public PrestoFunctionCall(PrestoDefaultFunction function, List<Node<PrestoExpression>> arguments) {
        this.function = function;
        this.arguments = arguments;
    }

    public List<Node<PrestoExpression>> getArguments() {
        return arguments;
    }

    public PrestoDefaultFunction getFunction() {
        return function;
    }

    public String getName() {
        return function.getFunctionName();
    }

}
