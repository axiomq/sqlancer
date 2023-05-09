package sqlancer.presto.ast;

import sqlancer.common.ast.newast.Node;

import java.util.List;

public class PrestoFunctionCall implements Node<PrestoExpression> {

    private final PrestoFunction function;
    private final List<Node<PrestoExpression>> arguments;

    public PrestoFunctionCall(PrestoFunction function, List<Node<PrestoExpression>> arguments) {
        this.function = function;
        this.arguments = arguments;
    }

    public List<Node<PrestoExpression>> getArguments() {
        return arguments;
    }

    public PrestoFunction getFunction() {
        return function;
    }

    public String getName() {
        return function.getFunctionName();
    }

}
