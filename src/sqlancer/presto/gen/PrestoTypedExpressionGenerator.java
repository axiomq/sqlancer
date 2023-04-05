package sqlancer.presto.gen;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.ast.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class PrestoTypedExpressionGenerator extends
    TypedExpressionGenerator<Node<PrestoExpression>, PrestoSchema.PrestoColumn, PrestoSchema.PrestoCompositeDataType> {

    private enum Expression {
        BINARY_LOGICAL, BINARY_COMPARISON, BINARY_ARITHMETIC
    }

    private final Randomly randomly;
    private final PrestoGlobalState globalState;
    private final int maxDepth;

    public PrestoTypedExpressionGenerator(PrestoGlobalState globalState) {
        this.globalState = globalState;
        this.randomly = globalState.getRandomly();
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
    }

    @Override
    public Node<PrestoExpression> generatePredicate() {
        return generateExpressionWithColumns(columns, randomly.getInteger(0, 3));
//          return generateExpressionWithColumns(columns, new PrestoSchema.PrestoCompositeDataType(PrestoSchema.PrestoDataType.BOOLEAN, 1, 1) , randomly.getInteger(0, 5));
//        Node<PrestoExpression> prestoExpressionNode = generateExpression(new PrestoSchema.PrestoCompositeDataType(PrestoSchema.PrestoDataType.BOOLEAN, 1, 1));
//        return prestoExpressionNode;
    }

    @Override
    public Node<PrestoExpression> negatePredicate(Node<PrestoExpression> predicate) {
        return new PrestoUnaryPrefixOperation(PrestoUnaryPrefixOperation.PrestoUnaryPrefixOperator.NOT, predicate);
    }

    @Override
    public Node<PrestoExpression> isNull(Node<PrestoExpression> expr) {
        return new PrestoUnaryPostfixOperation(expr, PrestoUnaryPostfixOperation.PrestoUnaryPostfixOperator.IS_NULL);
    }

    @Override
    public Node<PrestoExpression> generateConstant(PrestoSchema.PrestoCompositeDataType type) {
        switch (type.getPrimitiveDataType()) {
            case NULL:
                return PrestoConstant
                    .createNullConstant();
            case CHAR:
                return PrestoConstant.PrestoTextConstant
                    .createStringConstant(globalState.getRandomly().getAlphabeticChar(), type.getSize());
            case VARCHAR:
                return PrestoConstant.PrestoTextConstant
                    .createStringConstant(globalState.getRandomly().getString(), type.getSize());
            case VARBINARY:
                return PrestoConstant.createVarbinaryConstant(globalState.getRandomly().getString());
            case JSON:
                return PrestoConstant.PrestoJsonConstant.createJsonConstant();
            case TIME:
                return PrestoConstant
                    .createTimeConstant(globalState.getRandomly().getLong(0, System.currentTimeMillis()));
            case TIME_WITH_TIME_ZONE:
                return PrestoConstant
                    .createTimeWithTimeZoneConstant(globalState.getRandomly().getLong(0, System.currentTimeMillis()));
            case TIMESTAMP:
                return PrestoConstant
                    .createTimestampConstant(globalState.getRandomly().getLong(0, System.currentTimeMillis()));
            case TIMESTAMP_WITH_TIME_ZONE:
                return PrestoConstant
                    .createTimestampWithTimeZoneConstant(globalState.getRandomly().getLong(0, System.currentTimeMillis()));
            case INTERVAL_YEAR_TO_MONTH:
                return PrestoConstant
                    .createIntervalYearToMonth(globalState.getRandomly().getLong(0, System.currentTimeMillis()));
            case INTERVAL_DAY_TO_SECOND:
                return PrestoConstant
                    .createIntervalDayToSecond(globalState.getRandomly().getLong(0, System.currentTimeMillis()));
            case INT:
                return PrestoConstant.PrestoIntConstant
                    .createIntConstant(Randomly.getNonCachedInteger());
            case FLOAT:
                return PrestoConstant.PrestoFloatConstant
                    .createFloatConstant(globalState.getRandomly().getDouble());
            case BOOLEAN:
                return PrestoConstant.PrestoBooleanConstant
                    .createBooleanConstant(Randomly.getBoolean());
            case DATE:
                return PrestoConstant
                    .createDateConstant(globalState.getRandomly().getLong(0, System.currentTimeMillis()));
            case DECIMAL:
                return PrestoConstant
                    .createDecimalConstant(globalState.getRandomly().getLong(0, System.currentTimeMillis()));
            default:
                throw new AssertionError("Unknown type: " + type);
        }
    }

    @Override
    protected Node<PrestoExpression> generateExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (depth >= maxDepth || Randomly.getBooleanWithSmallProbability()) {
            return generateLeafNode(type);
        }

        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        PrestoTypedExpressionGenerator.Expression expr = Randomly.fromList(possibleOptions);
        BinaryOperatorNode.Operator op;
        switch (expr) {
            case BINARY_LOGICAL:
            case BINARY_ARITHMETIC:
                op = PrestoTypedExpressionGenerator.PrestoBinaryLogicalOperator.getRandom();
                break;
            case BINARY_COMPARISON:
                op = PrestoBinaryComparisonOperator.getRandom();
                break;
            default:
                throw new AssertionError();
        }

        int depth1 = depth + 1;
        return new NewBinaryOperatorNode<>(generateExpression(type, depth1), generateExpression(type, depth1), op);

    }

    private enum BooleanExpression {
        POSTFIX_OPERATOR, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, LIKE, BETWEEN, IN_OPERATION
    }

    Node<PrestoExpression> generateBooleanExpression(int depth) {
//        if (allowAggregateFunctions) {
//            allowAggregateFunctions = false;
//        }
        List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(BooleanExpression.values()));
        BooleanExpression option = Randomly.fromList(validOptions);
        switch (option) {
            case POSTFIX_OPERATOR:
                return generatePostfixExpression(depth + 1);
            case NOT:
                return generateNOT(depth + 1);
//            case BETWEEN: // TODO (NULL BETWEEN NULL AND NULL)
//                return getBetween(depth + 1);
//            case IN_OPERATION:
//                return getIn(depth + 1);
//            case BINARY_LOGICAL_OPERATOR:
//                return getBinaryLogical(depth + 1, DatabendSchema.DatabendDataType.BOOLEAN);
//            case BINARY_COMPARISON:
//                return getComparison(depth + 1);
//            case LIKE:
//                return getLike(depth + 1, DatabendSchema.DatabendDataType.VARCHAR);
            default:
                throw new AssertionError();
        }
    }

    private Node<PrestoExpression> generatePostfixExpression(int depth) {
        PrestoUnaryPostfixOperation.PrestoUnaryPostfixOperator operator = PrestoUnaryPostfixOperation.PrestoUnaryPostfixOperator.getRandom();
        PrestoSchema.PrestoDataType dataType = Randomly.fromOptions(operator.getInputDataTypes());
        PrestoSchema.PrestoCompositeDataType compositeDataType = PrestoSchema.PrestoCompositeDataType.fromDataType(dataType);
        return new PrestoUnaryPostfixOperation(generateExpression(compositeDataType, depth), operator);
    }

    private Node<PrestoExpression> generateNOT(int depth) {
        PrestoUnaryPrefixOperation.PrestoUnaryPrefixOperator operator = PrestoUnaryPrefixOperation.PrestoUnaryPrefixOperator.NOT;
        PrestoSchema.PrestoDataType dataType = Randomly.fromOptions(operator.getRandomInputDataTypes());
        PrestoSchema.PrestoCompositeDataType compositeDataType = PrestoSchema.PrestoCompositeDataType.fromDataType(dataType);
        return new PrestoUnaryPrefixOperation(operator, generateExpression(compositeDataType, depth));
    }

    @Override
    protected Node<PrestoExpression> generateColumn(PrestoSchema.PrestoCompositeDataType type) {
        PrestoSchema.PrestoColumn column = Randomly
            .fromList(columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList()));
        return new PrestoColumnReference(column);
    }

    @Override
    protected PrestoSchema.PrestoCompositeDataType getRandomType() {
        return PrestoSchema.PrestoCompositeDataType.getRandomWithoutNull();
    }

    @Override
    protected boolean canGenerateColumnOfType(PrestoSchema.PrestoCompositeDataType type) {
        return columns.stream().anyMatch(c -> c.getType() == type);
    }

    public enum PrestoBinaryLogicalOperator implements BinaryOperatorNode.Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum PrestoBinaryComparisonOperator implements BinaryOperatorNode.Operator {
        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!=");

        private final String textRepr;

        PrestoBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public enum PrestoBinaryArithmeticOperator implements BinaryOperatorNode.Operator {
        CONCAT("||"),
        ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"),
        ;

        private final String textRepr;

        PrestoBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public enum PrestoAggregateFunction {
        MAX(1), MIN(1), AVG(1), COUNT(1), STRING_AGG(1), FIRST(1), SUM(1), STDDEV_SAMP(1), STDDEV_POP(1), VAR_POP(1),
        VAR_SAMP(1), COVAR_POP(1), COVAR_SAMP(1);

        private final int nrArgs;

        PrestoAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static PrestoAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }

    }

    public NewFunctionNode<PrestoExpression, PrestoAggregateFunction> generateArgsForAggregate(PrestoAggregateFunction aggregateFunction) {
        return new NewFunctionNode<PrestoExpression, PrestoAggregateFunction>(
            generateExpressions(aggregateFunction.getNrArgs()), aggregateFunction);
    }

    public Node<PrestoExpression> generateAggregate() {
        PrestoAggregateFunction aggrFunc = PrestoAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    @Override
    public List<Node<PrestoExpression>> generateOrderBys() {
        List<Node<PrestoExpression>> expressions = new ArrayList<>();
        int nr = Randomly.smallNumber() + 1;
        ArrayList<PrestoSchema.PrestoColumn> hsqldbColumns = new ArrayList<>(columns);
        for (int i = 0; i < nr && !hsqldbColumns.isEmpty(); i++) {
            PrestoSchema.PrestoColumn randomColumn = Randomly.fromList(hsqldbColumns);
            PrestoColumnReference columnReference = new PrestoColumnReference(randomColumn);
            hsqldbColumns.remove(randomColumn);
            expressions.add(columnReference);
        }
        return expressions;
    }

    public Node<PrestoExpression> generateHavingClause() {
        allowAggregates = true;
        Node<PrestoExpression> expr = generateExpression(PrestoSchema.PrestoCompositeDataType.getRandomWithoutNull());
        allowAggregates = false;
        return expr;
    }

    public Node<PrestoExpression> generateExpressionWithColumns(List<PrestoSchema.PrestoColumn> columns, int remainingDepth) {
        if (columns.isEmpty() || remainingDepth <= 2 && Randomly.getBooleanWithRatherLowProbability()) {
            return generateConstant(PrestoSchema.PrestoCompositeDataType.getRandomWithoutNull());
        }
        PrestoSchema.PrestoColumn column = Randomly.fromList(columns);
        if (remainingDepth <= 2 || Randomly.getBooleanWithRatherLowProbability()) {
            return new PrestoColumnReference(column);
        }

        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(PrestoTypedExpressionGenerator.Expression.values()));

        PrestoTypedExpressionGenerator.Expression expr = Randomly.fromList(possibleOptions);
        BinaryOperatorNode.Operator op;
        switch (expr) {
            case BINARY_LOGICAL:
            case BINARY_ARITHMETIC:
                op = PrestoTypedExpressionGenerator.PrestoBinaryLogicalOperator.getRandom();
                break;
            case BINARY_COMPARISON:
                op = PrestoBinaryComparisonOperator.getRandom();
                break;
            default:
                throw new AssertionError();
        }
        return new NewBinaryOperatorNode<>(generateExpression(column.getType(), remainingDepth - 1), generateExpression(column.getType(), remainingDepth - 1), op);
    }

    private Node<PrestoExpression> generateExpressionWithColumns(List<PrestoSchema.PrestoColumn> columns, PrestoSchema.PrestoCompositeDataType prestoCompositeDataType, int remainingDepth) {
        return null;
    }

}
