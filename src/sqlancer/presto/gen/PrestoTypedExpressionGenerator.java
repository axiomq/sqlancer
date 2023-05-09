package sqlancer.presto.gen;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.*;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.ast.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static sqlancer.presto.PrestoSchema.PrestoDataType.BOOLEAN;
import static sqlancer.presto.PrestoSchema.PrestoDataType.VARCHAR;

public final class PrestoTypedExpressionGenerator extends
        TypedExpressionGenerator<Node<PrestoExpression>, PrestoSchema.PrestoColumn, PrestoSchema.PrestoCompositeDataType> {

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
        return generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN), randomly.getInteger(0, maxDepth));
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

//    @Override
//    public Node<PrestoExpression> generateExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
//        if (depth >= maxDepth || Randomly.getBooleanWithSmallProbability()) {
//            return generateLeafNode(type);
//        }
//
//        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
//        PrestoTypedExpressionGenerator.Expression expr = Randomly.fromList(possibleOptions);
//        BinaryOperatorNode.Operator op;
//        switch (expr) {
//            case BINARY_LOGICAL:
//                op = PrestoTypedExpressionGenerator.PrestoBinaryLogicalOperator.getRandom();
//                break;
//            case BINARY_ARITHMETIC:
//                op = PrestoTypedExpressionGenerator.PrestoBinaryLogicalOperator.getRandom();
//                break;
//            case BINARY_COMPARISON:
//                op = PrestoBinaryComparisonOperator.getRandom();
//                break;
//            default:
//                throw new AssertionError();
//        }
//
//        int depth1 = depth + 1;
//        return new NewBinaryOperatorNode<>(generateExpression(type, depth1), generateExpression(type, depth1), op);
//
//    }

    @Override
    public Node<PrestoExpression> generateExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
        // if (type == PrestoDataType.FLOAT && Randomly.getBooleanWithRatherLowProbability()) {
        // type = PrestoDataType.INT;
        // }
        if (allowAggregates && Randomly.getBoolean()) {
            return generateAggregate(type);
        }
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode(type);
        } else {
            if (Randomly.getBooleanWithRatherLowProbability()) {
                List<PrestoFunction> applicableFunctions = PrestoFunction.getFunctionsCompatibleWith(type);
                if (!applicableFunctions.isEmpty()) {
                    PrestoFunction function = Randomly.fromList(applicableFunctions);
                    return function.getCall(type, this, depth + 1);
                }
            }
            if (Randomly.getBooleanWithRatherLowProbability()) {
                return new PrestoCastFunction(generateExpression(getRandomType(), depth + 1), type);
            }
            if (Randomly.getBooleanWithRatherLowProbability()) {

                List<Node<PrestoExpression>> conditions = new ArrayList<>();
                List<Node<PrestoExpression>> cases = new ArrayList<>();
                for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
                    conditions.add(generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(BOOLEAN), depth + 1));
                    cases.add(generateExpression(type, depth + 1));
                }
                Node<PrestoExpression> elseExpr = null;
                if (Randomly.getBoolean()) {
                    elseExpr = generateExpression(type, depth + 1);
                }
                Node<PrestoExpression> expression = generateExpression(type);
                return new NewCaseOperatorNode<PrestoExpression>(expression, conditions, cases, elseExpr);
            }

            switch (type.getPrimitiveDataType()) {
                case BOOLEAN:
                    return generateBooleanExpression(depth);
                case INT:
                case VARCHAR:
                    return generateStringExpression(depth);
//                case ARRAY:
                case DATE:
                case TIME:
                case TIMESTAMP:
                case TIME_WITH_TIME_ZONE:
                case TIMESTAMP_WITH_TIME_ZONE:
                case DECIMAL:
                case FLOAT:
                case JSON:
                case INTERVAL_YEAR_TO_MONTH:
                case INTERVAL_DAY_TO_SECOND:
                    return generateLeafNode(type); // TODO
                default:
                    throw new AssertionError(type);
            }
        }
    }

    private enum StringExpression {
        CONCAT
    }


    public enum PrestBinaryStringOperator implements BinaryOperatorNode.Operator {
        CONCAT("||");

        private String textRepr;

        PrestBinaryStringOperator(String textRepr) {
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

    private Node<PrestoExpression> generateStringExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode(PrestoSchema.PrestoCompositeDataType.fromDataType(VARCHAR));
        }
        if (Randomly.getBoolean()) {
//            return getStringFunction(depth);
            return getStringOperation(depth);
        } else {
            return getStringOperation(depth);
        }
    }

    // TODO: implement
    private Node<PrestoExpression> getStringFunction(int depth) {
        return null;
    }

    private NewBinaryOperatorNode<PrestoExpression> getStringOperation(int depth) {
        StringExpression exprType = Randomly.fromOptions(StringExpression.values());
        switch (exprType) {
            case CONCAT:
                return new NewBinaryOperatorNode<>(generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(VARCHAR), depth + 1), generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(VARCHAR), depth + 1), PrestBinaryStringOperator.CONCAT);
            default:
                throw new AssertionError(exprType);
        }
    }

    private enum BooleanExpression {
        NOT,
        BINARY_COMPARISON,
        BINARY_LOGICAL,
        AND_OR_CHAIN,
        REGEX,
        IS_NULL,
        IN,
        BETWEEN,
        LIKE
//        , MULTI_VALUED_COMPARISON
    }

    private Node<PrestoExpression> generateBooleanExpression(int depth) {
        BooleanExpression exprType = Randomly.fromOptions(BooleanExpression.values());
        PrestoExpression expr;
        switch (exprType) {
            case NOT:
                return generateNOT(depth + 1);
            case BINARY_COMPARISON:
                return getBinaryComparison(depth);
            case BINARY_LOGICAL:
                return getBinaryLogical(depth);
            case AND_OR_CHAIN:
                return getAndOrChain(depth);
            case REGEX:
                return getRegex(depth);
            case IS_NULL:
                return new PrestoUnaryPostfixOperation(generateExpression(getRandomType(), depth + 1), Randomly
                        .fromOptions(PrestoUnaryPostfixOperation.PrestoUnaryPostfixOperator.IS_NULL, PrestoUnaryPostfixOperation.PrestoUnaryPostfixOperator.IS_NOT_NULL));
            case IN:
                return getInOperation(depth);
            case BETWEEN:
                return getBetween(depth);
            case LIKE:
                return getLike(depth);
            default:
                throw new AssertionError(exprType);
        }
    }

    private Node<PrestoExpression> getLike(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType.fromDataType(VARCHAR);
        Node<PrestoExpression> expression = generateExpression(type, depth + 1);
        Node<PrestoExpression> pattern = generateExpression(type, depth + 1);
        PrestoConstant.PrestoTextConstant patternString = new PrestoConstant.PrestoTextConstant(randomly.getString());
        if (Randomly.getBoolean()) {
            return new NewBinaryOperatorNode<>(expression, pattern, PrestoLikeOperator.getRandom());
        } else {
            Node<PrestoExpression> escape = generateExpression(type, depth + 1);
            return new NewTernaryNode<>(expression, pattern,
                    escape, "LIKE", "ESCAPE");
        }
    }

    private NewBinaryOperatorNode<PrestoExpression> getRegex(int depth) {
        Node<PrestoExpression> left = generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(VARCHAR), depth + 1);
        Node<PrestoExpression> right = generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(VARCHAR), depth + 1);
        return new NewBinaryOperatorNode<>(left, right, PrestoBinaryLogicalOperator.getRandom());
    }

    private NewBinaryOperatorNode<PrestoExpression> getBinaryLogical(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType.fromDataType(BOOLEAN);
        Node<PrestoExpression> left = generateExpression(type, depth + 1);
        Node<PrestoExpression> right = generateExpression(type, depth + 1);
        return new NewBinaryOperatorNode<>(left, right, PrestoBinaryLogicalOperator.getRandom());
    }

    private Node<PrestoExpression> getBetween(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.getRandomWithoutNull());
        Node<PrestoExpression> expression = generateExpression(type, depth + 1);
        Node<PrestoExpression> left = generateExpression(type, depth + 1);
        Node<PrestoExpression> right = generateExpression(type, depth + 1);
        return new NewBetweenOperatorNode<PrestoExpression>(expression, left, right, Randomly.getBoolean());
    }

    private Node<PrestoExpression> getInOperation(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.getRandomWithoutNull());
        Node<PrestoExpression> left = generateExpression(type, depth + 1);
        List<Node<PrestoExpression>> inList = generateExpressions(type, Randomly.smallNumber() + 1, depth + 1);
        boolean isNegated = Randomly.getBoolean();
        return new NewInOperatorNode<PrestoExpression>(left, inList, isNegated);
    }

    private Node<PrestoExpression> getAndOrChain(int depth) {
        Node<PrestoExpression> left = generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(BOOLEAN), depth + 1);
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            Node<PrestoExpression> right = generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(BOOLEAN), depth + 1);
            left = new NewBinaryOperatorNode<>(left, right, PrestoBinaryLogicalOperator.getRandom());
        }
        return left;
    }

    private Node<PrestoExpression> getBinaryComparison(int depth) {
        PrestoSchema.PrestoCompositeDataType type = getRandomType();
        BinaryOperatorNode.Operator op = PrestoBinaryComparisonOperator.getRandom();
        Node<PrestoExpression> left = generateExpression(type, depth + 1);
        Node<PrestoExpression> right = generateExpression(type, depth + 1);
        return new NewBinaryOperatorNode<>(left, right, op);
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
                .fromList(columns
                        .stream()
                        .filter(c -> c.getType() == type)
                        .collect(Collectors.toList())
                );
        return new PrestoColumnReference(column);
    }

    @Override
    public Node<PrestoExpression> generateLeafNode(PrestoSchema.PrestoCompositeDataType type) {
        if (Randomly.getBoolean()) {
            return generateConstant(type);
        } else {
            if (filterColumns(type.getPrimitiveDataType()).isEmpty()) {
                return generateConstant(type);
            } else {
                return generateColumn(type);
            }
        }
    }

    private List<PrestoSchema.PrestoColumn> filterColumns(PrestoSchema.PrestoDataType dataType) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType().getPrimitiveDataType() == dataType)
                    .collect(Collectors.toList());
        }
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

    public enum PrestoLikeOperator implements BinaryOperatorNode.Operator {
        LIKE("LIKE"), //
        NOT_LIKE("NOT LIKE");

        private String textRepr;

        PrestoLikeOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }


        public static PrestoLikeOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum PrestoBinaryComparisonOperator implements BinaryOperatorNode.Operator {
        EQUALS("="),
        GREATER(">"), GREATER_EQUALS(">="),
        SMALLER("<"), SMALLER_EQUALS("<="),
        NOT_EQUALS("!="),
        IS_DISTINCT_FROM("IS DISTINCT FROM"), IS_NOT_DISTINCT_FROM("IS NOT DISTINCT FROM");

        private final String textRepresentation;

        PrestoBinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        public static BinaryOperatorNode.Operator getRandomStringOperator() {
            return Randomly.fromOptions(EQUALS, NOT_EQUALS, IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM);
        }


        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

    }

    public enum PrestoBinaryArithmeticOperator implements BinaryOperatorNode.Operator {
        CONCAT("||"),
        ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"),
        ;

        private final String textRepresentation;

        PrestoBinaryArithmeticOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
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

    private Node<PrestoExpression> generateAggregate(PrestoSchema.PrestoCompositeDataType type) {
        PrestoAggregateFunction agg = Randomly
                .fromList(PrestoAggregateFunction.getAggregates(type.getPrimitiveDataType()));
        return generateArgsForAggregate(type, agg);
    }

    public Node<PrestoExpression> generateArgsForAggregate(PrestoSchema.PrestoCompositeDataType type,
                                                           PrestoAggregateFunction agg) {
        List<PrestoSchema.PrestoDataType> types = agg.getReturnTypes(type.getPrimitiveDataType());
        List<Node<PrestoExpression>> args = new ArrayList<>();
        allowAggregates = false; //
        for (PrestoSchema.PrestoDataType argType : types) {
            args.add(generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(argType)));
        }
        return new NewFunctionNode<PrestoExpression, PrestoAggregateFunction>(args, agg);
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


    private enum Expression {
        BINARY_LOGICAL, BINARY_COMPARISON, BINARY_ARITHMETIC
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


    private Node<PrestoExpression> generatePredicate(List<PrestoSchema.PrestoColumn> columns, PrestoSchema.PrestoCompositeDataType prestoCompositeDataType, int remainingDepth) {
        return null;
    }


    public Randomly getRandomly() {
        return randomly;
    }

    public PrestoGlobalState getGlobalState() {
        return globalState;
    }

    public int getMaxDepth() {
        return maxDepth;
    }
}
