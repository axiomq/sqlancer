package sqlancer.presto.gen;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.newast.*;
import sqlancer.common.gen.TypedExpressionGenerator;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.ast.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static sqlancer.presto.PrestoSchema.PrestoDataType.*;
import static sqlancer.presto.ast.PrestoConstant.createTimezoneConstant;

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
                        .createStringConstant(randomly.getAlphabeticChar(), type.getSize());
            case VARCHAR:
                return PrestoConstant.PrestoTextConstant
                        .createStringConstant(randomly.getString(), type.getSize());
            case VARBINARY:
                return PrestoConstant.createVarbinaryConstant(randomly.getString());
            case JSON:
                return PrestoConstant.PrestoJsonConstant.createJsonConstant();
            case TIME:
                return PrestoConstant
                        .createTimeConstant(randomly.getLong(0, System.currentTimeMillis()));
            case TIME_WITH_TIME_ZONE:
                return PrestoConstant
                        .createTimeWithTimeZoneConstant(randomly.getLong(0, System.currentTimeMillis()));
            case TIMESTAMP:
                return PrestoConstant
                        .createTimestampConstant(randomly.getLong(0, System.currentTimeMillis()));
            case TIMESTAMP_WITH_TIME_ZONE:
                return PrestoConstant
                        .createTimestampWithTimeZoneConstant(randomly.getLong(0, System.currentTimeMillis()));
            case INTERVAL_YEAR_TO_MONTH:
                return PrestoConstant
                        .createIntervalYearToMonth(randomly.getLong(0, System.currentTimeMillis()));
            case INTERVAL_DAY_TO_SECOND:
                return PrestoConstant
                        .createIntervalDayToSecond(randomly.getLong(0, System.currentTimeMillis()));
            case INT:
                return PrestoConstant.PrestoIntConstant
                        .createIntConstant(type, Randomly.getNonCachedInteger());
            case FLOAT:
                return PrestoConstant.PrestoFloatConstant
                        .createFloatConstant(randomly.getDouble());
            case BOOLEAN:
                return PrestoConstant.PrestoBooleanConstant
                        .createBooleanConstant(Randomly.getBoolean());
            case DATE:
                return PrestoConstant
                        .createDateConstant(randomly.getLong(0, System.currentTimeMillis()));
            case DECIMAL:
                return PrestoConstant
                        .createDecimalConstant(type, randomly.getLong(0, System.currentTimeMillis()));
            default:
                throw new AssertionError("Unknown type: " + type);
        }
    }

    @Override
    public Node<PrestoExpression> generateExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (allowAggregates && Randomly.getBoolean()) {
            return generateAggregate(type);
        }
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode(type);
        } else {
            // TODO: functions
//            List<PrestoFunction> applicableFunctions = PrestoFunction.getFunctionsCompatibleWith(type);
//            if (Randomly.getBooleanWithRatherLowProbability() && !applicableFunctions.isEmpty()) {
//                PrestoFunction function = Randomly.fromList(applicableFunctions);
//                return getFunction(type, depth, function);
//            }
            // TODO: cast
//            if (Randomly.getBooleanWithRatherLowProbability()) {
//                Node<PrestoExpression> expressionNode = generateCast(type, depth);
//                return new PrestoCastFunction(expressionNode, type);
//            }
            if (Randomly.getBooleanWithRatherLowProbability()) {
                return getCase(type, depth);
            }
            switch (type.getPrimitiveDataType()) {
                case BOOLEAN:
                    return generateBooleanExpression(depth);
                case VARCHAR:
                case CHAR:
                    return generateStringExpression(type, depth);
                case INT:
                case DECIMAL:
                case FLOAT:
                    return generateNumericExpression(depth);
                case DATE:
                case TIME:
                case TIMESTAMP:
                case TIME_WITH_TIME_ZONE:
                case TIMESTAMP_WITH_TIME_ZONE:
                    return generateTemporalExpression(type, depth);
                case INTERVAL_YEAR_TO_MONTH:
                case INTERVAL_DAY_TO_SECOND:
                    return generateIntervalExpression(type, depth);
                case VARBINARY:
                case JSON:
//                case ARRAY:
//                case MAP:
                    return generateLeafNode(type); // TODO
                default:
                    throw new AssertionError(type);
            }
        }
    }

    private Node<PrestoExpression> generateCast(PrestoSchema.PrestoCompositeDataType type, int depth) {
        // check can cast
        return generateExpression(getRandomType(), depth + 1);
    }

    private NewCaseOperatorNode<PrestoExpression> getCase(PrestoSchema.PrestoCompositeDataType type, int depth) {
        List<Node<PrestoExpression>> conditions = new ArrayList<>();
        List<Node<PrestoExpression>> cases = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            conditions.add(generateExpression(type, depth + 1));
            cases.add(generateExpression(type, depth + 1));
        }
        Node<PrestoExpression> elseExpr = null;
        if (Randomly.getBoolean()) {
            elseExpr = generateExpression(type, depth + 1);
        }
        Node<PrestoExpression> expression = generateExpression(type);
        return new NewCaseOperatorNode<>(expression, conditions, cases, elseExpr);
    }

    private Node<PrestoExpression> getFunction(PrestoSchema.PrestoCompositeDataType type, int depth, PrestoFunction function) {

        PrestoSchema.PrestoDataType[] argumentTypes2 = function.getArgumentTypes(type);
//        List<Node<PrestoExpression>> arguments = getArgumentsForReturnType(gen, depth, argumentTypes2, type);

//        List<Node<PrestoExpression>> getArgumentsForReturnType (PrestoTypedExpressionGenerator gen,int depth,
//        PrestoSchema.PrestoDataType[] argumentTypes2, PrestoCompositeDataType returnType2){
        List<Node<PrestoExpression>> arguments = new ArrayList<>();

        // This is a workaround based on the assumption that array types should refer to the same element type.

        PrestoSchema.PrestoCompositeDataType savedArrayType = null;
//        if (returnType2.getPrimitiveDataType() == PrestoDataType.ARRAY) {
//            savedArrayType = returnType2;
//        }
        for (PrestoSchema.PrestoDataType arg : argumentTypes2) {
            PrestoSchema.PrestoCompositeDataType dataType;
//            if (arg == PrestoDataType.ARRAY) {
//                if (savedArrayType == null) {
//                    savedArrayType = arg.get();
//                }
//                dataType = savedArrayType;
//            } else
            dataType = PrestoSchema.PrestoCompositeDataType.fromDataType(arg);
            arguments.add(generateExpression(dataType, depth + 1));
        }
        return new NewFunctionNode<>(arguments, function);
    }

    private Node<PrestoExpression> generateStringExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode(type);
        }
        return getStringOperation(depth);
    }

    private NewBinaryOperatorNode<PrestoExpression> getStringOperation(int depth) {
        StringExpression exprType = Randomly.fromOptions(StringExpression.values());
        switch (exprType) {
            case CONCAT:
                Node<PrestoExpression> left = generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(VARCHAR), depth + 1);
                Node<PrestoExpression> right = generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(VARCHAR), depth + 1);
                PrestBinaryStringOperator operator = PrestBinaryStringOperator.CONCAT;
                return new NewBinaryOperatorNode<>(left, right, operator);
            default:
                throw new AssertionError(exprType);
        }
    }

    private Node<PrestoExpression> generateBooleanExpression(int depth) {
        List<BooleanExpression> booleanExpressions = new ArrayList<>(List.of(BooleanExpression.values()));
        booleanExpressions.remove(BooleanExpression.BETWEEN);
        BooleanExpression exprType = Randomly.fromList(booleanExpressions);
        switch (exprType) {
            case NOT:
                return generateNOT(depth + 1);
            case BINARY_COMPARISON:
                return getBinaryComparison(depth);
            case BINARY_LOGICAL:
                return getBinaryLogical(depth);
            case AND_OR_CHAIN:
                return getAndOrChain(depth);
//            case REGEX:
//                return getRegex(depth);
            case IS_NULL:
                return new PrestoUnaryPostfixOperation(generateExpression(getRandomType(), depth + 1), Randomly
                        .fromOptions(PrestoUnaryPostfixOperation.PrestoUnaryPostfixOperator.IS_NULL, PrestoUnaryPostfixOperation.PrestoUnaryPostfixOperator.IS_NOT_NULL));
            case IN:
                return getInOperation(depth);
            case BETWEEN:
                return getBetween(depth);
            case LIKE:
                return getLike(depth);
            case MULTI_VALUED_COMPARISON: // TODO other operators
                return getMultiValuedComparison(depth);
            default:
                throw new AssertionError(exprType);
        }
    }

    private Node<PrestoExpression> getMultiValuedComparison(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType.fromDataType(Randomly.fromList(getComparableTypes()));
        PrestoMultiValuedComparisonType comparisonType = PrestoMultiValuedComparisonType.getRandom();
        PrestoMultiValuedComparisonOperator comparisonOperator = PrestoMultiValuedComparisonOperator.getRandomForType(type);
        Node<PrestoExpression> left = generateExpression(type, depth + 1);
        // sub-query
        List<PrestoSchema.PrestoColumn> columnsOfType = columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList());
        if (Randomly.getBooleanWithRatherLowProbability() && !columnsOfType.isEmpty()) {
            PrestoSchema.PrestoColumn column = Randomly.fromList(columnsOfType);
            PrestoSelect subquery = generateSubquery(List.of(column));
            return new PrestoQuantifiedComparison(left, subquery, comparisonType, comparisonOperator);
        }
        int nr = Randomly.smallNumber() + 2;
        List<Node<PrestoExpression>> rightList = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            rightList.add(generateConstant(type));
        }
        return new PrestoMultiValuedComparison(left, rightList, comparisonType, comparisonOperator);
    }

    private PrestoSelect generateSubquery(List<PrestoSchema.PrestoColumn> columns) {
        PrestoSelect select = new PrestoSelect();
        List<Node<PrestoExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<PrestoExpression, PrestoSchema.PrestoColumn>(c)).collect(Collectors.toList());
        select.setFetchColumns(allColumns);
        List<PrestoSchema.PrestoTable> tables = columns.stream()
                .map(AbstractTableColumn::getTable).collect(Collectors.toList());
        List<TableReferenceNode<PrestoExpression, PrestoSchema.PrestoTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<PrestoExpression, PrestoSchema.PrestoTable>(t))
                .distinct()
                .collect(Collectors.toList());
        List<Node<PrestoExpression>> tableNodeList = tables.stream()
                .map(t -> new TableReferenceNode<PrestoExpression, PrestoSchema.PrestoTable>(t))
                .collect(Collectors.toList());
        select.setFromList(tableNodeList);
        TypedExpressionGenerator<Node<PrestoExpression>, PrestoSchema.PrestoColumn, PrestoSchema.PrestoCompositeDataType> typedExpressionGenerator = new PrestoTypedExpressionGenerator(globalState).setColumns(columns);
        Node<PrestoExpression> predicate = typedExpressionGenerator.generatePredicate();
        select.setWhereClause(predicate);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(typedExpressionGenerator.generateOrderBys());
        }
        List<Node<PrestoExpression>> joins = PrestoJoin.getJoins(tableList, globalState);
        select.setJoinList(joins);
        return select;
    }

    private Node<PrestoExpression> generateNumericExpression(int depth) {
        PrestoSchema.PrestoDataType dataType = Randomly.fromList(PrestoSchema.PrestoDataType.getNumberTypes());
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType.fromDataType(dataType);
        if (Randomly.getBoolean()) {
            BinaryOperatorNode.Operator operator = PrestoBinaryArithmeticOperator.getRandom();
            Node<PrestoExpression> left = generateExpression(type, depth);
            Node<PrestoExpression> right = generateExpression(type, depth);
            return new NewBinaryOperatorNode<>(left, right, operator);
        } else {
            BinaryOperatorNode.Operator operator = PrestoUnaryArithmeticOperator.MINUS;
            Node<PrestoExpression> left = generateExpression(type, depth);
            return new NewUnaryPrefixOperatorNode<>(left, operator);
        }
    }

    private Node<PrestoExpression> generateTemporalExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (Randomly.getBooleanWithSmallProbability()) {
            Node<PrestoExpression> left = generateExpression(
                    type, depth);
            Node<PrestoExpression> right = generateExpression(
                    PrestoSchema.PrestoCompositeDataType.fromDataType(
                            Randomly.fromList(PrestoSchema.PrestoDataType.getIntervalTypes())
                    ), depth);
            BinaryOperatorNode.Operator operator = PrestoBinaryTemporalOperator.getRandom();
            return new NewBinaryOperatorNode<>(left, right, operator);
        }

        // timestamp at time zone
        if (Randomly.getBooleanWithSmallProbability()) {
            if (type.getPrimitiveDataType() == TIMESTAMP || type.getPrimitiveDataType() == TIMESTAMP_WITH_TIME_ZONE) {
                return new PrestoAtTimeZoneOperator(
                        generateExpression(type, depth + 1), createTimezoneConstant());
            }
        }
        return generateLeafNode(type);
    }

    private Node<PrestoExpression> generateIntervalExpression(PrestoSchema.PrestoCompositeDataType type, int depth) {
        if (Randomly.getBooleanWithSmallProbability()) {
            Node<PrestoExpression> left = generateExpression(
                    PrestoSchema.PrestoCompositeDataType.fromDataType(
                            Randomly.fromList(PrestoSchema.PrestoDataType.getIntervalTypes())
                    ), depth);

            Node<PrestoExpression> right;
            if (Randomly.getBoolean()) {
                right = generateExpression(
                        PrestoSchema.PrestoCompositeDataType.fromDataType(
                                Randomly.fromList(PrestoSchema.PrestoDataType.getTemporalTypes())
                        ), depth);
            } else {
                right = generateExpression(
                        PrestoSchema.PrestoCompositeDataType.fromDataType(
                                Randomly.fromList(PrestoSchema.PrestoDataType.getIntervalTypes())
                        ), depth);
            }
            BinaryOperatorNode.Operator operator = PrestoBinaryTemporalOperator.getRandom();
            if (Randomly.getBoolean())
                return new NewBinaryOperatorNode<>(left, right, operator);
            else
                return new NewBinaryOperatorNode<>(right, left, operator);
        }
        return generateLeafNode(type);

        // functions

        // timestamp at time zone
    }

    private Node<PrestoExpression> getLike(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType.fromDataType(VARCHAR);
        Node<PrestoExpression> expression = generateExpression(type, depth + 1);
        Node<PrestoExpression> pattern = generateExpression(type, depth + 1);
        if (Randomly.getBoolean()) {
            return new NewBinaryOperatorNode<>(expression, pattern, PrestoLikeOperator.getRandom());
        } else {
            String randomlyString = randomly.getString();
            String randomlyChar = randomly.getChar();
            Node<PrestoExpression> escape = new PrestoConstant.PrestoTextConstant(randomlyChar, 1);
            int index = randomlyString.indexOf(randomlyChar);
            while (index > -1) {
                String wildcard = Randomly.fromOptions("%", "_");
                randomlyString = randomlyString.substring(0, index + 1) + wildcard + randomlyString.substring(index + 1);
                index = randomlyString.indexOf(randomlyChar, index + 1);
            }
            PrestoConstant.PrestoTextConstant patternString = new PrestoConstant.PrestoTextConstant(randomlyString);
            return new NewTernaryNode<>(expression, patternString, escape, "LIKE", "ESCAPE");
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
        BinaryOperatorNode.Operator operator = PrestoBinaryLogicalOperator.getRandom();
        return new NewBinaryOperatorNode<>(left, right, operator);
    }

    private Node<PrestoExpression> getBetween(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType.fromDataType(Randomly.fromList(PrestoSchema.PrestoDataType.getNumericTypes()));
        Node<PrestoExpression> expression = generateExpression(type, depth + 1);
        Node<PrestoExpression> left = generateExpression(type, depth + 1);
        Node<PrestoExpression> right = generateExpression(type, depth + 1);
        return new NewBetweenOperatorNode<>(expression, left, right, Randomly.getBoolean());
    }

    private Node<PrestoExpression> getInOperation(int depth) {
        PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.getRandomWithoutNull());
        Node<PrestoExpression> left = generateExpression(type, depth + 1);
        List<Node<PrestoExpression>> inList = generateExpressions(type, Randomly.smallNumber() + 1, depth + 1);
        boolean isNegated = Randomly.getBoolean();
        return new NewInOperatorNode<>(left, inList, isNegated);
    }

    private Node<PrestoExpression> getAndOrChain(int depth) {
        Node<PrestoExpression> left = generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(BOOLEAN), depth + 1);
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            Node<PrestoExpression> right = generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(BOOLEAN), depth + 1);
            BinaryOperatorNode.Operator operator = PrestoBinaryLogicalOperator.getRandom();
            left = new NewBinaryOperatorNode<>(left, right, operator);
        }
        return left;
    }

    private Node<PrestoExpression> getBinaryComparison(int depth) {
        PrestoSchema.PrestoCompositeDataType type = getRandomType();
        BinaryOperatorNode.Operator op = PrestoBinaryComparisonOperator.getRandomForType(type);
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
        return new PrestoUnaryPrefixOperation(operator, generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(BOOLEAN), depth));
    }

    @Override
    protected Node<PrestoExpression> generateColumn(PrestoSchema.PrestoCompositeDataType type) {
        List<PrestoSchema.PrestoColumn> columnList = columns
                .stream()
                .filter(c -> c.getType().getPrimitiveDataType() == type.getPrimitiveDataType())
                .collect(Collectors.toList());
        PrestoSchema.PrestoColumn column = Randomly.fromList(columnList);
        return new PrestoColumnReference(column);
    }

    @Override
    public Node<PrestoExpression> generateLeafNode(PrestoSchema.PrestoCompositeDataType type) {
        if (Randomly.getBoolean()) {
            return generateConstant(type);
        } else {
            List<PrestoSchema.PrestoColumn> columnList = filterColumns(type.getPrimitiveDataType());
            if (columnList.isEmpty()) {
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
            return columns.stream()
                    .filter(c -> c.getType().getPrimitiveDataType() == dataType)
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

    public NewFunctionNode<PrestoExpression, PrestoAggregateFunction> generateArgsForAggregate
            (PrestoAggregateFunction aggregateFunction) {
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
        ArrayList<PrestoSchema.PrestoColumn> prestoColumns = new ArrayList<>(columns);
        prestoColumns.removeIf(c -> !c.isOrderable());
        for (int i = 0; i < nr && !prestoColumns.isEmpty(); i++) {
            PrestoSchema.PrestoColumn randomColumn = Randomly.fromList(prestoColumns);
            PrestoColumnReference columnReference = new PrestoColumnReference(randomColumn);
            prestoColumns.remove(randomColumn);
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

    public Node<PrestoExpression> generateExpressionWithColumns(List<PrestoSchema.PrestoColumn> columns,
                                                                int remainingDepth) {
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

    private Node<PrestoExpression> generateExpressionWithColumns
            (List<PrestoSchema.PrestoColumn> columns, PrestoSchema.PrestoCompositeDataType prestoCompositeDataType,
             int remainingDepth) {
        return null;
    }

    private Node<PrestoExpression> generatePredicate
            (List<PrestoSchema.PrestoColumn> columns, PrestoSchema.PrestoCompositeDataType prestoCompositeDataType,
             int remainingDepth) {
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

    private enum TemporalExpression {
        CONCAT
    }

    public enum PrestoBinaryTemporalOperator implements BinaryOperatorNode.Operator {
        ADD("+"), SUB("-");

        private String textRepr;

        PrestoBinaryTemporalOperator(String textRepr) {
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

    private enum BooleanExpression {
        NOT,
        BINARY_COMPARISON,
        BINARY_LOGICAL,
        AND_OR_CHAIN,
        //        REGEX,
        IS_NULL,
        IN,
        BETWEEN,
        LIKE,
        MULTI_VALUED_COMPARISON
    }

    public enum PrestoBinaryLogicalOperator implements BinaryOperatorNode.Operator {

        AND, OR;

        public static BinaryOperatorNode.Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }

    }

    public enum PrestoLikeOperator implements BinaryOperatorNode.Operator {
        LIKE("LIKE"), //
        NOT_LIKE("NOT LIKE");

        private String textRepr;

        PrestoLikeOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static PrestoLikeOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public enum PrestoBinaryComparisonOperator implements BinaryOperatorNode.Operator {
        EQUALS("="),
        NOT_EQUALS("<>"), NOT_EQUALS_ALT("!="),
        IS_DISTINCT_FROM("IS DISTINCT FROM"), IS_NOT_DISTINCT_FROM("IS NOT DISTINCT FROM"),
        GREATER(">"), GREATER_EQUALS(">="),
        SMALLER("<"), SMALLER_EQUALS("<=");

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

        public static BinaryOperatorNode.Operator getRandomForType(PrestoSchema.PrestoCompositeDataType type) {
            PrestoSchema.PrestoDataType dataType = type.getPrimitiveDataType();

            switch (dataType) {
                case BOOLEAN:
                case INT:
                case FLOAT:
                case DECIMAL:
                case DATE:
                case TIME:
                case TIMESTAMP:
                case TIME_WITH_TIME_ZONE:
                case TIMESTAMP_WITH_TIME_ZONE:
                    return getRandom();
                case VARCHAR:
                case CHAR:
                case VARBINARY:
                case JSON:
                case INTERVAL_YEAR_TO_MONTH:
                case INTERVAL_DAY_TO_SECOND:
                    return Randomly.fromOptions(EQUALS, NOT_EQUALS, NOT_EQUALS_ALT, IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM);
                default:
                    return Randomly.fromOptions(EQUALS, NOT_EQUALS, NOT_EQUALS_ALT, IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM);
            }
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

    }

    public enum PrestoBinaryArithmeticOperator implements BinaryOperatorNode.Operator {
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

    public enum PrestoUnaryArithmeticOperator implements BinaryOperatorNode.Operator {
        MINUS("-");

        private String textRepr;

        PrestoUnaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    private enum Expression {
        BINARY_LOGICAL, BINARY_COMPARISON, BINARY_ARITHMETIC
    }

}
