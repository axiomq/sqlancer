package sqlancer.presto.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.*;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema.PrestoColumn;
import sqlancer.presto.PrestoSchema.PrestoDataType;
import sqlancer.presto.ast.PrestoConstant;
import sqlancer.presto.ast.PrestoExpression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class PrestoUntypedExpressionGenerator extends UntypedExpressionGenerator<Node<PrestoExpression>, PrestoColumn> {

    private final PrestoGlobalState globalState;

    public PrestoUntypedExpressionGenerator(PrestoGlobalState globalState) {
        this.globalState = globalState;
    }

    //    @formatter:off
    private enum Expression {
//        UNARY_POSTFIX
//        , UNARY_PREFIX
//        ,
        BINARY_COMPARISON
        , BINARY_LOGICAL
        , BINARY_ARITHMETIC
//        , CAST
//        , FUNC
//        , BETWEEN
//        , CASE
//        , IN
//        , LIKE_ESCAPE
    }
    // @formatter:on

    @Override
    protected Node<PrestoExpression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            Node<PrestoExpression> prestoExpressionNode = generateLeafNode();
            return prestoExpressionNode;
        }
//        if (allowAggregates && Randomly.getBoolean()) {
        if (false) {
            PrestoAggregateFunction aggregate = PrestoAggregateFunction.getRandom();
            allowAggregates = false;
            NewFunctionNode<PrestoExpression, PrestoAggregateFunction> functionNode = new NewFunctionNode<>(generateExpressions(aggregate.getNrArgs(), depth + 1), aggregate);
            return functionNode;
        }
        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
//        if (!globalState.getDbmsSpecificOptions().testFunctions) {
//            possibleOptions.remove(Expression.FUNC);
//        }
//        if (!globalState.getDbmsSpecificOptions().testCasts) {
//            possibleOptions.remove(Expression.CAST);
//        }
//        if (!globalState.getDbmsSpecificOptions().testBetween) {
//            possibleOptions.remove(Expression.BETWEEN);
//        }
//        if (!globalState.getDbmsSpecificOptions().testIn) {
//            possibleOptions.remove(Expression.IN);
//        }
//        if (!globalState.getDbmsSpecificOptions().testCase) {
//            possibleOptions.remove(Expression.CASE);
//        }
//        if (!globalState.getDbmsSpecificOptions().testBinaryComparisons) {
//            possibleOptions.remove(Expression.BINARY_COMPARISON);
//        }
//        if (!globalState.getDbmsSpecificOptions().testBinaryLogicals) {
//            possibleOptions.remove(Expression.BINARY_LOGICAL);
//        }
        Expression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
//            case UNARY_PREFIX:
//                return new NewUnaryPrefixOperatorNode<PrestoExpression>(generateExpression(depth + 1),
//                    PrestoUnaryPrefixOperator.getRandom());
//            case UNARY_POSTFIX:
//                return new NewUnaryPostfixOperatorNode<PrestoExpression>(generateExpression(depth + 1),
//                    PrestoUnaryPostfixOperator.getRandom());
            case BINARY_COMPARISON:
                Operator operator = PrestoBinaryComparisonOperator.getRandom();
//                type
                return new NewBinaryOperatorNode<PrestoExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), operator);
            case BINARY_LOGICAL:
                operator = PrestoBinaryLogicalOperator.getRandom();
                NewBinaryOperatorNode<PrestoExpression> binaryOperatorNode = new NewBinaryOperatorNode<>(generateExpression(depth + 1),
                    generateExpression(depth + 1), operator);
                return binaryOperatorNode;
            case BINARY_ARITHMETIC:
                return new NewBinaryOperatorNode<PrestoExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), PrestoBinaryArithmeticOperator.getRandom());
//            case CAST:
//                return new PrestoCastOperation(generateExpression(depth + 1),
//                    PrestoCompositeDataType.getRandomWithoutNull());
//            case FUNC:
//                DBFunction func = DBFunction.getRandom();
//                return new NewFunctionNode<PrestoExpression, DBFunction>(generateExpressions(func.getNrArgs()), func);
//            case BETWEEN:
//                return new NewBetweenOperatorNode<PrestoExpression>(generateExpression(depth + 1),
//                    generateExpression(depth + 1), generateExpression(depth + 1), Randomly.getBoolean());
//            case IN:
//                return new NewInOperatorNode<PrestoExpression>(generateExpression(depth + 1),
//                    generateExpressions(Randomly.smallNumber() + 1, depth + 1), Randomly.getBoolean());
//            case CASE:
//                int nr = Randomly.smallNumber() + 1;
//                return new NewCaseOperatorNode<PrestoExpression>(generateExpression(depth + 1),
//                    generateExpressions(nr, depth + 1), generateExpressions(nr, depth + 1),
//                    generateExpression(depth + 1));
//            case LIKE_ESCAPE:
//                return new NewBinaryOperatorNode<PrestoExpression>(generateExpression(depth + 1), generateExpression(depth + 1),
//                    generateExpression(depth + 1), "LIKE");
            default:
                throw new AssertionError();
        }
    }

    @Override
    protected Node<PrestoExpression> generateColumn() {
        PrestoColumn column = Randomly.fromList(columns);
        return new ColumnReferenceNode<PrestoExpression, PrestoColumn>(column);
    }

    @Override
    public Node<PrestoExpression> generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            return PrestoConstant.createNullConstant();
        }
        PrestoDataType type = PrestoDataType.getRandomWithoutNull();
        return generateConstant(type, -1, -1);
    }

    public Node<PrestoExpression> generateConstant(PrestoColumn prestoColumn) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return PrestoConstant.createNullConstant();
        }
        PrestoDataType type = prestoColumn.getType().getPrimitiveDataType();
        int size = prestoColumn.getType().getSize();
        int scale = prestoColumn.getType().getScale();
        return generateConstant(type, size, scale);
    }

    private Node<PrestoExpression> generateConstant(PrestoDataType type, int size, int scale) {
        switch (type) {
            case INT:
                if (!globalState.getDbmsSpecificOptions().testIntConstants) {
                    throw new IgnoreMeException();
                }
                return PrestoConstant.createIntConstant(globalState.getRandomly().getInteger());
            case DATE:
                if (!globalState.getDbmsSpecificOptions().testDateConstants) {
                    throw new IgnoreMeException();
                }
                return PrestoConstant.createDateConstant(globalState.getRandomly().getInteger());
            case TIME:
                if (!globalState.getDbmsSpecificOptions().testTimeConstants) {
                    throw new IgnoreMeException();
                }
                return PrestoConstant.createTimeConstant(globalState.getRandomly().getInteger());
            case TIMESTAMP:
                if (!globalState.getDbmsSpecificOptions().testTimestampConstants) {
                    throw new IgnoreMeException();
                }
                return PrestoConstant.createTimestampConstant(globalState.getRandomly().getInteger());
            case VARCHAR:
                if (!globalState.getDbmsSpecificOptions().testStringConstants) {
                    throw new IgnoreMeException();
                }
                return PrestoConstant.createStringConstant(globalState.getRandomly().getString());
            case CHAR:
                if (!globalState.getDbmsSpecificOptions().testStringConstants) {
                    throw new IgnoreMeException();
                }
                return PrestoConstant.createStringConstant(globalState.getRandomly().getString());
            case BOOLEAN:
                if (!globalState.getDbmsSpecificOptions().testBooleanConstants) {
                    throw new IgnoreMeException();
                }
                return PrestoConstant.createBooleanConstant(Randomly.getBoolean());
            case FLOAT:
                if (!globalState.getDbmsSpecificOptions().testFloatConstants) {
                    throw new IgnoreMeException();
                }
                return PrestoConstant.createFloatConstant(globalState.getRandomly().getDouble());
            case DECIMAL:
                if (!globalState.getDbmsSpecificOptions().testFloatConstants) {
                    throw new IgnoreMeException();
                }
                return PrestoConstant.createDecimalConstant(globalState.getRandomly().getDouble());
            default:
//                System.out.print("PrestoExpressionGenerator.generateConstant ");
//                System.out.println("... AssertionError ..." + type.name());
                throw new AssertionError();
        }
    }

    @Override
    public List<Node<PrestoExpression>> generateOrderBys() {
        List<Node<PrestoExpression>> expr = super.generateOrderBys();
        List<Node<PrestoExpression>> newExpr = new ArrayList<>(expr.size());
        for (Node<PrestoExpression> curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new NewOrderingTerm<>(curExpr, Ordering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
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

    public enum DBFunction {
        // trigonometric functions
        ACOS(1), //
        ASIN(1), //
        ATAN(1), //
        COS(1), //
        SIN(1), //
        TAN(1), //
        COT(1), //
        ATAN2(1), //
        // math functions
        ABS(1), //
        CEIL(1), //
        CEILING(1), //
        FLOOR(1), //
        LOG(1), //
        LOG10(1), LOG2(1), //
        LN(1), //
        PI(0), //
        SQRT(1), //
        POWER(1), //
        CBRT(1), //
        ROUND(2), //
        SIGN(1), //
        DEGREES(1), //
        RADIANS(1), //
        MOD(2), //
        XOR(2), //
        // string functions
        LENGTH(1), //
        LOWER(1), //
        UPPER(1), //
        SUBSTRING(3), //
        REVERSE(1), //
        CONCAT(1, true), //
        CONCAT_WS(1, true), CONTAINS(2), //
        PREFIX(2), //
        SUFFIX(2), //
        INSTR(2), //
        PRINTF(1, true), //
        REGEXP_MATCHES(2), //
        REGEXP_REPLACE(3), //
        STRIP_ACCENTS(1), //

        // date functions
        DATE_PART(2), AGE(2),

        COALESCE(3), NULLIF(2),

        // LPAD(3),
        // RPAD(3),
        LTRIM(1), RTRIM(1),
        // LEFT(2), https://github.com/cwida/presto/issues/633
        // REPEAT(2),
        REPLACE(3), UNICODE(1),

        BIT_COUNT(1), BIT_LENGTH(1), LAST_DAY(1), MONTHNAME(1), DAYNAME(1), YEARWEEK(1), DAYOFMONTH(1), WEEKDAY(1),
        WEEKOFYEAR(1),

        IFNULL(2), IF(3);

        private final int nrArgs;
        private final boolean isVariadic;

        DBFunction(int nrArgs) {
            this(nrArgs, false);
        }

        DBFunction(int nrArgs, boolean isVariadic) {
            this.nrArgs = nrArgs;
            this.isVariadic = isVariadic;
        }

        public static DBFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            if (isVariadic) {
                return Randomly.smallNumber() + nrArgs;
            } else {
                return nrArgs;
            }
        }

    }

    public enum PrestoUnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private final String textRepr;

        PrestoUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static PrestoUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum PrestoUnaryPrefixOperator implements Operator {

        NOT("NOT"), PLUS("+"), MINUS("-");

        private final String textRepr;

        PrestoUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static PrestoUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum PrestoBinaryLogicalOperator implements Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum PrestoBinaryArithmeticOperator implements Operator {
        CONCAT("||"),
        ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"),
//        AND("&"), OR("|"),
//        LSHIFT("<<"), RSHIFT(">>")
        ;

        private final String textRepr;

        PrestoBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public enum PrestoBinaryComparisonOperator implements Operator {
        EQUALS("="), NOT_EQUALS("!="),
        GREATER(">", PrestoDataType.getNumericTypes()),
        GREATER_EQUALS(">=", PrestoDataType.getNumericTypes()),
        SMALLER("<", PrestoDataType.getNumericTypes()),
        SMALLER_EQUALS("<=", PrestoDataType.getNumericTypes()),
        LIKE("LIKE", PrestoDataType.getTextTypes()),
        NOT_LIKE("NOT LIKE", PrestoDataType.getTextTypes())
//        , SIMILAR_TO("SIMILAR TO"), NOT_SIMILAR_TO("NOT SIMILAR TO")
//        ,
//        REGEX_POSIX("~"), REGEX_POSIT_NOT("!~")
        ;

        private final List<PrestoDataType> types;
        private final String textRepresentation;

        PrestoBinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
            this.types = List.of(PrestoDataType.values());
        }

        PrestoBinaryComparisonOperator(String textRepresentation, List<PrestoDataType> types) {
            this.textRepresentation = textRepresentation;
            this.types = types;
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        public static Operator getRandom(PrestoDataType type) {
            return Randomly.fromOptions(
                Arrays.stream(
                    values()).filter(operator -> operator.getTypes().contains(type)).toArray(Operator[]::new)
            );
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public List<PrestoDataType> getTypes() {
            return types;
        }

    }

    public NewFunctionNode<PrestoExpression, PrestoAggregateFunction> generateArgsForAggregate(
        PrestoAggregateFunction aggregateFunction) {
        return new NewFunctionNode<PrestoExpression, PrestoAggregateFunction>(
            generateExpressions(aggregateFunction.getNrArgs()), aggregateFunction);
    }

    public Node<PrestoExpression> generateAggregate() {
        PrestoAggregateFunction aggrFunc = PrestoAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    @Override
    public Node<PrestoExpression> negatePredicate(Node<PrestoExpression> predicate) {
        return new NewUnaryPrefixOperatorNode<>(predicate, PrestoUnaryPrefixOperator.NOT);
    }

    @Override
    public Node<PrestoExpression> isNull(Node<PrestoExpression> expr) {
        return new NewUnaryPostfixOperatorNode<>(expr, PrestoUnaryPostfixOperator.IS_NULL);
    }

}
