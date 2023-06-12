package sqlancer.presto.test;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.*;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;
import sqlancer.presto.PrestoSchema.PrestoDataType;
import sqlancer.presto.PrestoToStringVisitor;
import sqlancer.presto.ast.PrestoCastFunction;
import sqlancer.presto.ast.PrestoExpression;
import sqlancer.presto.ast.PrestoSelect;
import sqlancer.presto.ast.PrestoAggregateFunctionOld;
import sqlancer.presto.gen.PrestoUntypedExpressionGenerator.PrestoBinaryArithmeticOperator;
import sqlancer.presto.gen.PrestoUntypedExpressionGenerator.PrestoUnaryPostfixOperator;
import sqlancer.presto.gen.PrestoUntypedExpressionGenerator.PrestoUnaryPrefixOperator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PrestoQueryPartitioningAggregateTester extends PrestoQueryPartitioningBase
    implements TestOracle<PrestoGlobalState> {

    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public PrestoQueryPartitioningAggregateTester(PrestoGlobalState state) {
        super(state);
        PrestoErrors.addGroupByErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        PrestoAggregateFunctionOld aggregateFunction = Randomly.fromOptions(PrestoAggregateFunctionOld.MAX,
            PrestoAggregateFunctionOld.MIN, PrestoAggregateFunctionOld.SUM, PrestoAggregateFunctionOld.COUNT,
            PrestoAggregateFunctionOld.AVG/* , PrestoAggregateFunction.STDDEV_POP */);
        NewFunctionNode<PrestoExpression, PrestoAggregateFunctionOld> aggregate = gen
            .generateArgsForAggregate(aggregateFunction);
        List<Node<PrestoExpression>> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add(gen.generateAggregate());
        }
        select.setFetchColumns(List.of(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        originalQuery = PrestoToStringVisitor.asString(select);
        firstResult = getAggregateResult(originalQuery);
        metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        secondResult = getAggregateResult(metamorphicQuery);

        state.getState().getLocalState().log(
            "--" + originalQuery + ";\n--" + metamorphicQuery + "\n-- " + firstResult + "\n-- " + secondResult);
        if (firstResult == null && secondResult != null
            || firstResult != null && (!firstResult.contentEquals(secondResult)
            && !ComparatorHelper.isEqualDouble(firstResult, secondResult))) {
            if (secondResult.contains("Inf")) {
                throw new IgnoreMeException(); // FIXME: average computation
            }
            throw new AssertionError();
        }

    }

    private String createMetamorphicUnionQuery(PrestoSelect select,
                                               NewFunctionNode<PrestoExpression, PrestoAggregateFunctionOld> aggregate, List<Node<PrestoExpression>> from) {
        String metamorphicQuery;
        Node<PrestoExpression> whereClause = gen.generateExpression(PrestoCompositeDataType.getRandomWithoutNull());
        Node<PrestoExpression> negatedClause = new NewUnaryPrefixOperatorNode<>(whereClause,
            PrestoUnaryPrefixOperator.NOT);
        Node<PrestoExpression> notNullClause = new NewUnaryPostfixOperatorNode<>(whereClause,
            PrestoUnaryPostfixOperator.IS_NULL);
        List<Node<PrestoExpression>> mappedAggregate = mapped(aggregate);
        PrestoSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinList());
        PrestoSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinList());
        PrestoSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinList());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += PrestoToStringVisitor.asString(leftSelect) + " UNION ALL "
            + PrestoToStringVisitor.asString(middleSelect) + " UNION ALL "
            + PrestoToStringVisitor.asString(rightSelect);
        metamorphicQuery += ") as asdf";
        return metamorphicQuery;
    }

    private String getAggregateResult(String queryString) throws SQLException {
        String resultString;
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        try (SQLancerResultSet result = q.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            if (!result.next()) {
                resultString = null;
            } else {
                resultString = result.getString(1);
            }
            return resultString;
        } catch (SQLException e) {
            if (!e.getMessage().contains("Not implemented type")) {
                throw new AssertionError(queryString, e);
            } else {
                throw new IgnoreMeException();
            }
        }
    }

    private List<Node<PrestoExpression>> mapped(NewFunctionNode<PrestoExpression, PrestoAggregateFunctionOld> aggregate) {
        PrestoCastFunction count;
        switch (aggregate.getFunc()) {
            case COUNT:
            case MAX:
            case MIN:
            case SUM:
                return aliasArgs(List.of(aggregate));
            case AVG:
                NewFunctionNode<PrestoExpression, PrestoAggregateFunctionOld> sum = new NewFunctionNode<>(aggregate.getArgs(),
                    PrestoAggregateFunctionOld.SUM);
                count = new PrestoCastFunction(new NewFunctionNode<>(aggregate.getArgs(), PrestoAggregateFunctionOld.COUNT),
                    new PrestoCompositeDataType(PrestoDataType.FLOAT, 8, 0));
                return aliasArgs(Arrays.asList(sum, count));
            case STDDEV_POP:
                NewFunctionNode<PrestoExpression, PrestoAggregateFunctionOld> sumSquared = new NewFunctionNode<>(
                    List.of(new NewBinaryOperatorNode<>(aggregate.getArgs().get(0), aggregate.getArgs().get(0),
                        PrestoBinaryArithmeticOperator.MULT)),
                    PrestoAggregateFunctionOld.SUM);
                count = new PrestoCastFunction(
                    new NewFunctionNode<PrestoExpression, PrestoAggregateFunctionOld>(aggregate.getArgs(),
                        PrestoAggregateFunctionOld.COUNT),
                    new PrestoCompositeDataType(PrestoDataType.FLOAT, 8, 0));
                NewFunctionNode<PrestoExpression, PrestoAggregateFunctionOld> avg = new NewFunctionNode<>(aggregate.getArgs(),
                    PrestoAggregateFunctionOld.AVG);
                return aliasArgs(Arrays.asList(sumSquared, count, avg));
            default:
                throw new AssertionError(aggregate.getFunc());
        }
    }

    private List<Node<PrestoExpression>> aliasArgs(List<Node<PrestoExpression>> originalAggregateArgs) {
        List<Node<PrestoExpression>> args = new ArrayList<>();
        int i = 0;
        for (Node<PrestoExpression> expr : originalAggregateArgs) {
            args.add(new NewAliasNode<PrestoExpression>(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(NewFunctionNode<PrestoExpression, PrestoAggregateFunctionOld> aggregate) {
        switch (aggregate.getFunc()) {
            case STDDEV_POP:
                return "sqrt(SUM(agg0)/SUM(agg1)-SUM(agg2)*SUM(agg2))";
            case AVG:
                return "SUM(agg0::FLOAT)/SUM(agg1)::FLOAT";
            case COUNT:
                return PrestoAggregateFunctionOld.SUM + "(agg0)";
            default:
                return aggregate.getFunc().toString() + "(agg0)";
        }
    }

    private PrestoSelect getSelect(List<Node<PrestoExpression>> aggregates, List<Node<PrestoExpression>> from,
                                   Node<PrestoExpression> whereClause, List<Node<PrestoExpression>> joinList) {
        PrestoSelect leftSelect = new PrestoSelect();
        leftSelect.setFetchColumns(aggregates);
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        leftSelect.setJoinList(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            leftSelect.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        return leftSelect;
    }

}
