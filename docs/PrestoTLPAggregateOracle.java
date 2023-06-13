package sqlancer.presto.test;

import org.postgresql.util.PSQLException;
import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.NewAliasNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.presto.PrestoErrors;
import sqlancer.presto.PrestoGlobalState;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoToStringVisitor;
import sqlancer.presto.ast.*;
import sqlancer.presto.gen.PrestoTypedExpressionGenerator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class PrestoTLPAggregateOracle implements TestOracle<PrestoGlobalState> {

    private final PrestoGlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();
    private PrestoTypedExpressionGenerator gen;
    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public PrestoTLPAggregateOracle(PrestoGlobalState state) {
        this.state = state;
        PrestoErrors.addExpressionErrors(errors);
        errors.add("interface conversion: coldata.column");
        errors.add("float out of range");
    }

    @Override
    public void check() throws SQLException {
        PrestoSchema s = state.getSchema();
        PrestoSchema.PrestoTables targetTables = s.getRandomTableNonEmptyTables();
        gen = new PrestoTypedExpressionGenerator(state).setColumns(targetTables.getColumns());
        PrestoSelect select = new PrestoSelect();
        PrestoAggregateFunction prestoAggregateFunction = Randomly.fromOptions(PrestoAggregateFunction.getRandomMetamorphicOracle());
        List<Node<PrestoExpression>> argsForAggregate = gen.generateArgsForAggregate(PrestoSchema.PrestoCompositeDataType.fromDataType(prestoAggregateFunction.getReturnType()),
                prestoAggregateFunction);

        NewFunctionNode<PrestoExpression, PrestoAggregateFunction> aggregateFunction = new NewFunctionNode<>(argsForAggregate, prestoAggregateFunction);

        List<Node<PrestoExpression>> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregateFunction);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add(gen.generateAggregate());
        }
        select.setFetchColumns(List.of(aggregateFunction));
        List<PrestoTableReference> tableList = targetTables.getTables().stream()
                .map(PrestoTableReference::new)
                .collect(Collectors.toList());
        List<Node<PrestoExpression>> from = tableList.stream().map(t -> (Node<PrestoExpression>) t).collect(Collectors.toList());
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setJoinList(PrestoJoin.getJoins(tableList.stream().map(t -> (TableReferenceNode<PrestoExpression, PrestoSchema.PrestoTable>) t).collect(Collectors.toList()), state));
        }
        select.setFromList(from);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        originalQuery = PrestoToStringVisitor.asString(select);
        firstResult = getAggregateResult(originalQuery);
        metamorphicQuery = createMetamorphicUnionQuery(select, aggregateFunction, from);
        secondResult = getAggregateResult(metamorphicQuery);

        state.getState().getLocalState().log(
                "--" + originalQuery + ";\n--" + metamorphicQuery + "\n-- " + firstResult + "\n-- " + secondResult);
        if (firstResult == null && secondResult != null || firstResult != null && secondResult == null
                || firstResult != null && secondResult != null && (!firstResult.contentEquals(secondResult)
                && !ComparatorHelper.isEqualDouble(firstResult, secondResult))) {
            if (secondResult.contains("Inf")) {
                throw new IgnoreMeException(); // FIXME: average computation
            }
            throw new AssertionError();
        }

    }

    private String createMetamorphicUnionQuery(PrestoSelect select, Node<PrestoExpression> aggregate,
                                               List<Node<PrestoExpression>> from) {
        String metamorphicQuery;
        Node<PrestoExpression> whereClause = gen.generateExpression(PrestoSchema.PrestoCompositeDataType.fromDataType(PrestoSchema.PrestoDataType.BOOLEAN));

        Node<PrestoExpression> negatedClause = gen.negatePredicate(whereClause);

        Node<PrestoExpression> notNullClause = gen.isNull(whereClause);

        List<PrestoExpression> mappedAggregate = mapped(aggregate);
        PrestoSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinList());
        PrestoSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinList());
        PrestoSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinList());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += PrestoVisitor.asString(leftSelect) + " UNION ALL "
                + PrestoVisitor.asString(middleSelect) + " UNION ALL " + PrestoVisitor.asString(rightSelect);
        metamorphicQuery += ")";
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
        } catch (PSQLException e) {
            throw new AssertionError(queryString, e);
        }
        return resultString;
    }

    private List<PrestoExpression> mapped(Node<PrestoExpression> aggregate) {
        NewFunctionNode<List<Node<PrestoExpression>>, PrestoAggregateFunction> agg = (NewFunctionNode<List<Node<PrestoExpression>>, PrestoAggregateFunction>) aggregate;
        switch (agg.getFunc()) {
            case SUM:
            case COUNT:
            case BOOL_AND:
            case BOOL_OR:
            case MAX:
            case MIN:
                return aliasArgs(Arrays.asList(aggregate));
            case AVG:
                // List<PrestoExpression> arg = Arrays.asList(new PrestoCast(aggregate.getExpr().get(0),
                // PrestoDataType.DECIMAL.get()));
//                PrestoAggregateFunction sum = new PrestoAggregate(PrestoAggregateFunction.SUM, aggregate.getExpr());

                Node<PrestoExpression> sum = new NewFunctionNode<>(aggregate.getx, PrestoAggregateFunction.SUM);

                PrestoCastFunction count = new PrestoCastFunction(
                        new PrestoAggregate(PrestoAggregateFunction.COUNT, aggregate.getExpr()),
                        PrestoSchema.PrestoDataType.DECIMAL.get());
                // PrestoBinaryArithmeticOperation avg = new PrestoBinaryArithmeticOperation(sum, count,
                // PrestoBinaryArithmeticOperator.DIV);
                return aliasArgs(Arrays.asList(sum, count));
            default:
                throw new AssertionError(aggregate);
        }
    }

    private List<Node<PrestoExpression>> aliasArgs(List<Node<PrestoExpression>> originalAggregateArgs) {
        List<Node<PrestoExpression>> args = new ArrayList<>();
        int i = 0;
        for (Node<PrestoExpression> expr : originalAggregateArgs) {
            args.add(new NewAliasNode<>(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(PrestoAggregateFunction aggregate) {
        switch (aggregate) {
            case AVG:
                return "SUM(agg0::DECIMAL)/SUM(agg1)::DECIMAL";
            case COUNT:
            case COUNT_ALL:
                return PrestoAggregateFunction.SUM.toString() + "(agg0)";
            default:
                return aggregate + "(agg0)";
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

    @Override
    public String getLastQueryString() {
        return originalQuery;
    }

}
