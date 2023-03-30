package sqlancer.presto;

import sqlancer.common.ast.newast.NewToStringVisitor;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.ast.*;

public class PrestoToStringVisitor extends NewToStringVisitor<PrestoExpression> {

    @Override
    public void visitSpecific(Node<PrestoExpression> expr) {
        if (expr instanceof PrestoConstant) {
            visit((PrestoConstant) expr);
        } else if (expr instanceof PrestoSelect) {
            visit((PrestoSelect) expr);
        } else if (expr instanceof PrestoJoin) {
            visit((PrestoJoin) expr);
        } else if (expr instanceof PrestoCastFunction) {
            visit((PrestoCastFunction) expr);
        } else {
            throw new AssertionError(expr.getClass());
        }
    }

    private void visit(PrestoJoin join) {
        visit(join.getLeftTable());
        sb.append(" ");
        sb.append(join.getJoinType());
        sb.append(" ");
        if (join.getOuterType() != null) {
            sb.append(join.getOuterType());
        }
        sb.append(" JOIN ");
        visit(join.getRightTable());
        if (join.getOnCondition() != null) {
            sb.append(" ON ");
            visit(join.getOnCondition());
        }
    }

    private void visit(PrestoConstant constant) {
        sb.append(constant.toString());
    }

    private void visit(PrestoSelect select) {
        sb.append("SELECT ");
        if (select.isDistinct()) {
            sb.append("DISTINCT ");
        }
        visit(select.getFetchColumns());
        sb.append(" FROM ");
        visit(select.getFromList());
        if (!select.getFromList().isEmpty() && !select.getJoinList().isEmpty()) {
            sb.append(", ");
        }
        if (!select.getJoinList().isEmpty()) {
            visit(select.getJoinList());
        }
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
        if (!select.getGroupByExpressions().isEmpty()) {
            sb.append(" GROUP BY ");
            visit(select.getGroupByExpressions());
        }
        if (select.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(select.getHavingClause());
        }
        if (!select.getOrderByExpressions().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(select.getOrderByExpressions());
        }
        if (select.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(select.getLimitClause());
        }
        if (select.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(select.getOffsetClause());
        }
    }

    public static String asString(Node<PrestoExpression> expr) {
        PrestoToStringVisitor visitor = new PrestoToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    public void visit(PrestoCastFunction cast) {
        sb.append("CAST((");
        visit(cast.getExpr());
        sb.append(") AS ");
        sb.append(cast.getType().toString());
        sb.append(")");
    }

}
