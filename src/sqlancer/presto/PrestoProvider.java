package sqlancer.presto;

import com.google.auto.service.AutoService;
import sqlancer.*;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.presto.gen.PrestoInsertGenerator;
import sqlancer.presto.gen.PrestoTableGenerator;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@AutoService(DatabaseProvider.class)
public class PrestoProvider extends SQLProviderAdapter<PrestoGlobalState, PrestoOptions> {

    public PrestoProvider() {
        super(PrestoGlobalState.class, PrestoOptions.class);
    }

    public enum Action implements AbstractAction<PrestoGlobalState> {
        //        SHOW_TABLES((g) -> new SQLQueryAdapter("SHOW TABLES")), //
        INSERT(PrestoInsertGenerator::getQuery);
        //        , //
        //        CREATE_INDEX(PrestoIndexGenerator::getQuery), //
        //        VACUUM((g) -> new SQLQueryAdapter("VACUUM")), //
        //        ANALYZE((g) -> new SQLQueryAdapter("ANALYZE")), //
        //        DELETE(PrestoDeleteGenerator::generate), //
        //        UPDATE(PrestoUpdateGenerator::getQuery), //
        //        CREATE_VIEW(PrestoViewGenerator::generate), //
        //        EXPLAIN((g) -> {
        //            ExpectedErrors errors = new ExpectedErrors();
        //            PrestoErrors.addExpressionErrors(errors);
        //            PrestoErrors.addGroupByErrors(errors);
        //            return new SQLQueryAdapter(
        //                "EXPLAIN " + PrestoToStringVisitor
        //                    .asString(PrestoRandomQuerySynthesizer.generateSelect(g, Randomly.smallNumber() + 1)),
        //                errors);
        //        });

        private final SQLQueryProvider<PrestoGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<PrestoGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(PrestoGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    // BANE:
    // returns number of actions
    private static int mapActions(PrestoGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        if (Objects.requireNonNull(a) == Action.INSERT) {
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            //            case UPDATE:
            //                return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumUpdates + 1);
            //        case VACUUM: // seems to be ignored
            //            case ANALYZE: // seems to be ignored
            //            case EXPLAIN:
            //                return r.getInteger(0, 2);
            //            case DELETE:
            //                return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumDeletes + 1);
            //            case CREATE_VIEW:
            //                return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumViews + 1);
        }
        throw new AssertionError(a);
    }

    @Override
    public void generateDatabase(PrestoGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new PrestoTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException(); // TODO
        }
        StatementExecutor<PrestoGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
            PrestoProvider::mapActions, (q) -> {
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(PrestoGlobalState globalState) throws SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        boolean useSSl = true;
        if (globalState.getOptions().isDefaultUsername() && globalState.getOptions().isDefaultPassword()) {
            username = "presto";
            password = null;
            useSSl = false;
        }
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = PrestoOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = PrestoOptions.DEFAULT_PORT;
        }
        String catalogName = globalState.getDbmsSpecificOptions().catalog;
        String databaseName = globalState.getDatabaseName();
        globalState.getState().logStatement("DROP SCHEMA IF EXISTS " + catalogName + "." + databaseName);
        globalState.getState().logStatement("CREATE SCHEMA IF NOT EXISTS " + catalogName + "." + databaseName);
        globalState.getState().logStatement("USE " + catalogName + "." + databaseName);
        String url = String.format("jdbc:presto://%s:%d/%s?SSL=%b",
            host, port, catalogName, useSSl);
        Connection con = DriverManager.getConnection(url, username, password);
        List<String> schemaNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SHOW SCHEMAS FROM " + catalogName + " LIKE '" + databaseName + "'")) {
                while (rs.next()) {
                    schemaNames.add(rs.getString("Schema"));
                }
            }
        }
        if (!schemaNames.isEmpty()) {
            List<String> tableNames = new ArrayList<>();
            try (Statement s = con.createStatement()) {
                try (ResultSet rs = s.executeQuery("SHOW TABLES FROM " + catalogName + "." + databaseName)) {
                    while (rs.next()) {
                        tableNames.add(rs.getString("Table"));
                    }
                }
            }
            try (Statement s = con.createStatement()) {
                for (String tableName : tableNames) {
                    s.execute("DROP TABLE IF EXISTS " + catalogName + "." + databaseName + "." + tableName);
                }
            }
        }
        try (Statement s = con.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS " + catalogName + "." + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS " + catalogName + "." + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + catalogName + "." + databaseName);
        }
        return new SQLConnection(con);

    }

    @Override
    public String getDBMSName() {
        return "presto";
    }

}
