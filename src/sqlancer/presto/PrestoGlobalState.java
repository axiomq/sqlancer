package sqlancer.presto;

import java.sql.SQLException;

import sqlancer.SQLGlobalState;

public class PrestoGlobalState extends SQLGlobalState<PrestoOptions, PrestoSchema> {

    @Override
    protected PrestoSchema readSchema() throws SQLException {
        return PrestoSchema.fromConnection(getConnection(), getDatabaseName());
    }

    // BANE: check
    // public boolean usesPQS() {
    // return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == MySQLOptions.MySQLOracleFactory.PQS);
    // }

}
