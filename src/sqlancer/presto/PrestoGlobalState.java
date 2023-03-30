package sqlancer.presto;

import sqlancer.SQLGlobalState;

import java.sql.SQLException;

public class PrestoGlobalState extends SQLGlobalState<PrestoOptions, PrestoSchema> {

    @Override
    protected PrestoSchema readSchema() throws SQLException {
        return PrestoSchema.fromConnection(getConnection(), getDatabaseName());
    }

    // BANE: check

    //    public boolean usesPQS() {
    //        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == MySQLOptions.MySQLOracleFactory.PQS);
    //    }

}
