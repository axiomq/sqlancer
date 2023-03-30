package sqlancer.presto;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PrestoSchema extends AbstractSchema<PrestoGlobalState, PrestoSchema.PrestoTable> {

    public enum PrestoDataType {
        BOOLEAN,
        INT,
        FLOAT,
        DECIMAL,
        VARCHAR,
        CHAR,
        //  VARBINARY,
        //  JSON,
        DATE,
        TIME,
        TIMESTAMP,
        //  TIME_WITH_TIME_ZONE,
        //  TIMESTAMP_WITH_TIME_ZONE,
        //  INTERVAL_YEAR_TO_MONTH,
        //  INTERVAL_DAY_TO_SECOND,
        //  ARRAY,
        //  MAP,
        //  ROW,
        //  IPADDRESS,
        //  UID,
        //  IPPREFIX,
        //  HyperLogLog,
        //  P4HyperLogLog,
        //  KHyperLogLog,
        //  QDigest,
        //  TDigest,
        NULL;

        public static PrestoDataType getRandomWithoutNull() {
            PrestoDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == PrestoDataType.NULL);
            return dt;
        }

        public boolean isNumeric() {
            switch (this) {
                case INT:
                case FLOAT:
                case DECIMAL:
                    return true;
                case VARCHAR:
                case CHAR:
                    return false;
                default:
                    throw new AssertionError(this);
            }
        }

        public static List<PrestoDataType> getNumericTypes() {
            return Arrays.asList(INT, FLOAT, DECIMAL, VARCHAR, CHAR, DATE, TIME, TIMESTAMP);
        }

        public static List<PrestoDataType> getTextTypes() {
            return Arrays.asList(VARCHAR, CHAR);
        }

    }

    public static class PrestoCompositeDataType {

        private final PrestoDataType dataType;

        private final int size;

        private final int scale;

        public PrestoCompositeDataType(PrestoDataType dataType, int size, int scale) {
            this.dataType = dataType;
            this.size = size;
            this.scale = scale;
        }

        public PrestoDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public int getScale() {
            if (scale == -1) {
                throw new AssertionError(this);
            }
            return scale;
        }

        public static PrestoCompositeDataType getRandomWithoutNull() {
            PrestoDataType type = PrestoDataType.getRandomWithoutNull();
            int size = -1;
            int scale = -1;
            switch (type) {
                case BOOLEAN:
                    size = 0;
                    break;
                case INT:
                    size = Randomly.fromOptions(1, 2, 4, 8);
                    break;
                case FLOAT:
                    size = Randomly.fromOptions(4, 8);
                    break;
                case DECIMAL:
                    size = Math.toIntExact(8);
                    scale = Math.toIntExact(4);
                    break;
                case VARCHAR:
                    size = Math.toIntExact(Randomly.getNotCachedInteger(10, 250));
                    break;
                case CHAR:
                    size = Math.toIntExact(Randomly.getNotCachedInteger(10, 250));
                    break;
                case DATE:
                case TIME:
                case TIMESTAMP:
                    size = 0;
                    break;
                default:
                    throw new AssertionError(type);
            }

            return new PrestoCompositeDataType(type, size, scale);
        }

        @Override
        public String toString() {
            switch (getPrimitiveDataType()) {
                case INT:
                    switch (size) {
                        case 8:
                            return Randomly.fromOptions("BIGINT");
                        case 4:
                            return Randomly.fromOptions("INTEGER");
                        case 2:
                            return Randomly.fromOptions("SMALLINT");
                        case 1:
                            return Randomly.fromOptions("TINYINT");
                        default:
                            throw new AssertionError(size);
                    }
                case VARCHAR:
                    return "VARCHAR" + "(" + size + ")";
                case CHAR:
                    return "CHAR" + "(" + size + ")";
                case FLOAT:
                    switch (size) {
                        case 4:
                            return Randomly.fromOptions("REAL");
                        case 8:
                            return Randomly.fromOptions("DOUBLE");
                        default:
                            throw new AssertionError(size);
                    }
                case DECIMAL:
                    return "DECIMAL" + "(" + size + ", " + scale + ")";
                case BOOLEAN:
                    return Randomly.fromOptions("BOOLEAN");
                case TIMESTAMP:
                    return Randomly.fromOptions("TIMESTAMP");
                case DATE:
                    return Randomly.fromOptions("DATE");
                case TIME:
                    return Randomly.fromOptions("TIME");
                case NULL:
                    return Randomly.fromOptions("NULL");
                default:
                    throw new AssertionError(getPrimitiveDataType());
            }
        }

    }

    public static class PrestoColumn extends AbstractTableColumn<PrestoTable, PrestoCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;

        public PrestoColumn(String name, PrestoCompositeDataType columnType, boolean isPrimaryKey, boolean isNullable) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

    }

    public static class PrestoTables extends AbstractTables<PrestoTable, PrestoColumn> {

        public PrestoTables(List<PrestoTable> tables) {
            super(tables);
        }

    }

    public PrestoSchema(List<PrestoTable> databaseTables) {
        super(databaseTables);
    }

    public PrestoTables getRandomTableNonEmptyTables() {
        return new PrestoTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public static class PrestoTable extends AbstractRelationalTable<PrestoColumn, TableIndex, PrestoGlobalState> {

        public PrestoTable(String tableName, List<PrestoColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

    }

    public static PrestoSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<PrestoTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            if (DBMSCommon.matchesIndexName(tableName)) {
                continue; // TODO: unexpected?
            }
            List<PrestoColumn> databaseColumns = getTableColumns(con, databaseName, tableName);
            boolean isView = tableName.startsWith("v");
            PrestoTable t = new PrestoTable(tableName, databaseColumns, isView);
            for (PrestoColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new PrestoSchema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {

            // TODO: UPDATE
            // SHOW TABLES [ FROM schema ] [ LIKE pattern [ ESCAPE 'escape_character' ] ]
            try (ResultSet rs = s.executeQuery("SHOW TABLES")) {
                while (rs.next()) {
                    tableNames.add(rs.getString("Table"));
                }
            }
        }
        return tableNames;
    }

    private static List<PrestoColumn> getTableColumns(SQLConnection con, String databaseName, String tableName) throws SQLException {
        List<PrestoColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            // SHOW COLUMNS FROM memory.test.t0;
            // Column,Type,Extra,Comment
            // c0,tinyint,"",""
            try (ResultSet rs = s.executeQuery(String.format("select * from information_schema.columns where table_schema = '%s' and table_name = '%s'", databaseName, tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    boolean isNullable = rs.getString("is_nullable").contentEquals("YES");
                    PrestoColumn c = new PrestoColumn(columnName, getColumnType(dataType), false, isNullable);
                    columns.add(c);
                }
            }
//            columns.add(new PrestoColumn("rowid", new PrestoCompositeDataType(PrestoDataType.INT, 4, 0), false, false));
        }

        return columns;
    }

    public static void main(String[] args) {

        String typeString;
        int bracesStart;
        String type;
        String sizeString;
        int size;
        int precision;

        typeString = "char(67)";
        bracesStart = typeString.indexOf('(');
        if (bracesStart != -1) {
            type = typeString.substring(0, bracesStart);
            sizeString = typeString.substring(bracesStart + 1, typeString.indexOf(")"));

            String[] sizes = sizeString.split(",");
            if (sizes.length == 2) {
                size = Integer.parseInt(sizes[0].strip());
                precision = Integer.parseInt(sizes[1].strip());
            } else if (sizes.length == 1) {
                size = Integer.parseInt(sizes[0].strip());
                precision = 0;
            } else {
                size = 0;
                precision = 0;
            }
        } else {
            type = typeString;
            sizeString = "";
            size = 0;
            precision = 0;
        }
        type = type.toUpperCase();

        System.out.println(type);
        System.out.println(sizeString);
        System.out.println(size);
        System.out.println(precision);

        typeString = "tinyint";
        bracesStart = typeString.indexOf('(');
        if (bracesStart != -1) {
            type = typeString.substring(0, bracesStart);
            sizeString = typeString.substring(bracesStart + 1, typeString.indexOf(")"));

            String[] sizes = sizeString.split(",");
            if (sizes.length == 2) {
                size = Integer.parseInt(sizes[0].strip());
                precision = Integer.parseInt(sizes[1].strip());
            } else if (sizes.length == 1) {
                size = Integer.parseInt(sizes[0].strip());
                precision = 0;
            } else {
                size = 0;
                precision = 0;
            }

        } else {
            type = typeString;
            sizeString = "";
            size = 0;
            precision = 0;
        }
        type = type.toUpperCase();

        System.out.println(type);
        System.out.println(sizeString);
        System.out.println(size);
        System.out.println(precision);

        typeString = "decimal(6, 4)";
        bracesStart = typeString.indexOf('(');
        if (bracesStart != -1) {
            type = typeString.substring(0, bracesStart);
            sizeString = typeString.substring(bracesStart + 1, typeString.indexOf(")"));

            String[] sizes = sizeString.split(",");
            if (sizes.length == 2) {
                size = Integer.parseInt(sizes[0].strip());
                precision = Integer.parseInt(sizes[1].strip());
            } else if (sizes.length == 1) {
                size = Integer.parseInt(sizes[0].strip());
                precision = 0;
            } else {
                size = 0;
                precision = 0;
            }

        } else {
            type = typeString;
            sizeString = "";
            size = 0;
            precision = 0;
        }
        type = type.toUpperCase();

        System.out.println(type);
        System.out.println(sizeString);
        System.out.println(size);
        System.out.println(precision);
    }

    private static PrestoCompositeDataType getColumnType(String typeString) {
        int bracesStart = typeString.indexOf('(');
        String type;
        int size = 0;
        int precision = 0;
        if (bracesStart != -1) {
            type = typeString.substring(0, bracesStart);
            String sizeString = typeString.substring(bracesStart + 1, typeString.indexOf(")"));
            String[] sizes = sizeString.split(",");
            if (sizes.length == 2) {
                size = Integer.parseInt(sizes[0].strip());
                precision = Integer.parseInt(sizes[1].strip());
            } else if (sizes.length == 1) {
                size = Integer.parseInt(sizes[0].strip());
            }

        } else {
            type = typeString;
        }
        type = type.toUpperCase();

        PrestoDataType primitiveType;
        switch (type) {
            case "INTEGER":
                primitiveType = PrestoDataType.INT;
                size = 4;
                break;
            case "SMALLINT":
                primitiveType = PrestoDataType.INT;
                size = 2;
                break;
            case "BIGINT":
                primitiveType = PrestoDataType.INT;
                size = 8;
                break;
            case "TINYINT":
                primitiveType = PrestoDataType.INT;
                size = 1;
                break;
            case "VARCHAR":
                primitiveType = PrestoDataType.VARCHAR;
                break;
            case "CHAR":
                primitiveType = PrestoDataType.CHAR;
                break;
            case "FLOAT":
                primitiveType = PrestoDataType.FLOAT;
                size = 4;
                break;
            case "REAL":
                primitiveType = PrestoDataType.FLOAT;
                size = 4;
                break;
            case "DOUBLE":
                primitiveType = PrestoDataType.FLOAT;
                size = 8;
                break;
            case "DECIMAL":
                primitiveType = PrestoDataType.DECIMAL;
                break;
            case "BOOLEAN":
                primitiveType = PrestoDataType.BOOLEAN;
                break;
            case "DATE":
                primitiveType = PrestoDataType.DATE;
                break;
            case "TIME":
                primitiveType = PrestoDataType.TIME;
                break;
            case "TIMESTAMP":
                primitiveType = PrestoDataType.TIMESTAMP;
                break;
            case "NULL":
                primitiveType = PrestoDataType.NULL;
                break;
            case "INTERVAL":
                throw new IgnoreMeException();
                // TODO: caused when a view contains a computation like ((TIMESTAMP '1970-01-05 11:26:57')-(TIMESTAMP
                // '1969-12-29 06:50:27'))
            default:
                throw new AssertionError(typeString);
        }
        return new PrestoCompositeDataType(primitiveType, size, precision);
    }

}
