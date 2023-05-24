package sqlancer.presto;

import sqlancer.common.query.ExpectedErrors;

public final class PrestoErrors {

    private PrestoErrors() {
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        /*
        errors.add("with non-constant precision is not supported");
        errors.add("Like pattern must not end with escape character");
        errors.add("Could not convert string");
        errors.add("ORDER term out of range - should be between ");
        errors.add("You might need to add explicit type casts.");
        errors.add("can't be cast because the value is out of range for the destination type");
        errors.add("field value out of range");
        errors.add("Unimplemented type for cast");

        errors.add("Escape string must be empty or one character.");
        errors.add("Type mismatch when combining rows"); // BETWEEN

        errors.add("invalid UTF-8"); // TODO
        errors.add("String value is not valid UTF8");

        errors.add("Invalid TypeId "); // TODO

        errors.add("GROUP BY clause cannot contain aggregates!"); // investigate

        addRegexErrors(errors);

        addFunctionErrors(errors);

        errors.add("Overflow in multiplication");
        errors.add("Out of Range");
        errors.add("Date out of range");

        // collate
        errors.add("Cannot combine types with different collation!");
        errors.add("collations are only supported for type varchar");

        errors.add("Like pattern must not end with escape character!"); // LIKE

        errors.add("does not have a column named \"rowid\""); // TODO: this can be removed if we can query whether a
        // table supports rowids

        errors.add("does not have a column named"); // TODO: this only happens for views whose underlying table has a
        // removed column
        errors.add("Contents of view were altered: types don't match!");
        errors.add("Not implemented: ROUND(DECIMAL, INTEGER) with non-constant precision is not supported");

        // timestamp
        errors.add("Cannot subtract infinite timestamps");
        errors.add("Timestamp difference is out of bounds");
*/
        // Presto errors
        errors.add("cannot be applied to");
        errors.add("LIKE expression must evaluate to a varchar");
        errors.add("JOIN ON clause must evaluate to a boolean");
        errors.add("Unexpected parameters");

        //  SELECT SUM(count) FROM (SELECT CAST((-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.0000 IS NOT NULL AND -179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.0000) AS BIGINT)as count FROM t0) as res
        errors.add("Decimal overflow");
        errors.add("multiplication overflow");
        errors.add("addition overflow");
        errors.add("subtraction overflow");

        // cast
//        errors.add("Cannot cast");
        errors.add("Value cannot be cast to");
        errors.add("Cannot cast DECIMAL");
        errors.add("Cannot cast BIGINT");
        errors.add("Cannot cast INTEGER");

        //  9223372036854775808
        errors.add("Invalid numeric literal");

        errors.add("Division by zero");
        errors.add("/ by zero");

        errors.add("Cannot subtract hour, minutes or seconds from a date");
        errors.add("Cannot add hour, minutes or seconds to a date");

        errors.add("DECIMAL scale must be in range");
        errors.add("multiplication overflow");
        errors.add("addition overflow");
        errors.add("subtraction overflow");
        errors.add("Decimal overflow");
        errors.add("IN value and list items must be the same type");
    }

    private static void addRegexErrors(ExpectedErrors errors) {
        errors.add("missing ]");
        errors.add("missing )");
        errors.add("invalid escape sequence");
        errors.add("no argument for repetition operator: ");
        errors.add("bad repetition operator");
        errors.add("trailing \\");
        errors.add("invalid perl operator");
        errors.add("invalid character class range");
        errors.add("width is not integer");
    }

    private static void addFunctionErrors(ExpectedErrors errors) {
        errors.add("SUBSTRING cannot handle negative lengths");
        errors.add("is undefined outside [-1,1]"); // ACOS etc
        errors.add("invalid type specifier"); // PRINTF
        errors.add("argument index out of range"); // PRINTF
        errors.add("invalid format string"); // PRINTF
        errors.add("number is too big"); // PRINTF
        errors.add("Like pattern must not end with escape character!"); // LIKE
        errors.add("Could not choose a best candidate function for the function call \"date_part"); // date_part
        errors.add("extract specifier"); // date_part
        errors.add("not recognized"); // date_part
        errors.add("not supported"); // date_part
        errors.add("Failed to cast");
        errors.add("Conversion Error");
        errors.add("Could not cast value");
        errors.add("Insufficient padding in RPAD"); // RPAD
        errors.add("Could not choose a best candidate function for the function call"); // monthname
        errors.add("expected a numeric precision field"); // ROUND
        errors.add("with non-constant precision is not supported"); // ROUND
    }

    // TODO: cover presto error
    public static void addInsertErrors(ExpectedErrors errors) {
        addRegexErrors(errors);
        addFunctionErrors(errors);

        errors.add("NOT NULL constraint failed");
        errors.add("PRIMARY KEY or UNIQUE constraint violated");
        errors.add("duplicate key");
        errors.add("can't be cast because the value is out of range for the destination type");
        errors.add("Could not convert string");
        errors.add("Unimplemented type for cast");
        errors.add("field value out of range");
        errors.add("CHECK constraint failed");
        errors.add("Cannot explicitly insert values into rowid column"); // TODO: don't insert into rowid
        errors.add(" Column with name rowid does not exist!"); // currently, there doesn't seem to way to determine if
        // the table has a primary key
        errors.add("Could not cast value");
        errors.add("create unique index, table contains duplicate data");
        errors.add("Failed to cast");

        errors.add("Values rows have mismatched types");
        errors.add("Mismatch at column");
        errors.add("This connector does not support updates or deletes");
        errors.add("Values rows have mismatched types");
        errors.add("Invalid numeric literal");

    }

    public static void addGroupByErrors(ExpectedErrors errors) {
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("GROUP BY term out of range");
    }

}
