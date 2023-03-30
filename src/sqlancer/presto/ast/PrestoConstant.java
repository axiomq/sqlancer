package sqlancer.presto.ast;

import sqlancer.common.ast.newast.Node;

import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public class PrestoConstant implements Node<PrestoExpression> {

    private PrestoConstant() {
    }

    public static class PrestoNullConstant extends PrestoConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class PrestoIntConstant extends PrestoConstant {

        private final long value;

        public PrestoIntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public long getValue() {
            return value;
        }

    }

    public static class PrestoFloatConstant extends PrestoConstant {

        private final double value;

        public PrestoFloatConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return String.valueOf(value);
        }

    }

    public static class PrestoDecimalConstant extends PrestoConstant {

        private static final DecimalFormat df = new DecimalFormat("###0.0000");

        private final double value;

        public PrestoDecimalConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return df.format(value);
        }

    }

    public static class PrestoTextConstant extends PrestoConstant {

        private final String value;

        public PrestoTextConstant(String value) {
            this.value = value;
        }

        public PrestoTextConstant(String value, int size) {
            this.value = value.substring(0, Math.min(value.length(), size));
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value.replace("'", "''") + "'";
        }

    }

    public static class PrestoBitConstant extends PrestoConstant {

        private final String value;

        public PrestoBitConstant(long value) {
            this.value = Long.toBinaryString(value);
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "B'" + value + "'";
        }

    }

    public static class PrestoDateConstant extends PrestoConstant {

        public String textRepr;

        public PrestoDateConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("DATE '%s'", textRepr);
        }

    }

    public static class PrestoTimeConstant extends PrestoConstant {

        public String textRepr;

        public PrestoTimeConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("TIME '%s'", textRepr);
        }

    }

    public static class PrestoTimestampConstant extends PrestoConstant {

        public String textRepr;

        public PrestoTimestampConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP '%s'", textRepr);
        }

    }

    public static class PrestoBooleanConstant extends PrestoConstant {

        private final boolean value;

        public PrestoBooleanConstant(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

    }

    public static Node<PrestoExpression> createStringConstant(String text) {
        return new PrestoTextConstant(text);
    }

    public static Node<PrestoExpression> createStringConstant(String text, int size) {
        return new PrestoTextConstant(text, size);
    }

    public static Node<PrestoExpression> createFloatConstant(double val) {
        return new PrestoFloatConstant(val);
    }

    public static Node<PrestoExpression> createDecimalConstant(double val) {
        return new PrestoDecimalConstant(val);
    }

    public static Node<PrestoExpression> createIntConstant(long val) {
        return new PrestoIntConstant(val);
    }

    public static Node<PrestoExpression> createNullConstant() {
        return new PrestoNullConstant();
    }

    public static Node<PrestoExpression> createBooleanConstant(boolean val) {
        return new PrestoBooleanConstant(val);
    }

    public static Node<PrestoExpression> createDateConstant(long integer) {
        return new PrestoDateConstant(integer);
    }

    public static Node<PrestoExpression> createTimeConstant(long integer) {
        return new PrestoTimeConstant(integer);
    }

    public static Node<PrestoExpression> createTimestampConstant(long integer) {
        return new PrestoTimestampConstant(integer);
    }

}
