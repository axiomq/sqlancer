package sqlancer.presto.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.PrestoSchema.PrestoCompositeDataType;
import sqlancer.presto.PrestoSchema.PrestoDataType;
import sqlancer.presto.gen.PrestoTypedExpressionGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum PrestoFunction {

    IF(null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoSchema.PrestoCompositeDataType returnType) {
            return true;
        }

        @Override
        public PrestoSchema.PrestoDataType[] getArgumentTypes(PrestoSchema.PrestoCompositeDataType returnType) {
            return new PrestoSchema.PrestoDataType[]{PrestoSchema.PrestoDataType.BOOLEAN, returnType.getPrimitiveDataType(),
                    returnType.getPrimitiveDataType()};
        }
    },
    NULLIF(null) {
        @Override
        public boolean isCompatibleWithReturnType(PrestoSchema.PrestoCompositeDataType returnType) {
            return true;
        }

        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            return new PrestoDataType[]{returnType.getPrimitiveDataType(), returnType.getPrimitiveDataType()};
        }
    },

    // MATH and numeric functions
    ABS_INT("ABS", PrestoDataType.INT, PrestoDataType.INT),
    // ABS_FLOAT("ABS", PrestoDataType.INT, PrestoDataType.FLOAT),
    ACOS(PrestoDataType.FLOAT, PrestoDataType.FLOAT),

    // string and byte functions
    ASCII(PrestoDataType.INT, PrestoDataType.VARCHAR), // ascii(val: string) → int
//    BIT_LENGTH1("BIT_LENGTH", PrestoDataType.INT, PrestoDataType.BYTES), // bit_length(val: bytes) → int
    BIT_LENGTH2("BIT_LENGTH", PrestoDataType.INT, PrestoDataType.VARCHAR), // bit_length(val: string) → int
    BTRIM1("BTRIM", PrestoDataType.VARCHAR, PrestoDataType.VARCHAR, PrestoDataType.VARCHAR), // btrim(input:
    // string,
    // trim_chars:
    // string) →
    // string
    BTRIM2("BTRIM", PrestoDataType.VARCHAR, PrestoDataType.VARCHAR), // btrim(val: string) → string
//    CHAR_LENGTH1("CHAR_LENGTH", PrestoDataType.INT, PrestoDataType.BYTES), // char_length(val: bytes) → int
    CHAR_LENGTH2("CHAR_LENGTH", PrestoDataType.INT, PrestoDataType.VARCHAR),
    CHARACTER_LENGTH1("CHARACTER_LENGTH", PrestoDataType.INT, PrestoDataType.VARCHAR),
//    CHARACTER_LENGTH2("CHARACTER_LENGTH", PrestoDataType.INT, PrestoDataType.BYTES),
    CHR(PrestoDataType.VARCHAR, PrestoDataType.INT),
    INITCAP(PrestoDataType.VARCHAR, PrestoDataType.VARCHAR),
//    LEFT1("LEFT", PrestoDataType.BYTES, PrestoDataType.BYTES, PrestoDataType.INT),
    LEFT2("LEFT", PrestoDataType.VARCHAR, PrestoDataType.VARCHAR, PrestoDataType.INT),
//    LENGTH1("LENGTH", PrestoDataType.INT, PrestoDataType.BYTES),
    LENGTH2("LENGTH", PrestoDataType.INT, PrestoDataType.VARCHAR),
    LOWER(PrestoDataType.VARCHAR, PrestoDataType.VARCHAR),
    // LPAD(PrestoDataType.VARCHAR, PrestoDataType.VARCHAR, PrestoDataType.INT), // TODO: can cause out of
    // memory errors
    LTRIM(PrestoDataType.VARCHAR, PrestoDataType.VARCHAR, PrestoDataType.VARCHAR),
    OVERLAY(PrestoDataType.VARCHAR, PrestoDataType.VARCHAR, PrestoDataType.VARCHAR,
            PrestoDataType.INT),
    QUOTE_IDENT(PrestoDataType.VARCHAR, PrestoDataType.VARCHAR),
    QUOTE_LITERAL(PrestoDataType.VARCHAR, PrestoDataType.VARCHAR),
    QUOTE_NULLABLE(PrestoDataType.VARCHAR, PrestoDataType.VARCHAR),
    REVERSE(PrestoDataType.VARCHAR, PrestoDataType.VARCHAR),
    STRPOS(PrestoDataType.INT, PrestoDataType.VARCHAR, PrestoDataType.VARCHAR),
    SPLIT_PART(PrestoDataType.VARCHAR, PrestoDataType.VARCHAR, PrestoDataType.VARCHAR,
            PrestoDataType.INT),
    SUBSTRING1("SUBSTRING", PrestoDataType.VARCHAR, PrestoDataType.VARCHAR, PrestoDataType.VARCHAR),
    SUBSTRING2("SUBSTRING", PrestoDataType.VARCHAR, PrestoDataType.VARCHAR, PrestoDataType.VARCHAR,
            PrestoDataType.VARCHAR),
    SUBSTRING3("SUBSTRING", PrestoDataType.VARCHAR, PrestoDataType.VARCHAR, PrestoDataType.INT),
    SUBSTRING4("SUBSTRING", PrestoDataType.VARCHAR, PrestoDataType.VARCHAR, PrestoDataType.INT,
            PrestoDataType.INT),
    /* https://github.com/cockroachdb/cockroach/issues/44152 */
    TO_ENGLISH(PrestoDataType.VARCHAR, PrestoDataType.INT),
    TO_HEX1("TO_HEX", PrestoDataType.VARCHAR, PrestoDataType.INT),
//    TO_HEX("TO_HEX", PrestoDataType.VARCHAR, PrestoDataType.BYTES),
//    TO_IP(PrestoDataType.BYTES, PrestoDataType.VARCHAR),
//    TO_UUID(PrestoDataType.BYTES, PrestoDataType.VARCHAR),
    TRANSLATE(PrestoDataType.VARCHAR, PrestoDataType.VARCHAR, PrestoDataType.VARCHAR,
            PrestoDataType.VARCHAR),
    UPPER(PrestoDataType.VARCHAR, PrestoDataType.VARCHAR),
    REGEXP_REPLACE(PrestoDataType.VARCHAR, PrestoDataType.VARCHAR, PrestoDataType.VARCHAR,
            PrestoDataType.VARCHAR),

    MD5(PrestoDataType.VARCHAR) {
        @Override
        public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
            int nrArgs = Randomly.smallNumber() + 1;
            PrestoDataType[] argTypes = new PrestoDataType[nrArgs];
            for (int i = 0; i < nrArgs; i++) {
                argTypes[i] = PrestoDataType.VARCHAR;
            }
            return argTypes;
        }
    }

    ;
//    ,
    // System info function
    /* see https://github.com/cockroachdb/cockroach/issues/44203 */
//    CURRENT_DATABASE(PrestoDataType.VARCHAR), CURRENT_SCHEMA(PrestoDataType.VARCHAR),
//    CURRENT_USER(PrestoDataType.VARCHAR), VERSION(PrestoDataType.VARCHAR);


    private PrestoDataType returnType;
    private PrestoDataType[] argumentTypes;
    private String functionName;

    PrestoFunction(PrestoDataType returnType, PrestoDataType... argumentTypes) {
        this.returnType = returnType;
        this.argumentTypes = argumentTypes.clone();
        this.functionName = toString();
    }

    PrestoFunction(PrestoDataType returnType) {
        this.returnType = returnType;
        this.argumentTypes = new PrestoDataType[0];
        this.functionName = toString();
    }

    public String getFunctionName() {
        return functionName;
    }

    PrestoFunction(String functionName, PrestoDataType returnType, PrestoDataType... argumentTypes) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes.clone();
    }

    public boolean isCompatibleWithReturnType(PrestoCompositeDataType returnType) {
        return this.returnType == returnType.getPrimitiveDataType();
    }

    public PrestoDataType[] getArgumentTypes(PrestoCompositeDataType returnType) {
        return argumentTypes.clone();
    }

    public PrestoFunctionCall getCall(PrestoCompositeDataType returnType, PrestoTypedExpressionGenerator gen,
                                      int depth) {
        PrestoDataType[] argumentTypes2 = getArgumentTypes(returnType);
        List<Node<PrestoExpression>> arguments = getArgumentsForReturnType(gen, depth, argumentTypes2, returnType);
        return new PrestoFunctionCall(this, arguments);
    }

    List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                                                           PrestoDataType[] argumentTypes2, PrestoCompositeDataType returnType2) {
        List<Node<PrestoExpression>> arguments = new ArrayList<>();

        // This is a workaround based on the assumption that array types should refer to the same element type.

        PrestoCompositeDataType savedArrayType = null;
//        if (returnType2.getPrimitiveDataType() == PrestoDataType.ARRAY) {
//            savedArrayType = returnType2;
//        }
        for (PrestoDataType arg : argumentTypes2) {
            PrestoCompositeDataType type;
//            if (arg == PrestoDataType.ARRAY) {
//                if (savedArrayType == null) {
//                    savedArrayType = arg.get();
//                }
//                type = savedArrayType;
//            } else

            type = PrestoCompositeDataType.fromDataType(arg);
            arguments.add(gen.generateExpression(type, depth + 1));
        }

        return arguments;
    }

    public static List<PrestoFunction> getFunctionsCompatibleWith(PrestoCompositeDataType returnType) {
        return Stream.of(values()).filter(f -> f.isCompatibleWithReturnType(returnType)).collect(Collectors.toList());
    }

}
