package sqlancer.presto.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.gen.PrestoTypedExpressionGenerator;

public interface PrestoFunction {

    String getFunctionName();

    boolean isCompatibleWithReturnType(PrestoSchema.PrestoCompositeDataType returnType);

    PrestoSchema.PrestoDataType[] getArgumentTypes(PrestoSchema.PrestoCompositeDataType returnType);

    default List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                                                                   PrestoSchema.PrestoDataType[] argumentTypes, PrestoSchema.PrestoCompositeDataType returnType) {

        List<Node<PrestoExpression>> arguments = new ArrayList<>();

        // This is a workaround based on the assumption that array types should refer to
        // the same element type.
        PrestoSchema.PrestoCompositeDataType savedArrayType = null;
        if (returnType.getPrimitiveDataType() == PrestoSchema.PrestoDataType.ARRAY) {
            savedArrayType = returnType;
        }
        if (getNumberOfArguments() == -1) {
            PrestoSchema.PrestoDataType dataType = argumentTypes[0];
            // TODO: consider upper
            long no = Randomly.getNotCachedInteger(2, 10);
            for (int i = 0; i < no; i++) {
                PrestoSchema.PrestoCompositeDataType type;

                if (dataType == PrestoSchema.PrestoDataType.ARRAY) {
                    if (savedArrayType == null) {
                        savedArrayType = dataType.get();
                    }
                    type = savedArrayType;
                } else {
                    type = PrestoSchema.PrestoCompositeDataType.fromDataType(dataType);
                }
                arguments.add(gen.generateExpression(type, depth + 1));
            }
        } else {
            for (PrestoSchema.PrestoDataType arg : argumentTypes) {
                PrestoSchema.PrestoCompositeDataType dataType;
                if (arg == PrestoSchema.PrestoDataType.ARRAY) {
                    if (savedArrayType == null) {
                        savedArrayType = arg.get();
                    }
                    dataType = savedArrayType;
                } else {
                    dataType = PrestoSchema.PrestoCompositeDataType.fromDataType(arg);
                }
                Node<PrestoExpression> expression = gen.generateExpression(dataType, depth + 1);
                arguments.add(expression);
            }
        }
        return arguments;
    }

    default List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                                                                   PrestoSchema.PrestoCompositeDataType returnType) {

        List<Node<PrestoExpression>> arguments = new ArrayList<>();

        // This is a workaround based on the assumption that array types should refer to
        // the same element type.
        PrestoSchema.PrestoCompositeDataType savedArrayType = null;
        if (returnType.getPrimitiveDataType() == PrestoSchema.PrestoDataType.ARRAY) {
            savedArrayType = returnType;
        }
        if (getNumberOfArguments() == -1) {
            PrestoSchema.PrestoDataType dataType = getArgumentTypes(returnType)[0];
            // TODO: consider upper
            long no = Randomly.getNotCachedInteger(2, 10);
            for (int i = 0; i < no; i++) {
                PrestoSchema.PrestoCompositeDataType type;

                if (dataType == PrestoSchema.PrestoDataType.ARRAY) {
                    if (savedArrayType == null) {
                        savedArrayType = dataType.get();
                    }
                    type = savedArrayType;
                } else {
                    type = PrestoSchema.PrestoCompositeDataType.fromDataType(dataType);
                }
                arguments.add(gen.generateExpression(type, depth + 1));
            }
        } else {
            for (PrestoSchema.PrestoDataType arg : getArgumentTypes(returnType)) {
                PrestoSchema.PrestoCompositeDataType dataType;
                if (arg == PrestoSchema.PrestoDataType.ARRAY) {
                    if (savedArrayType == null) {
                        savedArrayType = arg.get();
                    }
                    dataType = savedArrayType;
                } else {
                    dataType = PrestoSchema.PrestoCompositeDataType.fromDataType(arg);
                }
                Node<PrestoExpression> expression = gen.generateExpression(dataType, depth + 1);
                arguments.add(expression);
            }
        }
        return arguments;
    }

    default int getNumberOfArguments() {
        return getArgumentTypes(null).length;
    }

    default boolean shouldPreserveOrderOfArguments() {
        return false;
    }

    default boolean isStandardFunction() {
        return true;
    }
}
