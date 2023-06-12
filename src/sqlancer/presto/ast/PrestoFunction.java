package sqlancer.presto.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.PrestoSchema;
import sqlancer.presto.gen.PrestoTypedExpressionGenerator;

import java.util.ArrayList;
import java.util.List;

public interface PrestoFunction {

    String getFunctionName();

    boolean isCompatibleWithReturnType(PrestoSchema.PrestoCompositeDataType returnType);

    PrestoSchema.PrestoDataType[] getArgumentTypes(PrestoSchema.PrestoCompositeDataType returnType);

    default List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                                                                   PrestoSchema.PrestoDataType[] argumentTypes2, PrestoSchema.PrestoCompositeDataType returnType2) {
        List<Node<PrestoExpression>> arguments = new ArrayList<>();

        // This is a workaround based on the assumption that array types should refer to the same element type.

        PrestoSchema.PrestoCompositeDataType savedArrayType = null;
        //        if (returnType2.getPrimitiveDataType() == PrestoDataType.ARRAY) {
        //            savedArrayType = returnType2;
        //        }
        for (PrestoSchema.PrestoDataType arg : argumentTypes2) {
            PrestoSchema.PrestoCompositeDataType type;
            //            if (arg == PrestoDataType.ARRAY) {
            //                if (savedArrayType == null) {
            //                    savedArrayType = arg.get();
            //                }
            //                type = savedArrayType;
            //            } else
            type = PrestoSchema.PrestoCompositeDataType.fromDataType(arg);
            Node<PrestoExpression> expression = gen.generateExpression(type, depth + 1);
            arguments.add(expression);
        }
        return arguments;
    }

    default List<Node<PrestoExpression>> getArgumentsForReturnType(PrestoTypedExpressionGenerator gen, int depth,
                                                                   PrestoSchema.PrestoCompositeDataType returnType) {
        List<Node<PrestoExpression>> arguments = new ArrayList<>();

        // This is a workaround based on the assumption that array types should refer to the same element type.

        PrestoSchema.PrestoCompositeDataType savedArrayType = null;
        //        if (returnType.getPrimitiveDataType() == PrestoDataType.ARRAY) {
        //            savedArrayType = returnType;
        //        }


        if (getNumberOfArguments() == -1) {
            PrestoSchema.PrestoDataType dataType = getArgumentTypes(returnType)[0];
            long no = Randomly.getNotCachedInteger(2, 10);
            for (int i = 0; i < no; i++) {
                PrestoSchema.PrestoCompositeDataType type = PrestoSchema.PrestoCompositeDataType.fromDataType(dataType);
                arguments.add(gen.generateExpression(type, depth + 1));
            }
        } else {
            for (PrestoSchema.PrestoDataType arg : getArgumentTypes(returnType)) {
                PrestoSchema.PrestoCompositeDataType type;
                //            if (arg == PrestoDataType.ARRAY) {
                //                if (savedArrayType == null) {
                //                    savedArrayType = arg.get();
                //                }
                //                type = savedArrayType;
                //            } else
                type = PrestoSchema.PrestoCompositeDataType.fromDataType(arg);
                Node<PrestoExpression> expression = gen.generateExpression(type, depth + 1);
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
