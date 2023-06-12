package sqlancer.presto.ast;

import sqlancer.Randomly;
import sqlancer.presto.PrestoSchema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static sqlancer.presto.PrestoSchema.PrestoDataType.*;

public enum PrestoAggregateFunctionOld {
    MAX(0, 3, List.of(INT, FLOAT, DECIMAL, DATE, TIME, TIMESTAMP, TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE), List.of(INT, FLOAT, DECIMAL, DATE, TIME, TIMESTAMP, TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE)),
    MIN(0, 3, List.of(INT, FLOAT, DECIMAL, DATE, TIME, TIMESTAMP, TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE), List.of(INT, FLOAT, DECIMAL, DATE, TIME, TIMESTAMP, TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE)),
    AVG(0, 3, List.of(INT, FLOAT, DECIMAL, DATE, TIME, TIMESTAMP, TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE), List.of(INT, FLOAT, DECIMAL, DATE, TIME, TIMESTAMP, TIME_WITH_TIME_ZONE, TIMESTAMP_WITH_TIME_ZONE)),
    COUNT(0, 3, List.of(INT), List.of()),
    STRING_AGG(0, 3, List.of(VARCHAR), List.of(VARCHAR, CHAR)),
    FIRST(0, 3, List.of(), List.of()),
    SUM(0, 3, List.of(INT, FLOAT, DECIMAL), List.of(INT, FLOAT, DECIMAL)),
    STDDEV_SAMP(1, 3, List.of(INT, FLOAT, DECIMAL), List.of(INT, FLOAT, DECIMAL)),
    STDDEV_POP(1, 3, List.of(INT, FLOAT, DECIMAL), List.of(INT, FLOAT, DECIMAL)),
    VAR_POP(1, 3, List.of(INT, FLOAT, DECIMAL), List.of(INT, FLOAT, DECIMAL)),
    VAR_SAMP(1, 3, List.of(INT, FLOAT, DECIMAL), List.of(INT, FLOAT, DECIMAL)),
    COVAR_POP(1, 3, List.of(INT, FLOAT, DECIMAL), List.of(INT, FLOAT, DECIMAL)),
    COVAR_SAMP(1, 3, List.of(INT, FLOAT, DECIMAL), List.of(INT, FLOAT, DECIMAL));

    private final int nrArgs;
    private final List<PrestoSchema.PrestoDataType> supportedReturnTypes;
    private final List<PrestoSchema.PrestoDataType> supportedParamTypes;
    private final int maxNrArgs;

    PrestoAggregateFunctionOld(int nrArgs, int maxNrArgs, List<PrestoSchema.PrestoDataType> supportedReturnTypes, List<PrestoSchema.PrestoDataType> supportedParamTypes) {
        this.nrArgs = nrArgs;
        this.maxNrArgs = maxNrArgs;
        this.supportedReturnTypes = supportedReturnTypes;
        this.supportedParamTypes = supportedParamTypes;
    }

    public static PrestoAggregateFunctionOld getRandom() {
        return Randomly.fromOptions(values());
    }

    public int getNrArgs() {
        return nrArgs;
    }

    public boolean supportsReturnType(PrestoSchema.PrestoDataType returnType) {
        return supportedReturnTypes.stream().anyMatch(t -> t == returnType)
                || supportedReturnTypes.size() == 0;
    }

    public static List<PrestoAggregateFunctionOld> getAggregates(PrestoSchema.PrestoDataType type) {
        return Arrays.stream(values()).filter(p -> p.supportsReturnType(type))
                .collect(Collectors.toList());
    }

    public PrestoSchema.PrestoDataType getRandomReturnType() {
        if (supportedReturnTypes.size() == 0) {
            return Randomly.fromOptions(PrestoSchema.PrestoDataType.getRandomWithoutNull());
        } else {
            return Randomly.fromOptions(supportedReturnTypes.toArray(new PrestoSchema.PrestoDataType[0]));
        }
    }

    public List<PrestoSchema.PrestoDataType> getSupportedParamTypes() {
        return supportedParamTypes;
    }

    public List<PrestoSchema.PrestoDataType> getSupportedReturnTypes() {
        return supportedReturnTypes;
    }

    public List<PrestoSchema.PrestoDataType> getReturnTypes(PrestoSchema.PrestoDataType dataType) {
        return Collections.singletonList(dataType);
    }

    public int getMaxNrArgs() {
        return maxNrArgs;
    }
}
