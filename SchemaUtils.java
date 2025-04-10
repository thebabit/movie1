// SchemaUtils.java
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;

import java.util.*;

public class SchemaUtils {

    // Recursively normalize a StructType to match the target schema
    private static String buildStructExpr(String structAlias, StructType currentSchema, StructType targetSchema) {
        StringBuilder exprBuilder = new StringBuilder("named_struct(");
        StructField[] targetFields = targetSchema.fields();

        for (int i = 0; i < targetFields.length; i++) {
            String fieldName = targetFields[i].name();
            DataType targetType = targetFields[i].dataType();
            StructField currentField = currentSchema.getFieldIndex(fieldName).isDefined()
                    ? currentSchema.apply(fieldName)
                    : null;

            exprBuilder.append("'").append(fieldName).append("', ");

            if (currentField != null) {
                DataType currentType = currentField.dataType();
                if (targetType instanceof StructType && currentType instanceof StructType) {
                    exprBuilder.append(buildStructExpr(structAlias + "." + fieldName,
                            (StructType) currentType,
                            (StructType) targetType));
                } else if (!currentType.sameType(targetType)) {
                    exprBuilder.append("cast(").append(structAlias).append(".").append(fieldName)
                            .append(" as ").append(targetType.simpleString()).append(")");
                } else {
                    exprBuilder.append(structAlias).append(".").append(fieldName);
                }
            } else {
                exprBuilder.append("null");
            }

            if (i < targetFields.length - 1) {
                exprBuilder.append(", ");
            }
        }

        exprBuilder.append(")");
        return exprBuilder.toString();
    }

    // Automatically normalize an array<struct> column to match the target struct schema
    public static Dataset<Row> autoNormalizeArraySchema(
            Dataset<Row> df,
            String arrayFieldName,
            StructType targetElementSchema
    ) {
        DataType arrayType = df.schema().apply(arrayFieldName).dataType();
        if (!(arrayType instanceof ArrayType)) {
            throw new IllegalArgumentException("Field " + arrayFieldName + " is not an ArrayType");
        }

        StructType currentStruct = (StructType) ((ArrayType) arrayType).elementType();
        String structExpr = buildStructExpr("x", currentStruct, targetElementSchema);

        String finalExpr = "transform(" + arrayFieldName + ", x -> " + structExpr + ")";
        return df.withColumn(arrayFieldName, functions.expr(finalExpr));
    }

    // Automatically normalize, join and merge struct into array<struct>
    public static Dataset<Row> autoJoinWithArrayStructMerge(
            Dataset<Row> df1,
            Dataset<Row> df2,
            String joinKey1,
            String joinKey2,
            String arrayFieldName,
            String structFieldInDf2
    ) {
        StructType targetElementSchema = (StructType) df2.schema().apply(structFieldInDf2).dataType();
        Dataset<Row> normalizedDf1 = autoNormalizeArraySchema(df1, arrayFieldName, targetElementSchema);

        Dataset<Row> joined = normalizedDf1.join(df2, normalizedDf1.col(joinKey1).equalTo(df2.col(joinKey2)));

        Dataset<Row> result = joined.withColumn(
                arrayFieldName,
                functions.expr("array_union(" + arrayFieldName + ", array(" + structFieldInDf2 + "))")
        );

        return result.drop(joinKey2).drop(structFieldInDf2);
    }
}