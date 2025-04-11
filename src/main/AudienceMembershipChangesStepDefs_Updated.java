
package com.jpmc.pandi.cucumber.stepdefs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class AudienceMembershipChangesStepDefs {

    // Case 1: Add field to inclusionReasonValueText
    public static Dataset<Row> addFieldToInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "population.audienceDataFeatures",
            expr("transform(population.audienceDataFeatures, x -> struct(" +
                 "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                 "named_struct(" +
                 "  'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
                 "  'productCode', x.inclusionReasonValueText.productCode, " +
                 "  'newField', 'newValue'" +
                 ") as inclusionReasonValueText" +
                 "))")
        );
    }

    // Case 2: Remove field from inclusionReasonValueText
    public static Dataset<Row> removeFieldFromInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "population.audienceDataFeatures",
            expr("transform(population.audienceDataFeatures, x -> struct(" +
                 "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                 "named_struct(" +
                 "  'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
                 "  'productCode', x.inclusionReasonValueText.productCode" +
                 ") as inclusionReasonValueText" +
                 "))")
        );
    }

    // Case 3: Rename field inside inclusionReasonValueText
    public static Dataset<Row> renameFieldInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "population.audienceDataFeatures",
            expr("transform(population.audienceDataFeatures, x -> struct(" +
                 "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                 "named_struct(" +
                 "  'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
                 "  'productCodeRenamed', x.inclusionReasonValueText.productCode" +
                 ") as inclusionReasonValueText" +
                 "))")
        );
    }

    // Case 4: Change field type inside inclusionReasonValueText (e.g., productCode to int)
    public static Dataset<Row> changeFieldTypeInInclusionReasonValueText(Dataset<Row> input) {
        return input.withColumn(
            "population.audienceDataFeatures",
            expr("transform(population.audienceDataFeatures, x -> struct(" +
                 "x.inclusionReasonTypeCode as inclusionReasonTypeCode, " +
                 "named_struct(" +
                 "  'audienceDataFeatureTypeCode', x.inclusionReasonValueText.audienceDataFeatureTypeCode, " +
                 "  'productCode', CAST(x.inclusionReasonValueText.productCode AS int)" +
                 ") as inclusionReasonValueText" +
                 "))")
        );
    }
}
