from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession


def train_price_model(spark: SparkSession, config: dict) -> dict[str, float]:
    ml_config = config["ml"]
    frame = spark.read.format("delta").load(config["delta"]["gold_ml_features_path"]).dropna()

    train, test = frame.randomSplit([0.8, 0.2], seed=42)

    categorical_features = ml_config["categorical_features"]
    numeric_features = ml_config["numeric_features"]

    indexers = [
        StringIndexer(inputCol=column, outputCol=f"{column}_idx", handleInvalid="keep")
        for column in categorical_features
    ]
    encoders = [
        OneHotEncoder(inputCol=f"{column}_idx", outputCol=f"{column}_vec")
        for column in categorical_features
    ]
    assembler = VectorAssembler(
        inputCols=[f"{column}_vec" for column in categorical_features] + numeric_features,
        outputCol="features",
        handleInvalid="keep",
    )
    model = RandomForestRegressor(
        featuresCol="features",
        labelCol=ml_config["label_column"],
        predictionCol="predicted_price",
        numTrees=80,
        maxDepth=8,
        seed=42,
    )

    pipeline = Pipeline(stages=indexers + encoders + [assembler, model])
    trained_model = pipeline.fit(train)
    predictions = trained_model.transform(test)

    rmse = RegressionEvaluator(
        labelCol=ml_config["label_column"],
        predictionCol="predicted_price",
        metricName="rmse",
    ).evaluate(predictions)
    r2 = RegressionEvaluator(
        labelCol=ml_config["label_column"],
        predictionCol="predicted_price",
        metricName="r2",
    ).evaluate(predictions)

    trained_model.write().overwrite().save(ml_config["model_output_path"])
    return {"rmse": float(rmse), "r2": float(r2)}

