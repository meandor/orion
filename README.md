# Orion

[![CircleCI](https://circleci.com/gh/meandor/orion.svg?style=svg)](https://circleci.com/gh/meandor/orion)

Spark job transforming data from [favorita-grocery-sales-forecasting](https://www.kaggle.com/c/favorita-grocery-sales-forecasting).

The transformed dataset is saved in `/grocery/dataset/<GENERATION-NUMBER>/train.parquet` on the HDFS.

## Test
```bash
./gradlew check
```

## Build jar
```bash
./gradlew shadowJar
```

## Run on cluster
```bash
spark-submit --class com.github.meandor.Orion --master yarn --deploy-mode cluster --conf "spark.dynamicAllocation.enabled"="true" --conf "spark.dynamicAllocation.minExecutors"="1" --conf "spark.dynamicAllocation.maxExecutors"="5" --conf "spark.dynamicAllocation.executorIdleTimeout"="30" <jar-file>
```
