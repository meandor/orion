# Orion

[![CircleCI](https://circleci.com/gh/meandor/orion.svg?style=svg)](https://circleci.com/gh/meandor/orion)

## Test
```bash
./gradlew check
```

## Build jar
```bash
./gradlew shadowJar
```

## Run on Yarn cluster
```bash
spark-submit --class com.github.meandor.Orion --master yarn --deploy-mode cluster --conf "spark.dynamicAllocation.enabled"="true" --conf "spark.dynamicAllocation.minExecutors"="1" --conf "spark.dynamicAllocation.maxExecutors"="5" --conf "spark.dynamicAllocation.executorIdleTimeout"="30" <jar-file>
```
