# running on codespace

[Codespaces](https://github.com/codespaces)

```sh
# install java
cd ~/spark
wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
java --version

# set java path
export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"

# install spark
wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar xzfv spark-3.3.2-bin-hadoop3.tgz

# set spark path
export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"

# pyspark
# check the version of py4j-0.10.9.5-src.zip
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

# test spark
spark-shell
# val data = 1 to 10000
# val distData = sc.parallelize(data)
# distData.filter(_ < 10).collect()
```

- Port Fowarding
  - 8080: jupyter notebook
  - 4040: Spark UI