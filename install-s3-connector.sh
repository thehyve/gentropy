#!/bin/bash

set -ex

SPARK_HOME=${SPARK_HOME:-$(find_spark_home.py)}
mkdir -p ${SPARK_HOME}/conf/
touch ${SPARK_HOME}/conf/spark-defaults.conf

if [ ! -e ${SPARK_HOME}/jars/hadoop-aws-3.2.0.jar ]
then
    curl -sSL \
        https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar \
        > ${SPARK_HOME}/jars/hadoop-aws-3.2.0.jar
fi

if [ ! -e ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.11.375.jar ]
then
    curl -sSL \
        https://search.maven.org/remotecontent?filepath=com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar \
        > ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.11.375.jar
fi

# set default aws credentials providers that try, in order: aws cli credentials and anonymous credentials.
sed -i.bak \
    '/^### START: DO NOT EDIT, MANAGED BY: install-s3-connector.sh$/,/### END: DO NOT EDIT, MANAGED BY: install-s3-connector.sh/d' \
    ${SPARK_HOME}/conf/spark-defaults.conf
rm ${SPARK_HOME}/conf/spark-defaults.conf.bak
cat >> ${SPARK_HOME}/conf/spark-defaults.conf <<EOF
### START: DO NOT EDIT, MANAGED BY: install-s3-connector.sh
#spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.profile.ProfileCredentialsProvider,com.amazonaws.auth.profile.ProfileCredentialsProvider,org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider
spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider
### END: DO NOT EDIT, MANAGED BY: install-s3-connector.sh
spark.eventLog.enabled           true
spark.eventLog.dir               file:///ot/gentropy/spark_log
spark.executor.memory            350g
spark.driver.memory              350g
EOF
