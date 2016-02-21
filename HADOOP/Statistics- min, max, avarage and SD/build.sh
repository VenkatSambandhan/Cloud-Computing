if [ ! -d classes ]; then
        mkdir classes;
fi

# Compile WordCount
javac -classpath $HADOOP_HOME/hadoop-core-1.1.2.jar:$HADOOP_HOME/lib/commons-cli-1.2.jar -d ./classes *.java

# Create the Jar
jar -cvf statistics.jar -C ./classes/ .

hadoop jar statistics.jar statistics input output

