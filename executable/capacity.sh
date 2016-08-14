echo "Running Capacity Scheduler"

#START=$(date +%s)

rm $HADOOP_HOME/conf/hadoop-site.xml

cp $HOME/adaptive/hadoop-site.xml-capacity $HADOOP_HOME/conf/hadoop-site.xml

cd $HADOOP_HOME

#$HADOOP_HOME/bin/hadoop jar hadoop-0.20.2-examples.jar pi 10 1000000

#$HADOOP_HOME/bin/hadoop jar /root/adaptive/experiments/cloud9-1.1.1.jar edu.umd.cloud9.example.bigram.BigramCount data/bible+shakes.nopunc bigram 5

#$HADOOP_HOME/bin/hadoop jar /root/adaptive/experiments/cloud9-1.1.1.jar edu.umd.cloud9.example.ir.BuildInvertedIndex data/bible+shakes.nopunc index 100000

#$HADOOP_HOME/bin/hadoop jar /root/adaptive/experiments/cloud9-1.1.1.jar edu.umd.cloud9.example.ir.LookupPostings index data/bible+shakes.nopunc

#$HADOOP_HOME/bin/hadoop jar /root/adaptive/experiments/cloud9-1.1.1.jar edu.umd.cloud9.example.ir.BooleanRetrieval index data/bible+shakes.nopunc

$HADOOP_HOME/bin/hadoop jar hadoop-0.19.0-examples.jar wordcount data output

echo "Finished Running Capacity Scheduler"


#END=$(date +%s)
#DIFF=$(( $END - $START ))
#echo "It took $DIFF seconds"

