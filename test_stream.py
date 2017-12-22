from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# parse the messages
def message_parser(data):
    message_value_dict = data.map(lambda x: x.split("&"))
    return message_value_dict

def try_function(s):
    words = s.split(",")[0]
    wordlen = len(words)
           
    return wordlen


if __name__ == "__main__":
    
    # Initialize a SparkContext with a name
    spc = SparkContext(appName="MessageStream")

#     # Create a StreamingContext with a batch interval of 2 seconds
    sc = StreamingContext(spc, 10)
 
     # Checkpointing feature
    sc.checkpoint("checkpoint")
    
    # Set credentials
#     pyspark --packages org.apache.hadoop:hadoop-aws:2.7.1
#     sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "Access_key")
#     sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "AWS_password")

    data = sc.textFileStream("file:/home/vagrant/test_data.txt")

    length = data.map(lambda s: len(s))
    length.pprint()

    # Start the computation
    sc.start()
 
    # Wait for the computation to terminate
    sc.awaitTermination()
