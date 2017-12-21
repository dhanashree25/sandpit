from pyspark import SparkContext, SparkConf

if __name__=="__main__":
    conf =  SparkConf().setMaster("local[2]").setAppName("My Spark Application")

    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    
    data = sc.textFile("/home/vagrant/sandpit/test_data.txt")
    
    print data.take(4)
    data.persist()
      
    length =data.map(lambda s: len(s))
    print length.collect()
    print "total={}".format(length.reduce(lambda a, b: a + b))
      
    def try_function(s):
        words = s.split(",")[0]
        wordlen = len(words)
          
        return wordlen
      
    fun = data.map(try_function)
    print fun.collect()
      
    length.persist()
    
    mmap =data.map(lambda y: y.split("&")[0].split("=") )
    keydata= data.map(lambda y: y if y.split("&")[0].split("=")[1]=='APP_EVENT' else y[1])
    
    print keydata.collect()
    keydata.persist()
 
#     flatm = data.flatMap(lambda x: x.split(","))

    
    