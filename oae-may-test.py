from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


def String_Trans_to_Int( data ):
    '''
     back and forth transformation for userId
    :return: numerical ID rating data    frame.
    '''

    # Assign unique Long id for each userId
    userIdToInt = data.map(data.select("OLS_TXN_CIF_NBR")).distinct().zipWithUniqueId()
    userIdToInt.co


    # Reverse mapping from generated id to original
    #reverseMapping: userIdToInt.map{case(l, r) = > (r, l)}

    # Depends on data size, maybe too big to keep
    # on single machine
    map= userIdToInt.collect().toMap.mapValues(_.toInt)

    # Transform to MLLib rating
    #rating= data.map
    #{r = >    Rating(userIdToInt.lookup(r.userId).head.toInt, r.product, r.rating)
    # -- or
    #Rating(map(r.userId), r.product, r.rating)
    #}

    # ... train model

    # ... get back to MyRating userId from Int

    #someUserId: String = reverseMapping.lookup(123).head
