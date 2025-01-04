from pyspark.sql import SparkSession

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from Evaluator import Evaluator
from ProductRecommendation import ProductRecommendation
from pyspark.ml.feature import StringIndexer
import pandas as pd

if __name__ == "__main__":

    spark = SparkSession\
    .builder\
    .appName("ALSExample")\
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.cores", '5')\
    .getOrCreate()

    def LoadProductData():
        productRecommendation = ProductRecommendation()
        data = productRecommendation.loadProductDataDf()
        rankings = productRecommendation.getPopularityRanks()
        users = productRecommendation.loadUsers()
        return (productRecommendation, data, rankings, users)
        
    # Load up common data set for the recommender algorithms
    (productRecommendation, dataset, rankings, users) = LoadProductData()

    # Convert the ratings dataset into a pandas DataFrame
    ratings_df = pd.DataFrame(dataset, columns=['shopper_id', 'product_id', 'rating'])
        
    ratings = spark.createDataFrame(ratings_df)

    # Index session_id and post_id columns
    shopper_indexer = StringIndexer(inputCol="shopper_id", outputCol="shopper_id_index")
    post_indexer = StringIndexer(inputCol="product_id", outputCol="product_id_index")

    # Fit the indexers
    ratings = shopper_indexer.fit(ratings).transform(ratings)
    ratings = post_indexer.fit(ratings).transform(ratings)
    
    (training, test) = ratings.randomSplit([0.8, 0.2])

    als = ALS(maxIter=5, regParam=0.01, userCol="shopper_id_index", itemCol="product_id_index", ratingCol="rating",
              coldStartStrategy="drop")
    model = als.fit(training)

    predictions = model.transform(test)
    
    # evaluator = Evaluator(model, ratings)
    # evaluator.calculate_metrics(predictions)    

    userRecs = model.recommendForAllUsers(10)

    
    #user85Recs = userRecs.filter(userRecs['session_id'] == "9378998f-ec0f-4d96-9ad2-b711cdefad8e").collect()
    #user85Recs = userRecs.filter(userRecs['session_id'] == "9378998f-ec0f-4d96-9ad2-b711cdefad8e").collect()

    # Save the recommend for all user to redis
    # for row in userRecs.collect():
    #             recommendations = row['recommendations']

    #             # Create a list to hold the post IDs
    #             post_ids = []
    #             for rec in recommendations:
    #                 original_product_id = ratings.filter(ratings['post_id_index'] == rec['post_id_index']).select('post_id').collect()[0]['post_id']
    #                 post_ids.append(original_product_id)

    #             print("POst_ids:", post_ids)

    #             original_session_id = ratings.filter(ratings['shopper_id_index'] == row.shopper_id_index).select('session_id').collect()[0]['session_id']

    #             productRecommendation.save_data_to_redis(key = original_session_id, values= post_ids)




    # Get the index of the specific session_id
    shopper_id_index = ratings.filter(ratings['shopper_id'] == "470512b2-3e4d-4f41-8ef0-08eb9f728938").select('shopper_id_index').collect()[0]['shopper_id_index']

    # Filter recommendations for the specific user
    user85Recs = userRecs.filter(userRecs['shopper_id_index'] == shopper_id_index).collect()


        
    # for row in user85Recs:
    #     for rec in row.recommendations:


    #         print(productRecommendation.get_title_by_id(rec.post_id))

    for row in user85Recs:
        for rec in row.recommendations:
            # Convert numeric post_id back to string post_id]
            original_product_id = ratings.filter(ratings['product_id_index'] == rec.product_id_index).select('product_id').collect()[0]['product_id']
            # Get the title using the original string post_id
            print("Original product_id:", original_product_id)
            print(productRecommendation.getProductName(original_product_id))

    spark.stop()
