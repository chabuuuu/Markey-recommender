import psycopg2
from surprise import Dataset, Reader
from collections import defaultdict
import pandas as pd
from dotenv import load_dotenv
import os
from psycopg2 import pool

load_dotenv()

class DatabaseConnection:
    """Singleton class for managing PostgreSQL connection pool."""
    _instance = None
    _connection_pool = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)

            # Load database configuration from environment
            DB_HOST = os.getenv('DB_HOST')
            DB_NAME = os.getenv('DB_NAME')
            DB_USERNAME = os.getenv('DB_USERNAME')
            DB_PASSWORD = os.getenv('DB_PASSWORD')
            DB_PORT = os.getenv('DB_PORT')

            cls._connection_pool = pool.SimpleConnectionPool(
                1, 10,  # Min and max number of connections
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USERNAME,
                password=DB_PASSWORD,
                port=DB_PORT
            )
        return cls._instance

    def get_connection(self):
        """Get a connection from the pool."""
        return self._connection_pool.getconn()

    def release_connection(self, connection):
        """Return a connection to the pool."""
        self._connection_pool.putconn(connection)

    def close_all_connections(self):
        """Close all connections in the pool."""
        self._connection_pool.closeall()

class ProductRecommendation:
    productID_to_name = {}
    name_to_productID = {}
    productID_to_details = {}  # Store details: category_id, price

    def __init__(self):
        self.db_connection = DatabaseConnection()

    def _connect_db(self):
        """Get a connection from the database connection pool."""
        return self.db_connection.get_connection()

    def loadProductData(self):
        """Load product ratings dataset and product information from PostgreSQL."""
        self.productID_to_name = {}
        self.name_to_productID = {}
        self.productID_to_details = {}

        connection = self._connect_db()
        cursor = connection.cursor()

        # Load ratings dataset
        cursor.execute("SELECT shopper_id, product_id, rating FROM product_ratings")
        ratings = cursor.fetchall()

        # Use Surprise's Reader to format data
        reader = Reader(line_format='user item rating timestamp', sep=',')
        ratings_df = pd.DataFrame(ratings, columns=['shopper_id', 'product_id', 'rating'])

        ratingsDataset = Dataset.load_from_df(ratings_df, reader)

        # Load product details
        cursor.execute("SELECT id, name, category_id, price FROM products")
        products = cursor.fetchall()

        for row in products:
            productID = row[0]  # 'id' column
            productName = row[1]  # 'name' column
            category_id = row[2]  # 'category_id' column
            price = row[3]  # 'price' column

            self.productID_to_name[productID] = productName
            self.name_to_productID[productName] = productID
            self.productID_to_details[productID] = {
                'category_id': category_id,
                'price': price,
            }

        cursor.close()
        connection.close()

        return ratingsDataset
    
    def loadProductDataDf(self):
        """Load product ratings dataset and product information from PostgreSQL."""
        connection = self._connect_db()
        cursor = connection.cursor()

        # Load ratings dataset
        cursor.execute("SELECT shopper_id, product_id, rating FROM product_ratings")
        ratings = cursor.fetchall()

        # Use Surprise's Reader to format data
        reader = Reader(line_format='user item rating timestamp', sep=',')
        ratings_df = pd.DataFrame(ratings, columns=['shopper_id', 'product_id', 'rating'])

        ratingsDataset = Dataset.load_from_df(ratings_df, reader)

        # Load product details
        cursor.execute("SELECT id, name, category_id, price FROM products")
        products = cursor.fetchall()

        for row in products:
            productID = row[0]  # 'id' column
            productName = row[1]  # 'name' column
            category_id = row[2]  # 'category_id' column
            price = row[3]  # 'price' column

            self.productID_to_name[productID] = productName
            self.name_to_productID[productName] = productID
            self.productID_to_details[productID] = {
                'category_id': category_id,
                'price': price,
            }

        cursor.close()
        connection.close()

        return ratings_df

    def getUserRatings(self, user):
        """Get all ratings given by a specific user from PostgreSQL."""
        userRatings = []
        connection = self._connect_db()
        cursor = connection.cursor()

        cursor.execute("SELECT product_id, rating FROM product_ratings WHERE shopper_id = %s", (user,))
        ratings = cursor.fetchall()

        for row in ratings:
            productID = row[0]  # 'product_id' column
            rating = float(row[1])  # 'rating' column
            userRatings.append((productID, rating))

        cursor.close()
        connection.close()

        return userRatings

    def getPopularityRanks(self):
        """Calculate popularity ranks for products based on the number of ratings."""
        ratings = defaultdict(int)
        rankings = defaultdict(int)

        connection = self._connect_db()
        cursor = connection.cursor()

        cursor.execute("SELECT product_id FROM product_ratings")
        ratings_data = cursor.fetchall()

        for row in ratings_data:
            productID = row[0]  # 'product_id' column
            ratings[productID] += 1

        rank = 1
        for productID, ratingCount in sorted(ratings.items(), key=lambda x: x[1], reverse=True):
            rankings[productID] = rank
            rank += 1

        cursor.close()
        connection.close()

        return rankings

    def getProductName(self, productID):
        """Get product name by its ID."""
        return self.productID_to_name.get(productID, "")

    def getProductID(self, productName):
        """Get product ID by its name."""
        return self.name_to_productID.get(productName, 0)

    def getCategoryID(self, productID):
        """Get category ID of a product by its ID."""
        return self.productID_to_details.get(productID, {}).get('category_id', "")

    def getPrice(self, productID):
        """Get price of a product by its ID."""
        return self.productID_to_details.get(productID, {}).get('price', "")

    
    # Save recommend product ids to product_recommendations table
    def saveRecommendations(self, user, products):
        connection = self._connect_db()
        cursor = connection.cursor()

        products = ",".join(products)

        cursor.execute("INSERT INTO recommends (user_id, products) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET products = EXCLUDED.products", (user, products))

        connection.commit()
        cursor.close()
        connection.close()

    def saveRecommendForEveryUser(self, recommendForEveryUser):
        connection = self._connect_db()
        for recommend in recommendForEveryUser:
            cursor = connection.cursor()

            products = ",".join(recommend[1])

            cursor.execute("INSERT INTO recommends (user_id, products) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET products = EXCLUDED.products", (recommend[0], products))

            connection.commit()
            cursor.close()
        connection.close()

    # Load all users that have ratings
    def loadUsers(self):
        connection = self._connect_db()
        cursor = connection.cursor()

        cursor.execute("SELECT DISTINCT shopper_id FROM product_ratings")
        users = cursor.fetchall()

        cursor.close()
        connection.close()

        return users


# Example of how to use the modified class
if __name__ == "__main__":

    productRecommendation = ProductRecommendation()
    dataset = productRecommendation.loadProductData()
    print("Dataset loaded successfully.")

    # Get popularity ranks
    print("Top 5 popular products:", list(productRecommendation.getPopularityRanks().items())[:5])

    # Get product name from ID
    product_name = productRecommendation.getProductName("61ecfa2b-909c-4f1f-9c44-5833310989bc")
    print(f"Product Name: {product_name}")

    # Get product ID from name
    product_id = productRecommendation.getProductID("Laptop Dell Inspiron 15")
    print(f"Product ID: {product_id}")

    # Get product details
    category_id = productRecommendation.getCategoryID("c15ff891-9129-41a5-b85f-e687fc4c5213")
    price = productRecommendation.getPrice("c15ff891-9129-41a5-b85f-e687fc4c5213")
    print(f"Category ID: {category_id}, Price: {price}")
