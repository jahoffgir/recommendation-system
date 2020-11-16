from mysql.connector import pooling
from mysql import connector


db_credentials = {
    'user': "root",
    'password': 'password',
    'host': 'localhost',
    'database': 'recommend',
    'charset': 'utf8'
}


class MySQL:

    def __init__(self):
        self.connection_pool = None
        self.db_config = db_credentials
        self.create_pool()

    def create_pool(self):
        self.connection_pool = connector.pooling.MySQLConnectionPool(pool_name="pynative_pool",
                                                                           pool_size=4,
                                                                           pool_reset_session=False,
                                                                           **self.db_config)

    def get_connection(self):
        return self.connection_pool.get_connection()
