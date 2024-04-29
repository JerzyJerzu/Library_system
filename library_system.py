import threading
import random
import time
from cassandra.cluster import Cluster
from cassandra.metadata import KeyspaceMetadata

class DatabaseManagerSingleton:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize(*args, **kwargs)
        return cls._instance
    
    def _initialize(self, contact_points, port, keyspace_name):
        self.cluster = Cluster(contact_points, port=port)
        print('initialization')
        self.session = self.cluster.connect()
        print('connected to cluster')
        self.session.set_keyspace(keyspace_name)
        print('keyspace set')
        self._create_tables_if_not_exist()

    def _create_tables_if_not_exist(self):
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS books (
                book_id uuid,
                title text,
                author text,
                available boolean,
                PRIMARY KEY (book_id, title, author)
            )
            """
        )
        print('created books')
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id uuid,
                username text,
                PRIMARY KEY (user_id, username)
            )
            """
        )
        print('created users')
        self.session.execute(
            # POTENTIAL DANGER primary key
            """
            CREATE TABLE IF NOT EXISTS reservations (
                user_id text,
                book_id text,
                PRIMARY KEY ((user_id, book_id)),
            )
            """
        )
        print('created reservations')

def main():
    print('hello!')
    contact_points = ['127.0.1.1', '127.0.1.2', '127.0.1.3']
    port = 9042
    keyspace_name = 'library_project'

    db_manager = DatabaseManagerSingleton(contact_points,port,keyspace_name)

    #cluster = Cluster(contact_points, port)
    #session=cluster.connect()

    print('done')
if __name__ == "__main__":
    
    main()
