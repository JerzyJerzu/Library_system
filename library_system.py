import threading
import random
import time
from cassandra.cluster import Cluster
from cassandra.metadata import KeyspaceMetadata
from cassandra.query import SimpleStatement, ConsistencyLevel

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
            # ADD due date
            """
            CREATE TABLE IF NOT EXISTS reservations (
                user_id uuid,
                book_id uuid,
                PRIMARY KEY ((user_id, book_id)),
            )
            """
        )
        print('created reservations')
    
    def add_book(self, title, author):
        query = SimpleStatement("""
            INSERT INTO books (book_id, title, author, available) 
            VALUES (uuid(), %s, %s, true)
        """, consistency_level=ConsistencyLevel.TWO)
        self.session.execute(query, (title, author))

    def get_books_by_title(self, title):
        query = SimpleStatement("SELECT * FROM books WHERE title = %s ALLOW FILTERING", consistency_level=ConsistencyLevel.ONE)
        rows = self.session.execute(query, (title,))
        return rows._current_rows

    def get_books_by_author(self, author):
        query = SimpleStatement("SELECT * FROM books WHERE author = %s ALLOW FILTERING", consistency_level=ConsistencyLevel.ONE)
        rows = self.session.execute(query, (author,))
        return rows._current_rows

class MenuDialogSingleton:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def show_menu(self):
        print("Menu:")
        print("1. Add a book")
        print("2. Add a new user")
        print("3. Search for a book")
        choice = input("Enter your choice: ")
        self.process_choice(choice)

    def process_choice(self, choice):
        if choice == "1":
            self.add_book_dialog()
        elif choice == "2":
            self.add_user_dialog()
        elif choice == "3":
            self.search_book_dialog()
        else:
            self.dialog.show_dialog("Invalid choice. Please try again.")
            self.show_menu()

    def add_book_dialog(self):
        self.dialog.show_dialog("Add a book dialog")
        # Add code to handle adding a book

    def add_user_dialog(self):
        self.dialog.show_dialog("Add a new user dialog")
        # Add code to handle adding a new user

    def search_book_dialog(self):
        self.dialog.show_dialog("Search for a book dialog")
        # Add code to handle searching for a book

def main():
    print('hello!')
    contact_points = ['127.0.1.1', '127.0.1.2', '127.0.1.3']
    port = 9042
    keyspace_name = 'library_project'

    db_manager = DatabaseManagerSingleton(contact_points, port, keyspace_name)
    menu_dialog = MenuDialog()
    menu_dialog.show_menu()
    dialog.show_dialog('Done')

if __name__ == "__main__":
    main()
    

def main():
    print('hello!')
    contact_points = ['127.0.1.1', '127.0.1.2', '127.0.1.3']
    port = 9042
    keyspace_name = 'library_project'

    db_manager = DatabaseManagerSingleton(contact_points, port, keyspace_name)
    dialog = DialogSingleton()

    lotr_books = db_manager.get_books_by_title('LOTR')
    print(lotr_books)
    print(lotr_books[1].book_id)
    db_manager.get_replicas('books', lotr_books[1].book_id)
    dialog.show_dialog('Done')

if __name__ == "__main__":
    main()
