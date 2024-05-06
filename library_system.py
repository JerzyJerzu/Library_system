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
        #self.session.execute("DROP TABLE IF EXISTS books")
        #self.session.execute("DROP TABLE IF EXISTS users")
        #self.session.execute("DROP TABLE IF EXISTS reservations")

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
            # counter is 0 by default
            """
            CREATE TABLE IF NOT EXISTS users (
                username text,
                reserved_books counter,
                PRIMARY KEY (username)
            )
            """
        )
        print('created users')
        self.session.execute(
            # POTENTIAL DANGER primary key
            # ADD due date
            """
            CREATE TABLE IF NOT EXISTS reservations (
                username text,
                book_id uuid,
                PRIMARY KEY ((username, book_id)),
            )
            """
        )
        print('created reservations')
    # POTENTIAL CONCURRENT ISSUE
    def add_book(self, title, author):
        query = SimpleStatement("""
            INSERT INTO books (book_id, title, author, available) 
            VALUES (uuid(), %s, %s, true)
        """, consistency_level=ConsistencyLevel.TWO)
        self.session.execute(query, (title, author))
    # which consisteny level to use?
    # SERIOUS ISSUE: SOMETIMES DOES NOT RETURN ALL ROWS
    # why did copilot recommend above comment?
    def get_books_by_title(self, title):
        query = SimpleStatement("SELECT * FROM books WHERE title = %s ALLOW FILTERING", consistency_level=ConsistencyLevel.ONE)
        rows = self.session.execute(query, (title,))
        return rows._current_rows    
    # which consistency level to use?
    def get_books_by_author(self, author):
        query = SimpleStatement("SELECT * FROM books WHERE author = %s ALLOW FILTERING", consistency_level=ConsistencyLevel.ONE)
        rows = self.session.execute(query, (author,))
        return rows._current_rows    
    # POTENTIAL CONCURRENT ISSUE
    def add_user(self, username):
        if self.check_username_exists(username):
            print("Username already exists. Please choose a different username.")
            return
        # Even if the user does not exist in the table, the UPDATE statement will create a new row with the specified username and set the reserved_books counter to 0.
        query = SimpleStatement("""
            UPDATE users
            SET reserved_books = reserved_books + 0
            WHERE username = %s
        """, consistency_level=ConsistencyLevel.TWO)
        self.session.execute(query, (username,))
        print("User added successfully!")   
    # which consisteny level to use?
    # POTENTIAL CONCURRENT ISSUE
    def check_username_exists(self, username):
        query = SimpleStatement("SELECT * FROM users WHERE username = %s", consistency_level=ConsistencyLevel.ONE)
        print('checking username exists')
        rows = self.session.execute(query, (username,))
        return len(rows._current_rows) > 0    
    # which consisteny level to use?
    # POTENTIAL CONCURRENT ISSUE
    def get_book_by_id(self, book_id):
            query = SimpleStatement("SELECT * FROM books WHERE book_id = %s", consistency_level=ConsistencyLevel.TWO)
            rows = self.session.execute(query, (book_id,))
            return rows.one()
    # which consisteny level to use?
    # POTENTIAL CONCURRENT ISSUE
    def check_user_reserved_books(self, username):
        query = SimpleStatement("SELECT reserved_books FROM users WHERE username = %s", consistency_level=ConsistencyLevel.TWO)
        rows = self.session.execute(query, (username,))
        return rows.one().reserved_books
    # POTENTIAL CONCURRENT ISSUE
    # WHY DO I NEED TO SPECIFY ALL CLYSTERING KEY IN QUERY
    def make_reservation(self, username, book_id):
        if not self.check_username_exists(username):
            print("User does not exist. Cannot make reservation.")
            return False
        
        if self.check_user_reserved_books(username) >= 2:
            print("User has already reserved 2 books. Cannot make more reservations.")
            return False
        
        # LOCK
        # Set book to unavailable
        book = self.get_book_by_id(book_id)
        if book:
            if not book.available:
                print("Book is unavailable. Cannot make reservation.")
                return False
            
            query = SimpleStatement("""
            UPDATE books
            SET available = false
            WHERE book_id = %s AND title = %s AND author = %s
            """, consistency_level=ConsistencyLevel.TWO)
            self.session.execute(query, (book_id, book.title, book.author))
        else:
            print("Book not found. You should not be seeing this message.")
            return False
        
        self.increment_user_reserved_books(username)
        query = SimpleStatement("""
            INSERT INTO reservations (username, book_id)
            VALUES (%s, %s)
        """, consistency_level=ConsistencyLevel.TWO)
        self.session.execute(query, (username, book_id))
        
        # Release lock
        print("Reservation made successfully!")
        return True  
    # POTENTIAL CONCURRENT ISSUE
    def get_user_reserved_books(self, username):
        query = SimpleStatement("""
                SELECT book_id
                FROM reservations
                WHERE username = %s ALLOW FILTERING
        """, consistency_level=ConsistencyLevel.ONE)
        rows = self.session.execute(query, (username,))
        return [self.get_book_by_id(row.book_id) for row in rows._current_rows]
        #return self.get_book_by_id(rows.one().book_id)
    # POTENTIAL CONCURRENT ISSUE
    def finish_reservation(self, username, book_id):
        # Check if the user exists
        if not self.check_username_exists(username):
            print("User does not exist. Cannot finish reservation. You should not be seeing this message.")
            return False
        
        # Check if the book exists
        book = self.get_book_by_id(book_id)
        if not book:
            print("Book not found. Cannot finish reservation. You should not be seeing this message.")
            return False
        
        # Check if the book is reserved by the user
        query = SimpleStatement("""
            SELECT *
            FROM reservations
            WHERE username = %s AND book_id = %s
        """, consistency_level=ConsistencyLevel.ONE)
        rows = self.session.execute(query, (username, book_id))
        if not rows:
            print("User has not reserved this book. Cannot finish reservation.")
            return False

        # LOCK
        query = SimpleStatement("""
            DELETE FROM reservations
            WHERE username = %s AND book_id = %s
        """, consistency_level=ConsistencyLevel.TWO)
        self.session.execute(query, (username, book_id))
        self.decrement_user_reserved_books(username)
        
        # Set the book as available
        query = SimpleStatement("""
            UPDATE books
            SET available = true
            WHERE book_id = %s AND title = %s AND author = %s
        """, consistency_level=ConsistencyLevel.TWO)
        self.session.execute(query, (book_id, book.title, book.author))
        
        print("Reservation finished!")
        return True
    def increment_user_reserved_books(self, username):
        query = SimpleStatement("UPDATE users SET reserved_books = reserved_books + 1 WHERE username = %s", consistency_level=ConsistencyLevel.TWO)
        self.session.execute(query, (username,))
    def decrement_user_reserved_books(self, username):
        query = SimpleStatement("UPDATE users SET reserved_books = reserved_books - 1 WHERE username = %s", consistency_level=ConsistencyLevel.TWO)
        self.session.execute(query, (username,))
class MenuDialogSingleton:
    _instance = None
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize(*args, **kwargs)
        return cls._instance
    def _initialize(self):
        self.db_manager = DatabaseManagerSingleton()          
    def show_menu(self):
        print("Menu:")
        print("1. Add a book")
        print("2. Add a new user")
        print("3. Search for a book")
        print("4. View user's reservations")
        print("5. Exit")
        choice = input("Enter your choice: ")
        self.process_choice(choice)
    def process_choice(self, choice):
        if choice == "1":
            self.add_book_dialog()
        elif choice == "2":
            self.add_user_dialog()
        elif choice == "3":
            self.search_book_dialog()
        elif choice == "4":
            self.search_user_dialog()
        elif choice == "5":
            print("Exiting program...")
            return
        else:
            print("Invalid choice. Please try again.")
        self.show_menu()
    def add_book_dialog(self):
        title = input("Enter the title of the book: ")
        author = input("Enter the author of the book: ")
        confirm = input("Press Y to confirm or any other key to cancel: ")
        if confirm.upper() != "Y":
            return
        self.db_manager.add_book(title, author)
        print("Book added successfully!")
        return
    def add_user_dialog(self):
        username = input("Enter the username: ")
        confirm = input("Press Y to confirm or any other key to cancel: ")
        if confirm.upper() != "Y":
            return
        self.db_manager.add_user(username)
        return    
    def make_reservation_dialog(self, book_id):
        username = input("Enter a username to make reservation: ")
        if self.db_manager.make_reservation(username, book_id):
            return
        aborting = input("Reservation failed. Press N to return to main menu, or any other key to try again: ")
        if aborting.upper() == "N":
            return
        self.make_reservation_dialog(book_id)
    def search_book_dialog(self):
        search_option = input("Search by (1) Title or (2) Author: ")
        if search_option == "1":
            search_term = input("Enter the title of the book: ")
            books = self.db_manager.get_books_by_title(search_term)
        elif search_option == "2":
            search_term = input("Enter the author of the book: ")
            books = self.db_manager.get_books_by_author(search_term)
        else:
            print("Invalid search option. Please try again.")
            self.search_book_dialog()
            return
        
        if len(books) > 5:
            print(f"Matching books: {len(books)} books found.")
            proceed = input("Do you want to proceed? (Y/N): ")
            if proceed.upper() != "Y":
                return
        
        if books:
            print("Matching books:")
            for i, book in enumerate(books):
                print(f"{i+1}. {book.title} by {book.author} [{'Available' if book.available else 'Not available'}], ID: {book.book_id}")

            book_index = input("Enter the index of the book you wish to reserve, or N to cancel:")
            if book_index.upper() == "N":
                return
            # add test for invalid input
            book_index = int(book_index)
            if book_index < 1 or book_index > len(books):
                print("Invalid book index. Please try again.")
                self.search_book_dialog()
                return
            book_id = books[book_index-1].book_id
            self.make_reservation_dialog(book_id)
        else:
            print("No matching books found.")
        return    
    def view_book_details(self, book_id):
        return
        # Add code    
    def search_user_dialog(self):
        username = input("Enter the username: ")
        if not self.db_manager.check_username_exists(username):
            print("User not found.")
            return
        else:
            self.view_user_reservations(username)
            return
    def view_user_reservations(self, username):
        reservations = self.db_manager.get_user_reserved_books(username)
        if reservations:
            print(f"Reservations for user {username}:")
            for i, book in enumerate(reservations):
                print(f"{i+1}. {book.title} by {book.author}, ID: {book.book_id}")
            
            reservation_index = input("Enter the index of the reservation you wish to finish, or N to cancel:")
            if reservation_index.upper() == "N":
                return
            # add test for invalid input
            reservation_index = int(reservation_index)
            if reservation_index < 1 or reservation_index > len(reservations):
                print("Invalid reservation index. Please try again.")
                self.view_user_reservations(username)
                return
            book_id = reservations[reservation_index-1].book_id
            self.db_manager.finish_reservation(username, book_id)
        else:
            print(f"No reservations found for user {username}.")
        return

def main():
    print('hello!')
    contact_points = ['127.0.1.1', '127.0.1.2', '127.0.1.3']
    port = 9042
    keyspace_name = 'library_project'

    db_manager = DatabaseManagerSingleton(contact_points, port, keyspace_name)
    menu_dialog = MenuDialogSingleton()
    menu_dialog.show_menu()

if __name__ == "__main__":
    main()
    
"""
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
"""
