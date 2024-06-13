import threading
import random
import time
import unittest
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.metadata import KeyspaceMetadata
from cassandra.query import SimpleStatement, ConsistencyLevel
from datetime import datetime, timezone
import time

class ExampleTest(unittest.TestCase):
    def test_stress_1(self):
        start_time = time.time()
        db_manager = DatabaseManagerSingleton(['127.0.1.1', '127.0.1.2', '127.0.1.3'])
        username = "test_user"
        book_title = "Test Book"
        due_date_str = "20.06.2024"

        db_manager.add_book(book_title, "Test Author")

        books = db_manager.get_books_by_title(book_title)
        book_id = books[0].book_id

        db_manager.add_user(username)
        succesfull_reservations = 0
        lock = threading.Lock()
        def make_reservation_thread():
            nonlocal succesfull_reservations
            for i in range(1):
                if db_manager.make_reservation(username, book_title, book_id, due_date_str):
                    print('successful_reservation')
                    with lock:
                        succesfull_reservations += 1
                #else:
                    #print('failed_reservation')
        threads = []
        for i in range(10000):
            t = threading.Thread(target=make_reservation_thread)
            # user is spamming with the same request every 10 miliseconds
            time.sleep(0.01)
            threads.append(t)
            #print('thread ', i, ' started')

            t.start()

        for t in threads:
            t.join()
        print('threads joined succesfully')
        end_time = time.time()
        print(f"Test 1 Execution time: {end_time - start_time} seconds")
        self.assertEqual(succesfull_reservations, 1)

    # adds speciifed number of books and returns their titles
    def add_some_books(self, num_books):
        db_manager = DatabaseManagerSingleton(['127.0.1.1', '127.0.1.2', '127.0.1.3'])
        book_titles = []

        for i in range(num_books):
            book_title = f"Test Book {i+1}"
            db_manager.add_book(book_title, "Test Author")
            book_titles.append(book_title)

        return book_titles
    
    def make_random_requests(self, books, username):
            db_manager = DatabaseManagerSingleton(['127.0.1.1', '127.0.1.2', '127.0.1.3'])
            db_manager.add_user(username)
            due_date_str = "01.01.2023"
            for _ in range(2000):
                book_title = random.choice(books)
                books_by_title = db_manager.get_books_by_title(book_title)
                book_id = books_by_title[0].book_id
                db_manager.make_reservation(username, book_title, book_id, due_date_str)
    
    def test_stress_2(self):
        threads = []
        books = self.add_some_books(200)
        usernames = ['Test_user_A', 'Test_user_B', 'Test_user_C', 'Test_user_D', 'Test_user_E']
        start_time = time.time()
        for username in usernames:
            thread = threading.Thread(target=self.make_random_requests, args=(books, username))
            threads.append(thread)
            thread.start()

        for t in threads:
            t.join()

        print('All threads joined')
        end_time = time.time()
        print(f"Test 1 Execution time: {end_time - start_time} seconds")
        self.assertTrue(self.check_reserved_books(usernames))

    def test_stress_3(self):
        db_manager = DatabaseManagerSingleton(['127.0.1.1'])
        db_manager.reset_tables()
        books = self.add_some_books(10)
        book_ids = [db_manager.get_books_by_title(title)[0].book_id for title in books]
        if len(book_ids) != len(books):
            print('ERROR: BOOK IDS NOT FOUND')
            return
        db_manager.add_user('Darek')
        db_manager.add_user('Marek')

        threads = []
        barrier = threading.Barrier(2)
        thread_1 = threading.Thread(target=self.claim_books_pool, args=(books, book_ids, 'Darek',['127.0.1.3'],barrier))
        threads.append(thread_1)
        thread_2 = threading.Thread(target=self.claim_books_pool, args=(books, book_ids, 'Marek',['127.0.1.2'],barrier))
        threads.append(thread_2)
        start_time = time.time()
        thread_1.start()
        thread_2.start()

        for t in threads:
            t.join()

        end_time = time.time()
        print(f"Test 1 Execution time: {end_time - start_time} seconds")

        Darek_books = db_manager.get_user_reserved_books('Darek')
        Marek_books = db_manager.get_user_reserved_books('Marek')

        print('Darek_books: ', len(Darek_books))
        print(Darek_books)
        print('Marek_books: ', len(Marek_books))
        print(Marek_books)

        self.assertGreater(len(Darek_books), 0)
        self.assertGreater(len(Marek_books), 0)

    def claim_books_pool(self, book_titles, books_ids, username, contact_points, barrier, due_date_str="11.11.2024"):
        db_manager = DatabaseManagerSingleton(contact_points)
        barrier.wait()
        for i in range(len(book_titles)):
            db_manager.make_reservation(username, book_titles[i], books_ids[i], due_date_str)

    def check_reserved_books(self, usernames):
        db_manager = DatabaseManagerSingleton(['127.0.1.1', '127.0.1.2', '127.0.1.3'])
        reserved_books = []
        book_ids = set()

        for username in usernames:
            print(f"Test user: {username}")
            user_reserved_books = db_manager.get_user_reserved_books(username)
            if len(user_reserved_books) > db_manager.max_reserved_books:
                print('USER RESERVED MORE BOOKS THAN ALLOWED!')
                return False
            else:
                print("user reserved books: ", len(user_reserved_books))
            reserved_books.extend(user_reserved_books)

        book_ids = set()
        for book in reserved_books:
            if book[0].book_id in book_ids:
                return False
            #print(book[0].book_id)
            book_ids.add(book[0].book_id)
        
        print('STRESS TEST 2 CORRECT')
        return True
    
    def a_test_100_user_inserts(self):
        db_manager = DatabaseManagerSingleton()
        for i in range(100):
            db_manager.add_user(f'user{i}')
            self.assertEqual(db_manager.check_username_exists(f'user{i}'), True)

class DatabaseManagerSingleton():  
    def __init__(self, contact_points, logs_enabled=False, max_reserved_books=20):
        self.cluster = Cluster(contact_points, port=9042)
        self.logs_enabled = logs_enabled
        self.max_reserved_books = max_reserved_books
        self.log('initialization')
        self.session = self.cluster.connect()
        self.log('connected to cluster')
        self.session.set_keyspace('library_project')
        self.log('keyspace set')
        self.create_tables_if_not_exist()        
    def log(self, message):
        if self.logs_enabled:
            print(message)
    def create_tables_if_not_exist(self):
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS books (
                book_id uuid,
                title text,
                author text,
                available boolean,
                PRIMARY KEY (title, book_id)
            )
            """
        )
        self.log('created books')
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                username text,
                reserved_books INT,
                PRIMARY KEY (username)
            )
            """
        )
        self.log('created users')
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS reservations (
            username text,
            book_id uuid,
            book_title text,
            due_date timestamp,
            PRIMARY KEY (username, book_id)
            )
            """
        )
        self.log('created reservations')
    def reset_tables(self):
        self.session.execute("DROP TABLE IF EXISTS books")
        self.session.execute("DROP TABLE IF EXISTS users")
        self.session.execute("DROP TABLE IF EXISTS reservations")
        self.create_tables_if_not_exist()  
    def add_book(self, title, author):
        query = SimpleStatement("""
            INSERT INTO books (book_id, title, author, available) 
            VALUES (uuid(), %s, %s, true)
        """, consistency_level=ConsistencyLevel.TWO)
        self.session.execute(query, (title, author))
        return True
    def get_books_by_title(self, title):
        query = SimpleStatement("SELECT * FROM books WHERE title = %s", consistency_level=ConsistencyLevel.ONE)
        rows = self.session.execute(query, (title,))
        return rows._current_rows   
    def add_user(self, username):
        query = SimpleStatement("""
            INSERT INTO users (username, reserved_books)
            VALUES (%s, 0)
            IF NOT EXISTS
        """)
        result = self.session.execute(query, (username,))
        if result.one().applied:
            self.log("User added successfully!")
            return True
        else:
            self.log("Username already exists. Please choose a different username.")
            return False   
    def check_username_exists(self, username):
        query = SimpleStatement("SELECT * FROM users WHERE username = %s", consistency_level=ConsistencyLevel.TWO)
        self.log('checking username exists')
        rows = self.session.execute(query, (username,))
        if len(rows._current_rows) > 0:
            self.log('user exists')
            return True
        self.log('user does not exist')
        return False
    def get_book(self, title, book_id):
            query = SimpleStatement("SELECT * FROM books WHERE title = %s AND book_id = %s", consistency_level=ConsistencyLevel.TWO)
            rows = self.session.execute(query, (title, book_id))
            return rows.one()
    def check_user_reserved_books(self, username):
        query = SimpleStatement("SELECT reserved_books FROM users WHERE username = %s", consistency_level=ConsistencyLevel.TWO)
        rows = self.session.execute(query, (username,))
        return rows.one().reserved_books
    def lock_book(self, book_id, title):
        query = SimpleStatement("""
            UPDATE books
            SET available = false
            WHERE book_id = %s AND title = %s
            IF available = true
            """, consistency_level=ConsistencyLevel.TWO)
        try:
            query_result = self.session.execute(query, (book_id, title))
        except NoHostAvailable as e:
            print(f"Error occurred: {e}")
            return False
        if not query_result.one().applied:
            self.log("Didn't manage to lock the book")
            return False
        return True
    def unlock_book(self, book_id, title):
        query = SimpleStatement("""
            UPDATE books
            SET available = true
            WHERE book_id = %s AND title = %s
            """, consistency_level=ConsistencyLevel.TWO)
        query_result = self.session.execute(query, (book_id, title))
        if not query_result.one().applied:
            raise Exception("Failed to release lock on book. You shouldn't be seeing this message!")
        return True   
    def make_reservation(self, username, book_title, book_id, due_date_str):
        if not self.check_username_exists(username):
            self.log("User does not exist. Cannot make reservation.")
            return False        
        if self.check_user_reserved_books(username) >= self.max_reserved_books:
            self.log("User has already reserved meximum number of books. Cannot make more reservations.")
            return False
        due_date = datetime.strptime(due_date_str, '%d.%m.%Y')
        due_date = due_date.replace(tzinfo=timezone.utc)        
        book = self.get_book(book_title, book_id)
        if book:
            if not book.available:
                self.log("Book is unavailable. Cannot make reservation.")
                return False
            # Set book to unavailable
            if not self.lock_book(book_id, book_title):
                return False   
        else:
            self.log("Book not found. You should not be seeing this message.")
            return False
        # reserve user slot for a book
        if not self.increment_user_reserved_books(username):
            self.unlock_book(book_id, book_title)
        query = SimpleStatement("""
            INSERT INTO reservations (username, book_id, book_title, due_date)
            VALUES (%s, %s, %s, %s)
        """, consistency_level=ConsistencyLevel.TWO)
        self.session.execute(query, (username, book_id, book_title, due_date))
        #time.sleep(0.2)
        self.log("Reservation made successfully!")
        return True  
    def get_user_reserved_books(self, username):
        query = SimpleStatement("""
                SELECT book_title, book_id, due_date
                FROM reservations
                WHERE username = %s
        """, consistency_level=ConsistencyLevel.ONE)
        rows = self.session.execute(query, (username,))
        #print(rows._current_rows)
        #print([(self.get_book(row.book_title, row.book_id), row.due_date) for row in rows._current_rows])
        return [(self.get_book(row.book_title, row.book_id), row.due_date) for row in rows._current_rows]
    # DOES NOT SUPPORT CONCURENT OPERATIONS, BUT NOT REQUIRED IN THE PROJECT
    def finish_reservation(self, username, book_id, book_title):
        # Check if the user exists
        if not self.check_username_exists(username):
            self.log("User does not exist. Cannot finish reservation. You should not be seeing this message.")
            return False
        
        # Check if the book exists
        book = self.get_book(book_title, book_id)
        if not book:
            self.log("Book not found. Cannot finish reservation. You should not be seeing this message.")
            return False
        
        # Check if the book is reserved by the user
        query = SimpleStatement("""
            SELECT *
            FROM reservations
            WHERE username = %s AND book_id = %s
        """, consistency_level=ConsistencyLevel.ONE)
        rows = self.session.execute(query, (username, book_id))
        if not rows:
            self.log("User has not reserved this book. Cannot finish reservation.")
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
            WHERE book_id = %s AND title = %s
        """, consistency_level=ConsistencyLevel.TWO)
        self.session.execute(query, (book_id, book.title))
        
        print("Reservation finished!")
        return True
    def increment_user_reserved_books(self, username):
        query = SimpleStatement("UPDATE users SET reserved_books = reserved_books + 1 WHERE username = %s IF reserved_books < %s", consistency_level=ConsistencyLevel.TWO)
        result = self.session.execute(query, (username, self.max_reserved_books))
        if result.one().applied:
            self.log('books incermeted')
            return True
        else:
            self.log("Failed to increment reserved books. User has already reserved the maximum number of books.")
            return False
    # DOES NOT SUPPORT CONCURENT OPERATIONS, BUT NOT REQUIRED IN THE PROJECT
    def decrement_user_reserved_books(self, username):
        no_reserved_books = self.check_user_reserved_books(username)
        decremented_reserved_books = no_reserved_books - 1
        query = SimpleStatement("UPDATE users SET reserved_books = %s WHERE username = %s IF reserved_books > 0", consistency_level=ConsistencyLevel.TWO)
        result = self.session.execute(query, (decremented_reserved_books, username,))
        if not result.one().applied:
            raise Exception("Trying to decrement reserved books below 0.")
    def update_reservation_due_date(self, username, book_id, due_date_str):
        due_date = datetime.strptime(due_date_str, '%d.%m.%Y')
        due_date = due_date.replace(tzinfo=timezone.utc)
        query = SimpleStatement("""
            UPDATE reservations
            SET due_date = %s
            WHERE username = %s AND book_id = %s
            IF EXISTS
        """, consistency_level=ConsistencyLevel.TWO)
        result = self.session.execute(query, (due_date, username, book_id))
        if result.one().applied:
            return True
        return False
class MenuDialogSingleton:
    _instance = None
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize(*args, **kwargs)
        return cls._instance
    def _initialize(self, db_manager):
        self.db_manager = db_manager       
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
    def make_reservation_dialog(self, book_id, book_title):
        username = input("Enter a username to make reservation: ")
        while True:
            due_date = input("Enter the due date of the reservation (dd.mm.yyyy): ")
            try:
                datetime.strptime(due_date, "%d.%m.%Y")
                break
            except ValueError:
                print("Invalid date format. Please try again.")
        if self.db_manager.make_reservation(username, book_title, book_id, due_date):
            return
        aborting = input("Reservation failed. Press N to return to main menu, or any other key to try again: ")
        if aborting.upper() == "N":
            return
        self.make_reservation_dialog(book_id, book_title)
    # TODO display also by whom the book is reserved
    def search_book_dialog(self):
        search_term = input("Enter the title of the book: ")
        books = self.db_manager.get_books_by_title(search_term)
        
        if len(books) > 10:
            print(f"Matching books: {len(books)} books found.")
            proceed = input("Do you want to proceed? (Y/N): ")
            if proceed.upper() != "Y":
                return
        
        if books:
            print("Matching books:")
            for i, book in enumerate(books):
                if book.available:
                    print(f"{i+1}. {book.title} by {book.author} [Available], ID: {book.book_id}")
                else:
                    print(f"{i+1}. {book.title} by {book.author} [Reserved], ID: {book.book_id}")
                """
                    reserved_by = self.db_manager.check_user_reserved_books(book.reserved_by)
                    print(f"{i+1}. {book.title} by {book.author} [Reserved by: {reserved_by}], ID: {book.book_id}")
                """
            book_index = input("Enter the index of the book you wish to reserve, or N to cancel:")
            if book_index.upper() == "N":
                return
            book_index = int(book_index)
            if book_index < 1 or book_index > len(books):
                print("Invalid book index. Please try again.")
                self.search_book_dialog()
                return
            book_id = books[book_index-1].book_id
            book_title = books[book_index-1].title
            self.make_reservation_dialog(book_id, book_title)
        else:
            print("No matching books found.")
        return     
    def search_user_dialog(self):
        username = input("Enter the username: ")
        if not self.db_manager.check_username_exists(username):
            print("User not found.")
            return
        else:
            self.view_user_reservations(username)
            return
    def prolong_reservation_dialog(self, username, book_id):
        while True:
            due_date = input("Enter the new due date of the reservation (dd.mm.yyyy): ")
            try:
                datetime.strptime(due_date, "%d.%m.%Y")
                break
            except ValueError:
                print("Invalid date format. Please try again.")
        if self.db_manager.update_reservation_due_date(username, book_id, due_date):
            print("Reservation prolonged successfully!")
        else:
            print("Reservation prolongation failed.")
        return
    def view_user_reservations(self, username):
        reservations = self.db_manager.get_user_reserved_books(username)
        if reservations:
            print(f"Reservations for user {username}:")
            for i, book in enumerate(reservations):
                print(f"{i+1}. {book[0].title} by {book[0].author}, ID: {book[0].book_id}, Due date: {book[1]}")
            
            reservation_index = input("Enter the index of the reservation you wish to select, or N to cancel:")
            if reservation_index.upper() == "N":
                return
            reservation_index = int(reservation_index)
            if reservation_index < 1 or reservation_index > len(reservations):
                print("Invalid reservation index. Please try again.")
                self.view_user_reservations(username)
                return
            book_id = reservations[reservation_index-1][0].book_id
            book_title = reservations[reservation_index-1][0].title
            choice = input("Press F to finish the reservation, P to prolong it or any other key to cancel: ")
            if choice.upper() == "F":
                self.db_manager.finish_reservation(username, book_id, book_title)
            elif choice.upper() == "P":
                self.prolong_reservation_dialog(username, book_id)
            else:
                return
        else:
            print(f"No reservations found for user {username}.")
        return

if __name__ == "__main__":
    unittest.main()
    #print('TESTS FINISHED')
    contact_points = ['127.0.1.1', '127.0.1.2', '127.0.1.3']
    #contact_points = ['127.0.1.3']
    db_manager = DatabaseManagerSingleton(contact_points)
    #print('Database manager initialized')
    db_manager.reset_tables()
    menu_dialog = MenuDialogSingleton(db_manager)
    #print('Menu dialog initialized')
    menu_dialog.show_menu()