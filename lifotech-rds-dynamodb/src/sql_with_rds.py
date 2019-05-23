import os
import psycopg2
from configparser import ConfigParser

DB_CONFIG_FILE = os.path.dirname(__file__) + '/database.ini'

""" :type: pyboto3.rds"""


def config(filename=DB_CONFIG_FILE, section='postgresql'):
    # create a parser
    parser = ConfigParser()

    # read the configuration
    parser.read(filename)

    # get the section
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]

    else:
        raise Exception('Section {0} not found in the {1}'.format(section, filename))

    return db


def connect_to_rds():
    conn = None

    try:
        # read connection parameter
        params = config()

        # connect to the postgresql database
        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(**params)

        # create cursor
        cursor = conn.cursor()

        # execute the statement
        print('Postgres Database Version')
        cursor.execute('SELECT version()')

        db_version = cursor.fetchone()
        print(db_version)

        cursor.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn:
            conn.close()
            print("Database connection is closed")


def create_tables():
    # provide sql statements
    commands = (
        """
       CREATE TABLE accounts (
           account_id SERIAL PRIMARY KEY,
           account_name VARCHAR(255) NOT NULL
       )
       """,
        """
        CREATE TABLE users(
            user_id SERIAL PRIMARY KEY,
            user_name VARCHAR(255) NOT NULL
        )
        """

    )

    conn = None
    try:
        params = config()
        print(params)

        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        # execute comment one by one
        for command in commands:
            cur.execute(command)

        cur.close()

        conn.commit()

        print("Tables are created")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn:
            conn.close()


def insert_vendor_list(user_list):
    sql = 'INSERT INTO users(user_name) values(%s)'
    conn = None

    try:
        params = config()
        conn = psycopg2.connect(**params)

        cur = conn.cursor()

        cur.executemany(sql, user_list)

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn:
            conn.close()


def get_users():
    conn = None

    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute("SELECT user_id, user_name FROM users ORDER  BY user_name")
        print("Number of users: ", cur.rowcount)
        row = cur.fetchone()
        while (row):
            print(row)
            row = cur.fetchone()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn:
            conn.close()


def update_user(user_id, user_name):
    sql = """ UPDATE users
              SET user_name = %s
              WHERE user_id = %s """

    conn = None
    updated_rows = 0
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (user_name, user_id))

        updated_rows = cur.rowcount
        print("updated rows count " + str(updated_rows))

        conn.commit()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn:
            conn.close()

    return updated_rows


def delete_user(user_id):
    conn = None
    row_deleted = 0

    try:
        params = config()
        conn = psycopg2.connect(**params)

        cur = conn.cursor()
        cur.execute("DELETE FROM users WHERE  user_id = %s", (user_id))
        row_deleted = cur.rowcount
        print("Row deleted " + str(row_deleted))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn:
            conn.close()

    return row_deleted


if __name__ == '__main__':
    # connect_to_rds()
    # create_tables()
    # insert_vendor_list([
    #     ('John Doe',),
    #     ('Douglas Smith',),
    #     ('Anthony Smith',),
    #     ('David Forrester',),
    #     ('Shawn Reddick',),
    #     ('Philip Broyles',)
    # ])

    # get_users()
    #
    # response = update_user(1, "John Doe Updated")
    # print(response)
    #
    get_users()

    delete_user('1')
    get_users()
