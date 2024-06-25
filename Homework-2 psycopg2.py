import psycopg2
from psycopg2.sql import SQL, Identifier

def create_db(conn):
    with conn.cursor() as cur:
        # cur.execute("""
        # DROP TABLE client_info;
        # DROP TABLE phonebook;
        # """)
        cur.execute(""" 
        CREATE TABLE IF NOT EXISTS client_info(
            client_id SERIAL PRIMARY KEY,
            first_name VARCHAR(20),
            last_name VARCHAR(20),
            email VARCHAR(40)
        );    
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS phonebook(
            id SERIAL PRIMARY KEY,
            phone VARCHAR(12) UNIQUE,
            client_id INTEGER REFERENCES client_info(client_id)
        );            
        """)
        conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO client_info (first_name, last_name, email)
        VALUES(%s,%s,%s)
        RETURNING client_id, first_name, last_name, email;
        ''', (first_name, last_name, email))
        return cur.fetchone()


def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute('''
        INSERT INTO phonebook(client_id, phone)
        VALUES(%s,%s)
        RETURNING client_id, phone;
        ''', (client_id, phone))
        return cur.fetchone()


def change_client(conn, client_id, first_name=None, last_name=None, email=None):
    with conn.cursor() as cur:
        arg_list = {'first_name': first_name, 'last_name': last_name, 'email': email}
        for key, arg in arg_list.items():
            if arg:
                cur.execute(SQL('UPDATE client_info SET {}=%s WHERE client_id = %s').format(Identifier(key)), (arg,client_id))
        cur.execute('''
            SELECT * FROM client_info
            WHERE client_id = %s;
            ''', client_id)
        return cur.fetchall()


def change_phone(conn, client_id, phone=None):
    with conn.cursor() as cur:
        cur.execute('''
        UPDATE phonebook
        SET phone=%s
        WHERE client_id=%s
        RETURNING client_id, phone;
        ''', (phone, client_id))
        return cur.fetchall()


def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute('''
        DELETE FROM phonebook
        WHERE client_id=%s
        RETURNING client_id;
        ''', (client_id,))
        return cur.fetchone()


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute('''
        DELETE FROM client_info
        WHERE client_id = %s
        RETURNING client_id;
        ''', (client_id,))
        return cur.fetchone()


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        cur.execute('''
        SELECT * FROM client_info c
        LEFT JOIN phonebook p ON c.client_id = p.client_id
        WHERE (first_name = %(first_name)s OR %(first_name)s IS NULL)
        AND (last_name = %(last_name)s OR %(last_name)s IS NULL)
        AND (email = %(email)s OR %(email)s IS NULL)
        AND (phone = %(phone)s OR %(phone)s IS NULL);
        ''', {'first_name': first_name, 'last_name': last_name, 'email': email, 'phone': phone})
        return cur.fetchone()


if __name__ == '__main__':
    with psycopg2.connect(database='postgres', user='postgres', password='Pderfhm86') as conn:

        # create_db(conn)
        # print(add_client(conn, 'Azat', 'Khasanov', 'azatkh@mail.ru'))
        # print(add_phone(conn, 11, 89123456789))
        # print(change_client(conn, '1', last_name='Jackson'))
        # print(change_phone(conn, '1', '89123456790'))
        # print(delete_phone(conn, '1', '89123456790'))
        # print(delete_client(conn, '1'))
        # print(find_client(conn, last_name='Khasanov'))
        # with conn.cursor() as cur:
        #     cur.execute("""
        #     SELECT * FROM client_info;
        #     """)
        #     print(cur.fetchall())