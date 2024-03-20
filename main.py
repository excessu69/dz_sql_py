import psycopg2

# Функция для подключения к базе данных
def connect_db():
    return psycopg2.connect(database="dz_sql_py", user="postgres", password="Qgpd146itT78$")

# Функция для создания таблицы в базе данных
def create_table():
    with connect_db() as conn:
        with conn.cursor() as cur:
            # удаление таблиц
            cur.execute("""
                        DROP TABLE phone;
                        DROP TABLE clients;
                        """)
            cur.execute('''
                        CREATE TABLE IF NOT EXISTS clients
                        (id SERIAL PRIMARY KEY,
                        first_name VARCHAR(50),
                        last_name VARCHAR(50),
                        email VARCHAR(100))
                        ''')
            cur.execute('''
                        CREATE TABLE IF NOT EXISTS phone
                        (id SERIAL PRIMARY KEY,
                        phone VARCHAR(20),
                        client_id INTEGER NOT NULL REFERENCES clients(id))
                        ''')


# Функция для добавления нового клиента
def add_client(first_name, last_name, email, phone=None):
    with connect_db() as conn:
        with conn.cursor() as cur:
            # Вставляем данные клиента в таблицу clients
            cur.execute('''INSERT INTO clients (first_name, last_name, email)
                           VALUES (%s, %s, %s) RETURNING id''', (first_name, last_name, email))
            client_id = cur.fetchone()[0]  # Получаем идентификатор только что добавленного клиента

            # Если задан телефон, добавляем его в таблицу phone и связываем с клиентом
            if phone:
                cur.execute('''INSERT INTO phone (phone, client_id)
                               VALUES (%s, %s)''', (phone, client_id))


# Функция для добавления телефона для существующего клиента
def add_phone(client_id, phone):
    with connect_db() as conn:
        with conn.cursor() as cur:
            # Проверяем существует ли клиент с заданным client_id
            cur.execute("SELECT id FROM clients WHERE id = %s", (client_id,))
            if not cur.fetchone():
                print("Клиент с id={} не найден".format(client_id))
                return

            # Проверяем, есть ли уже такой номер телефона у клиента
            cur.execute("SELECT id FROM phone WHERE client_id = %s AND phone = %s", (client_id, phone))
            if cur.fetchone():
                print("Такой телефон уже существует у клиента с id={}".format(client_id))
                return

            # Добавляем телефон для существующего клиента
            cur.execute('''INSERT INTO phone (phone, client_id)
                           VALUES (%s, %s)''', (phone, client_id))


# Функция для изменения данных клиента
def update_client(client_id, first_name=None, last_name=None, email=None):
    with connect_db() as conn:
        with conn.cursor() as cur:
            update_query = "UPDATE clients SET "
            params = []
            if first_name:
                update_query += "first_name = %s, "
                params.append(first_name)
            if last_name:
                update_query += "last_name = %s, "
                params.append(last_name)
            if email:
                update_query += "email = %s, "
                params.append(email)
            update_query = update_query.rstrip(", ")
            update_query += " WHERE id = %s"
            params.append(client_id)
            cur.execute(update_query, tuple(params))

# Функция для удаления телефона для существующего клиента
def delete_phone(client_id, phone):
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute('''DELETE FROM phone
                           WHERE client_id = %s AND phone = %s''', (client_id, phone))

# Функция для удаления существующего клиента
def delete_client(client_id):
    with connect_db() as conn:
        with conn.cursor() as cur:
            # Сначала удаляем все телефонные номера клиента из таблицы phone
            cur.execute('''DELETE FROM phone
                           WHERE client_id = %s''', (client_id,))
            # Затем удаляем самого клиента из таблицы clients
            cur.execute('''DELETE FROM clients
                           WHERE id = %s''', (client_id,))


# Функция для поиска клиента по его данным
def find_client(first_name=None, last_name=None, email=None, phone=None):
    with connect_db() as conn:
        with conn.cursor() as cur:
            query = '''SELECT clients.*, phone.phone 
                       FROM clients LEFT JOIN phone 
                       ON clients.id = phone.client_id
                       WHERE (clients.first_name = %s OR %s IS NULL)
                       AND (clients.last_name = %s OR %s IS NULL)
                       AND (clients.email = %s OR %s IS NULL)
                       AND (phone.phone LIKE %s OR %s IS NULL)'''
            cur.execute(query, (first_name, first_name, last_name, last_name, email, email, phone, phone))
            clients = cur.fetchall()
    return clients

if __name__ == "__main__":
    # Создаем таблицу при запуске программы
    create_table()

    # Добавляем нового клиента
    add_client("Dmitriy", "Popov", "DmitrPop@email.ru", "9455567789")
    add_client("Lily", "Lis", "LiLu@mail.ru")

    # Добавляем телефон для существующего клиента
    add_phone(2, "9834445673")
    add_phone(2, "9834445673")
    add_phone(2, "9235556677")

    # Изменяем данные клиента
    update_client(1, first_name="Dima")
    update_client(2, last_name="Li")

    # Находим клиента по его данным
    found_clients = find_client(last_name="Li", phone="%444%")
    print("Найдены клиенты:")
    for client in found_clients:
        print(client)

    # Находим клиента по его данным
    found_clients = find_client(first_name="Dima")
    print("Найдены клиенты:")
    for client in found_clients:
        print(client)

    # Находим клиента по его данным
    found_clients = find_client(first_name="Lily")
    print("Найдены клиенты:")
    for client in found_clients:
        print(client)

    # Удаляем телефон для существующего клиента
    # delete_phone(2, "9834445673")

    # Удаляем существующего клиента
    # delete_client(2)




