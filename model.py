import psycopg2
import time
import logging
import traceback
from psycopg2 import errors
from config import DB_CONFIG

# Налаштування логування помилок (файл)
logging.basicConfig(
    filename='db_errors.log',
    level=logging.ERROR,
    format='%(asctime)s %(levelname)s %(message)s'
)

class Database:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            # використовуємо звичайний cursor, можна змінити на RealDictCursor за потреби
            self.cur = self.conn.cursor()
            print("Connected to PostgreSQL successfully!")
        except Exception:
            logging.error("Error connecting to PostgreSQL:\n" + traceback.format_exc())
            print("Error: Failed to connect to database (details in db_errors.log).")            
            self.conn = None
            self.cur = None

    def execute(self, query, params=None, fetch=False, report_time=False):

        if self.conn is None or self.cur is None:
            print("Error: Database connection is not established.")
            return ([] if fetch else None) if not report_time else (([] if fetch else None), 0.0)

        start_time = time.monotonic() if report_time else None
        try:
            self.cur.execute(query, params or ())
            if fetch:
                rows = self.cur.fetchall()
            else:
                self.conn.commit()
                rows = None

            duration_ms = (time.monotonic() - start_time) * 1000 if report_time else None

            if report_time:
                # повертаємо результат та час
                return (rows, duration_ms) if fetch else (None, duration_ms)

            return rows

        except errors.ForeignKeyViolation:
            logging.error("ForeignKeyViolation:\n" + traceback.format_exc())
            print("Error: connection broken (no associated record). Operation failed.")
            self.conn.rollback()
        except errors.UniqueViolation:
            logging.error("UniqueViolation:\n" + traceback.format_exc())
            print("Error: Uniqueness violation (duplicate key).")
            self.conn.rollback()
        except Exception:
            logging.error("SQL execution error:\n" + traceback.format_exc())
            print("SQL execution error: operation not performed (details in db_errors.log).")
            self.conn.rollback()

        # якщо сталася помилка — повернути пустий результат (або None)
        return ([] if fetch else None) if not report_time else (([] if fetch else None), None)


    # ---------------- GROOM ----------------
    def add_groom(self, name, age):
        query = 'INSERT INTO public.groom (name, age) VALUES (%s, %s) RETURNING groom_id'
        result = self.execute(query, (name, age), fetch=True)
        if result:
            new_id = result[0][0]
            print(f"Added groom id={new_id}")
            return new_id
        return None

    def edit_groom(self, groom_id, name, age):
        self.execute(
            'UPDATE public.groom SET name=%s, age=%s WHERE groom_id=%s',
            (name, age, groom_id)
        )

    def delete_groom(self, groom_id):
        # Перевіряємо наявність записів у дочірній таблиці
        count = self.execute(
            'SELECT COUNT(*) FROM public."order" WHERE groom_id = %s',
            (groom_id,),
            fetch=True
        )
        if count and count[0][0] > 0:
            print(f"Error: Unable to delete groom (ID: {groom_id}). He has {count[0][0]} related orders.")
            return

        self.execute('DELETE FROM public.groom WHERE groom_id=%s', (groom_id,))

    def get_grooms(self):
        return self.execute('SELECT * FROM public.groom ORDER BY groom_id', fetch=True)

    # ---------------- ORGANIZER ----------------
    def add_organizer(self, name, sc):
        query = 'INSERT INTO public.organizer (name, social_credit) VALUES (%s, %s) RETURNING organizer_id'
        result = self.execute(query, (name, sc), fetch=True)
        if result:
            new_id = result[0][0]
            print(f"Added organizer id={new_id}")
            return new_id
        return None

    def edit_organizer(self, oid, name, sc):
        self.execute('UPDATE public.organizer SET name=%s, social_credit=%s WHERE organizer_id=%s', (name, sc, oid))

    def delete_organizer(self, oid):
        count = self.execute('SELECT COUNT(*) FROM public."order" WHERE organizer_id = %s', (oid,), fetch=True)
        if count and count[0][0] > 0:
            print(f"Error: Unable to delete organizer (ID: {oid}). It has {count[0][0]} related orders.")
            return
        self.execute('DELETE FROM public.organizer WHERE organizer_id=%s', (oid,))

    def get_organizers(self):
        return self.execute('SELECT * FROM public.organizer ORDER BY organizer_id', fetch=True)

    # ---------------- ORDER ----------------
    def add_order(self, groom_id, date, guests, payment, location, organizer_id):
        query = '''
        INSERT INTO public."order" (groom_id, wedding_date, guests, payment, location, organizer_id)
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        '''
        res = self.execute(query, (groom_id, date, guests, payment, location, organizer_id), fetch=True)
        if res:
            new_id = res[0][0]
            print(f"Added order id={new_id}")
            return new_id
        return None

    def edit_order(self, oid, groom_id, date, guests, payment, location, organizer_id):
        self.execute(
            '''UPDATE public."order"
               SET groom_id=%s, wedding_date=%s, guests=%s, payment=%s,
                   location=%s, organizer_id=%s
               WHERE id=%s''',
            (groom_id, date, guests, payment, location, organizer_id, oid)
        )

    def delete_order(self, oid):
        self.execute('DELETE FROM public."order" WHERE id=%s', (oid,))

    def get_orders(self):
        return self.execute('SELECT * FROM public."order" ORDER BY id', fetch=True)

    # ---------------- ГЕНЕРАЦІЯ ДАНИХ ----------------

    def generate_random_data(self, count):
        print("Clearing tables...")
        self.execute('TRUNCATE public."order", public.organizer, public.groom RESTART IDENTITY CASCADE')

        print(f"Generating {count} rows...")

        groom_names = ['Michael', 'John', 'Robert', 'David', 'William']
        organizer_names = ['Golden Event', 'LoveStory Agency', 'WeddingPro', 'Romance Studio']
        locations = ['New York Central Park', 'Los Angeles Beach', 'Paris City Hall', 'Tokyo Garden']

        # ---- groom ----
        groom_query = """
            INSERT INTO public.groom (name, age)
            SELECT 
                (ARRAY[%s, %s, %s, %s, %s])[ceil(random() * 5)::int],
                (22 + floor(random() * 29))::int
            FROM generate_series(1, %s);
        """

        self.execute(groom_query, (*groom_names, count))

        # ---- organizer ----
        organizer_query = """
            INSERT INTO public.organizer (name, social_credit)
            SELECT 
                (ARRAY[%s, %s, %s, %s])[ceil(random() * 4)::int],
                (60 + floor(random() * 40))::int
            FROM generate_series(1, %s);
        """

        self.execute(organizer_query, (*organizer_names, count))

        # ---- orders ----
        order_query = """
            INSERT INTO public."order"
                (groom_id, wedding_date, guests, payment, location, organizer_id)
            SELECT
                g.groom_id,
                date '2025-01-01' + ((random() * 364)::int) * interval '1 day',
                (20 + floor(random() * 131))::int,
                (50000 + floor(random() * 150001))::int,
                (ARRAY[%s, %s, %s, %s])[ceil(random() * 4)::int],
                o.organizer_id
            FROM
                generate_series(1, %s) AS s(i)
            JOIN
                (SELECT groom_id, ROW_NUMBER() OVER (ORDER BY random()) as rn FROM public.groom) AS g ON g.rn = s.i
            JOIN
                (SELECT organizer_id, ROW_NUMBER() OVER (ORDER BY random()) as rn FROM public.organizer) AS o ON o.rn = s.i;
        """

        self.execute(order_query, (*locations, count))

        # ---- Sync sequences ----
        self.sync_sequences('public.groom', 'groom_id')
        self.sync_sequences('public.organizer', 'organizer_id')
        self.sync_sequences('public."order"', 'id')

        print("Data generation completed successfully.")


    def sync_sequences(self, table_name, pk_column):
        try:
            seq_sql = "SELECT pg_get_serial_sequence(%s, %s)"
            seq_name = self.execute(seq_sql, (table_name, pk_column), fetch=True)
            if seq_name and seq_name[0][0]:
                seq = seq_name[0][0]
                setval_sql = f"SELECT setval(%s, COALESCE((SELECT MAX({pk_column}) FROM {table_name}), 0) + 1, false)"
                # Викликаємо setval
                self.execute(setval_sql, (seq,))
                print(f"Sequence for {table_name}.{pk_column} synced to MAX+1.")
            else:
                # Якщо не знайшли sequence нічого не робимо
                print(f"No serial sequence detected for {table_name}.{pk_column}. Manual check recommended.")
        except Exception:
            logging.error("Error syncing sequence:\n" + traceback.format_exc())
            print("Warning: failed to automatically synchronize sequence (details in db_errors.log).")

    # ---------------- ПОШУКОВІ ЗАПИТИ ----------------
    def search_1_orders_by_payment_and_groom_name(self, min_payment, max_payment, name_pattern):
        query = """
        SELECT o.id, o.wedding_date, o.payment, g.name AS groom_name, g.age
        FROM public."order" o
        JOIN public.groom g ON o.groom_id = g.groom_id
        WHERE o.payment BETWEEN %s AND %s
          AND g.name ILIKE %s
        ORDER BY o.payment DESC;
        """
        return self.execute(query, (min_payment, max_payment, name_pattern), fetch=True, report_time=True)

    def search_2_orders_by_date_and_organizer_sc(self, start_date, end_date, min_sc):
        query = """
        SELECT o.id, o.wedding_date, o.guests, org.name AS organizer_name, org.social_credit
        FROM public."order" o
        JOIN public.organizer org ON o.organizer_id = org.organizer_id
        WHERE o.wedding_date BETWEEN %s AND %s
          AND org.social_credit >= %s
        ORDER BY o.wedding_date;
        """
        return self.execute(query, (start_date, end_date, min_sc), fetch=True, report_time=True)

    def search_3_grooms_by_order_details(self, min_guests, location_pattern):
        query = """
        SELECT 
            g.name, 
            g.age, 
            COUNT(o.id) AS total_orders, 
            SUM(o.payment) AS total_payment
        FROM public.groom g
        JOIN public."order" o ON g.groom_id = o.groom_id
        WHERE o.guests >= %s
          AND o.location ILIKE %s
        GROUP BY g.groom_id, g.name, g.age
        ORDER BY total_payment DESC;
        """
        return self.execute(query, (min_guests, location_pattern), fetch=True, report_time=True)

    # ---------------- CLOSE ----------------
    def close(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
            print("Database connection closed.")
