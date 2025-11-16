from model import Database
from view import View
from datetime import datetime

class Controller:
    def __init__(self):
        self.db = Database()
        self.view = View()

    def input_int(self, prompt, min_value=None, max_value=None):
        while True:
            try:
                val = int(input(prompt))
                if min_value is not None and val < min_value:
                    print(f"Value must be >= {min_value}")
                    continue
                if max_value is not None and val > max_value:
                    print(f"Value must be <= {max_value}")
                    continue
                return val
            except ValueError:
                print("Invalid number format. Try again.")

    def input_date(self, prompt, format="%Y-%m-%d"):
        while True:
            s = input(prompt)
            try:
                dt = datetime.strptime(s, format).date()
                return dt
            except ValueError:
                print(f"Invalid date format. Use {format} (e.g. 2025-11-16).")

    def input_str_nonempty(self, prompt):
        while True:
            s = input(prompt).strip()
            if s == "":
                print("Field cannot be empty.")
                continue
            return s

    def run(self):
        while True:
            self.view.show_menu()
            choice = input("You choice:")

            if choice == "0":
                print("Exit...")
                self.db.close()
                break

            elif choice == "1":
                rows = self.db.get_grooms()
                self.view.show_data(rows)

            elif choice == "2":
                rows = self.db.get_organizers()
                self.view.show_data(rows)

            elif choice == "3":
                rows = self.db.get_orders()
                self.view.show_data(rows)

            elif choice == "4":
                name = self.input_str_nonempty("Name: ")
                age = self.input_int("Age: ", min_value=0)
                self.db.add_groom(name, age)

            elif choice == "5":
                name = self.input_str_nonempty("Name: ")
                sc = self.input_int("Social credit: ", min_value=0)
                self.db.add_organizer(name, sc)

            elif choice == "6":
                groom_id = self.input_int("groom_id: ", min_value=1)
                date = self.input_date("Date (YYYY-MM-DD): ")
                guests = self.input_int("Guests: ", min_value=0)
                payment = self.input_int("Payment: ", min_value=0)
                location = self.input_str_nonempty("Location: ")
                organizer_id = self.input_int("organizer_id: ", min_value=1)
                self.db.add_order(groom_id, date, guests, payment, location, organizer_id)

            elif choice == "7":
                gid = self.input_int("ID groom: ", min_value=1)
                name = self.input_str_nonempty("New name: ")
                age = self.input_int("New age: ", min_value=0)
                self.db.edit_groom(gid, name, age)

            elif choice == "8":
                oid = self.input_int("ID organizer: ", min_value=1)
                name = self.input_str_nonempty("New name: ")
                sc = self.input_int("New social credit: ", min_value=0)
                self.db.edit_organizer(oid, name, sc)

            elif choice == "9":
                oid = self.input_int("ID order: ", min_value=1)
                groom_id = self.input_int("New groom_id: ", min_value=1)
                date = self.input_date("New date (YYYY-MM-DD): ")
                guests = self.input_int("Guests: ", min_value=0)
                payment = self.input_int("Payment: ", min_value=0)
                location = self.input_str_nonempty("Location: ")
                organizer_id = self.input_int("organizer_id: ", min_value=1)
                self.db.edit_order(oid, groom_id, date, guests, payment, location, organizer_id)

            elif choice == "10":
                gid = self.input_int("ID groom: ", min_value=1)
                self.db.delete_groom(gid)

            elif choice == "11":
                oid = self.input_int("ID organizer: ", min_value=1)
                self.db.delete_organizer(oid)

            elif choice == "12":
                oid = self.input_int("ID order: ", min_value=1)
                self.db.delete_order(oid)

            elif choice == "13":
                count = self.input_int("How many rows to generate in each table?: ", min_value=1)
                self.db.generate_random_data(count)

            elif choice == "14":
                min_p = self.input_int("Min payment: ", min_value=0)
                max_p = self.input_int("Max payment: ", min_value=0)
                name_pat = input("Name groom: ")

                rows, dur = self.db.search_1_orders_by_payment_and_groom_name(min_p, max_p, name_pat)
                print(f"\n=== RESULTS FOR PAYMENT {min_p} .. {max_p} AND NAME ILIKE '{name_pat}' ===")
                self.view.show_data(rows)
                print(f"Query time: {dur:.2f} ms\n")

            elif choice == "15":
                start = self.input_date("Start date (YYYY-MM-DD): ")
                end = self.input_date("End date (YYYY-MM-DD): ")
                sc = self.input_int("Min social credit: ", min_value=0)

                rows, dur = self.db.search_2_orders_by_date_and_organizer_sc(start, end, sc)
                print(f"\n=== RESULTS FOR DATE {start} .. {end} AND ORGANIZER SOCIAL CREDIT >= {sc} ===")
                self.view.show_data(rows)
                print(f"Query time: {dur:.2f} ms\n")

            elif choice == "16":
                guests = self.input_int("Min guests: ", min_value=0)
                loc = input("Location pattern: ")

                rows, dur = self.db.search_3_grooms_by_order_details(guests, loc)
                print(f"\n=== RESULTS FOR GUESTS >= {guests} AND LOCATION ILIKE '{loc}' ===")
                self.view.show_data(rows)
                print(f"Query time: {dur:.2f} ms\n")

            else:
                print("Wrong choice! Try again")
