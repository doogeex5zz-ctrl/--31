class View:
    @staticmethod
    def show_menu():
        print("\n=== MAIN MENU ===")
        print("1. Show groom")
        print("2. Show organizers")
        print("3. Show orders")
        print("4. Add groom")
        print("5. Add organizer")
        print("6. Add order")
        print("7. Edit groom")
        print("8. Edit organizer")
        print("9. Edit order")
        print("10. Delete groom")
        print("11. Delete organizer")
        print("12. Delete order")
        print("13. Generate random data")
        print("14. Search: payment + groom name")
        print("15. Search: date + organizer social credit")
        print("16. Search: grooms by order details")
        print("0. Exit")

    @staticmethod
    def show_data(data):
        for row in data:
            print(row)

    @staticmethod
    def input_data(prompt):
        return input(prompt)
