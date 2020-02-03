import os
import msvcrt as m
#import getch as g
import queries as q


# Function for waiting for key press
def wait():
    m.getch()


# Clear screen before to show menu, cls is MS Windows command
os.system('cls')

if __name__ == '__main__':
    ans = True
    while ans:
        print("""
        Menu:
        ------------
    
        1.Analyse a keyword 
        2.TODO
        3.TODO
        4.Exit/Quit
        """)
        ans = input("What would you like to do? Insert number: ")
        if ans == "1":
            keyword = input("Insert a keyword: ")
            q.start_query("filter", keyword)
            wait()
            os.system('cls')
            continue
        elif ans == "2":
            print("\nStudent Deleted")
            print("\nPress Enter...")
            wait()
            os.system('cls')
        elif ans == "3":
            print("\nStudent Record Found")
            print("\nPress Enter...")
            wait()
            os.system('cls')
        elif ans == "4":
            print("\nGoodbye")
            ans = None
            exit()
        else:
            print("\nNot Valid Choice Try again")
            print("\nPress Enter...")
            wait()
            os.system('cls')
            ans = True
