import os
import queries as q

if os.name == 'nt':
    import msvcrt as m
else:
    import getch as g


# Function for waiting for key press
def wait():
    if os.name == 'nt':
        m.getch()
    else:
        g.getch()


# define our clear function
def clear():
    # for windows
    if os.name == 'nt':
        os.system('cls')

        # for mac and linux(here, os.name is 'posix')
    else:
        os.system('clear')

    # Clear screen before to show menu, cls is MS Windows command


clear()

if __name__ == '__main__':
    ans = True
    while ans:
        print("""
        Menu:
        ------------
    
        1.Analyse a keyword 
        2.Analyse co-occurent terms
        3.Get tweets
        4.Get number of tweets per day
        5.Clear screen
        6.Exit/Quit
        """)
        ans = input("What would you like to do? Insert number: ")
        if ans == "1":
            keyword = input("Insert a keyword: ")
            q.query_analyse(keyword)
            wait()
            clear()

        elif ans == "2":
            keyword = input("Insert a keyword: ")
            year = input("Filter per year? (2019): ")
            month = input("Filter per month? (Jan/Feb): ")
            day = input("Filter per day? (02/03): ")
            hour = input("Filter per hour? (01): ")
            q.query_occurence(keyword, year, month, day, hour)

        elif ans == "3":
            keyword = input("Insert a keyword/phrase: ")
            year = input("Filter per year? (2019): ")
            month = input("Filter per month? (Jan/Feb): ")
            day = input("Filter per day? (02/03): ")
            hour = input("Filter per hour? (01): ")
            q.query_get_text(keyword, year, month, day, hour)
            print("\nPress Enter...")
            wait()
            clear()
        elif ans == "4":
            q.query_get_all_tweets_per_day()
            print("\nPress Enter...")
            wait()
            clear()
        elif ans == "5":
            clear()
        elif ans == "6":
            print("\nGoodbye")
            ans = None
            exit()
        else:
            print("\nNot Valid Choice Try again")
            print("\nPress Enter...")
            wait()
            clear()
            ans = True
