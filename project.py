
import psycopg2
from psycopg2 import sql
import psycopg2.extras

# Connection parameters
conn_params = {
    "dbname": "googleplaystore",
    "user": "postgres",
    "password": "Jung982511!",
    "host": "localhost",
    "port": "5432",
}

def insert_data(cur):
    table= input("Table name you want to insert the data in: ")
    columns = input("Column names, separate them by commas: ")
    values= input("Separate values by commas: ")

    # Splitting the columns and values and creating a list of placeholders
    columns = [col.strip() for col in columns.split(',')]
    values = tuple(val.strip() for val in values.split(','))

    placeholders = ', '.join(['%s'] * len(columns))  # The number of placeholders should match the number of columns

    query = sql.SQL("INSERT INTO {} ({}) VALUES ({});").format(
        sql.Identifier(table),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(placeholders)
    )

    try:
        cur.execute(query, values)
        print(f"Inserted successfully into {table}")
    except Exception as e:
        print(f"An error occurred: {e}")
        try:
            print(f"Failed query: {cur.mogrify(query, values)}")  # Shows the query that was attempted
        except Exception as ex:
            print(f"Error in mogrify: {ex}")
def delete_data(cur, conn):
    table = input("Table name you want to delete from: ")
    condition = input("Type in the condition where: ")

    query = sql.SQL("DELETE FROM {} WHERE {}").format(
        sql.Identifier(table),
        sql.SQL(condition)
    )

    try:
        cur.execute(query)
        conn.commit()
        print(f"Data deleted successfully from {table}.")
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()

def update_data(cur, conn):
    table = input("Table name you want to update the data from: ")
    set = input("Column names and their value names, make sure to separate them by commas ('column1=value1, column2=value2'): ")
    condition = input("Condition for the update, where ('id=1'): ")

    set_statements = [s.strip() for s in set.split(',')]
    set_pairs = [s.split('=') for s in set_statements]
    set_placeholders = [sql.SQL("{} = %s").format(sql.Identifier(pair[0])) for pair in set_pairs]
    values = [pair[1] for pair in set_pairs]

    query = sql.SQL("UPDATE {} SET {} WHERE {}").format(
        sql.Identifier(table),
        sql.SQL(', ').join(set_placeholders),
        sql.SQL(condition)
    )

    try:
        cur.execute(query, values)
        conn.commit()
        print(f"Table {table} updated successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
def search_data(cur, conn):
    # Ensure we start with a clean transaction state
    conn.rollback()

    table = input("Table name where you want to search the data from: ")
    condition_column = input("Column name for the condition, where: ")
    condition_value = input(f"Enter the value for {condition_column}: ")
    
    query = sql.SQL("SELECT * FROM {} WHERE {} = %s").format(
        sql.Identifier(table),
        sql.Identifier(condition_column)
    )

    try:
        cur.execute(query, (condition_value,))
        rows = cur.fetchall()
        if rows:
            print(f"Search results for {condition_column} = {condition_value}:")
            for row in rows:
                print(row)
        else:
            print("No data was found in this condition.")
    except Exception as e:
        print(f"An error occurred: {e}")

def aggregate_data(cur):
    table = input("Table name where you want to perform aggregation from: ")
    column = input("Colum name where you want to perform aggregation from: ")
    agg_type = input("Enter the aggregation type (SUM, AVG, COUNT, MIN, MAX): ").upper()

    if agg_type not in {'SUM', 'AVG', 'COUNT', 'MIN', 'MAX'}:
        print("Invalid aggregation type. Has to be one of SUM, AVG, COUNT, MIN, MAX.")
        return

    query = sql.SQL("SELECT {}({}) FROM {}").format(
        sql.SQL(agg_type),  # Note the change here
        sql.Identifier(column),
        sql.Identifier(table)
    )

    try:
        cur.execute(query)
        result = cur.fetchone()
        if result:
            print(f"The {agg_type} of {column} in {table} is: {result[0]}")
        else:
            print("No data found or unable to perform aggregation.")
    except Exception as e:
        print(f"An error occurred: {e}")

def sort_data(cur):
    table = input("Table name to sort the data from: ")
    column = input("Column name where you want to sort the data from: ")
    order = input("Enter the sort order (ASC or DESC): ").upper()

    if order not in {'ASC', 'DESC'}:
        print("Invalid sort order. Has to be ASC or DESC.")
        return

    query = sql.SQL("SELECT * FROM {} ORDER BY {} {}").format(
        sql.Identifier(table),
        sql.Identifier(column),
        sql.SQL(order)  # ORDER is a keyword and does not need to be an Identifier
    )

    try:
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"An error occurred: {e}")

def join_data(cur):
    # Get user input for table names
    table1 = input("First table for the join: ")
    table2 = input("Second table for the join: ")
    
    # Get user input for the join conditions
    join_condition = input("The join condition ('Table1.column1 = Table2.column2'): ")

    # Get user input for the type of join
    join_type = input("Which type of join? (INNER, LEFT OUTER, RIGHT OUTER, FULL OUTER): ").upper()
    if join_type not in {'INNER', 'LEFT OUTER', 'RIGHT OUTER', 'FULL OUTER'}:
        print("Invalid join type. Has to be INNER, LEFT OUTER, RIGHT OUTER, or FULL OUTER.")
        return

    # Prepare the SQL statement for joining tables
    query = sql.SQL("SELECT * FROM {} {} JOIN {} ON {}").format(
        sql.Identifier(table1),
        sql.SQL(join_type),
        sql.Identifier(table2),
        sql.SQL(join_condition)  # Assuming the user provides a valid SQL condition
    )

    try:
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"An error occurred: {e}")

def group_data(cur):
    table = input("Table name where you want to perform grouping: ")  
    grouping_column = input("Column name where you want to perform grouping: ")   
    aggregation = input("Which aggregate function? (COUNT, SUM, AVG, MAX, MIN): ").upper() 
    aggregate_column = input("Enter the column to aggregate: ")

    if aggregation not in {'COUNT', 'SUM', 'AVG', 'MAX', 'MIN'}:
        print("Not valid aggregation function. Must be one of COUNT, SUM, AVG, MAX, MIN.")
        return

    query = sql.SQL("SELECT {}, {}({}) FROM {} GROUP BY {}").format(
        sql.Identifier(grouping_column),
        sql.SQL(aggregation),
        sql.Identifier(aggregate_column),
        sql.Identifier(table),
        sql.Identifier(grouping_column)
    )

    try:
        cur.execute(query)
        result = cur.fetchall()
        for row in result:
            print(row)
    except Exception as e:
        print(f"An error occurred: {e}")


def subquery_data(cur):
    # Prompt the user for the outer query table and column
    outer_table = input("Table name for the main query: ")
    outer_column = input("Column name for the main query: ")
    
    # Prompt the user for the subquery details
    subquery_table = input("Table name for the subquery: ")
    subquery_column = input("Column name for the subquery: ")
    
    query = sql.SQL(
        "SELECT * FROM {} WHERE {} IN (SELECT {} FROM {})"
    ).format(
        sql.Identifier(outer_table),
        sql.Identifier(outer_column),
        sql.Identifier(subquery_column),
        sql.Identifier(subquery_table)
    )

    try:
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"An error occurred: {e}")

def transactions_data(cur):
    print("Starting a new transaction. Please enter your queries.")
    print("Type 'COMMIT' to commit the transaction or 'ROLLBACK' to rollback.")
    
    while True:
        query = input("Enter SQL query (or type 'COMMIT'/'ROLLBACK'): ")
        if query.upper() == 'COMMIT':
            try:
                cur.execute("COMMIT;")
                print("Transaction committed.")
                break
            except Exception as e:
                print(f"An error occurred during commit: {e}")
                continue
        elif query.upper() == 'ROLLBACK':
            try:
                cur.execute("ROLLBACK;")
                print("Transaction rolled back.")
                break
            except Exception as e:
                print(f"An error occurred during rollback: {e}")
                continue
        
        try:
            cur.execute(query)
            if cur.description:  # This checks if the query returns any results
                print(cur.fetchall())
            else:
                print("Executed successfully.")
        except Exception as e:
            print(f"An error occurred: {e}. Transaction has failed.")
            break


def main_menu():
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            while True:
                print("Welcome to the Database CLI Interface")
                print("1. Insert Data")
                print("2. Delete Data")
                print("3. Update Data")
                print("4. Search Data")
                print("5. Aggregate Data")
                print("6. Sort Data")
                print("7. Join Data")
                print("8. Group Data")
                print("9. Subquery Data")
                print("10. Transactions")
                print("11. Exit")
                choice = input("Enter your choice (1-11): ")

                if choice == "1":
                    insert_data(cur)
                    conn.commit()
                elif choice == "2":
                    delete_data(cur, conn)
                    conn.commit()
                elif choice == "3":
                     update_data(cur,conn)
                     conn.commit()
                elif choice == "4":
                    search_data(cur,conn)
                    conn.commit()
                elif choice == "5":
                    aggregate_data(cur)
                    conn.commit()
                elif choice == "6":
                    sort_data(cur)
                    conn.commit()
                elif choice == "7":
                    join_data(cur)
                    conn.commit()
                elif choice == "8":
                    group_data(cur)
                    conn.commit()
                elif choice == "9":
                    subquery_data(cur)
                    conn.commit()
                elif choice == "10":
                    transactions_data(cur)
                elif choice == "11":
                    print("Exiting the CLI.")
                    break
                else:
                    print("Choice is not valid. Try again.")
                # conn.commit()  # Moved inside the if statement

if __name__ == "__main__":
    main_menu()
