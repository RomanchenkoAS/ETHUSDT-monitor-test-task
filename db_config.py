import psycopg2

# Local database requisites
HOST = "127.0.0.1"
USER = "postgres"
PASSWORD = "171997"
DB_NAME = "crypto"

# Manipulate manually with: $ psql -h 127.0.0.1 -U postgres crypto

def execute(query_list):
    """ This function takes a query list or a single query and executes them in SQL 
        If the query is SELECT there will be a return list 
        Launch this source code to execute query from terminal by hand """
    
    if type(query_list) == str:
        # Transform a single query to a list with length 1 for unification
        _temp = query_list
        query_list = []
        query_list.append(_temp)
    
    try:
        # Connect to the existing db
        connection = psycopg2.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DB_NAME
        )
        print("[DB INFO] PostgreSQL connection is open ----> ", end="")
        
        connection.autocommit = True
        
        results = []
        
        with connection.cursor() as cursor:
            for query in query_list:
                cursor.execute(query)
                # print(f"[DB INFO] Execution of query {query}")
            
            # Get results if a query is SELECT
            if query_list[0].startswith("SELECT"):
                results.extend(cursor.fetchall())
                    
        
    except Exception as _ex:
        print("\n[DB ERR] Error while working with database: ", _ex)
        print("[DB INFO] Connection is ----> ", end="")
    finally:
        if connection:
            connection.close()
            print("closed")
            
    return results
            

def main():
    # In case you would want to execute queries from terminal 
    query = input("[INPUT] Write a db query: ")
    rows = execute(query)
    
    for row in rows:
        print(row)
    
if __name__ == "__main__":
    main()
