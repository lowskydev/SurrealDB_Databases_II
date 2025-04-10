# Import the Surreal class
from surrealdb import Surreal
import pandas as pd
import time

with Surreal("ws://localhost:8000/rpc") as db:
    db.signin({"username": 'root', "password": 'root'})
    db.use("Databases2", "Graph")

    # get every entry in the table
    time_start = time.time()
    nodes_table = db.query("SELECT * FROM Nodes")
    time_end = time.time()
    print('----')
    print(f'Nodes table loaded ({(time_end - time_start):.2f} seconds)')
    print('----')

    # optimizing shit
    time_start = time.time()
    nodes_dict = {item['node']: item for item in nodes_table}
    time_end = time.time()
    print(f'Nodes table converted to dictionary for faster access ({(time_end - time_start):.2f} seconds)')
    print('----')

    # Load CSV
    csv_file = "cskg.tsv"

    time_start = time.time()
    df = pd.read_csv(
        csv_file,
        sep="\t",
        on_bad_lines='skip',
        dtype=str,
        low_memory=False
    )
    time_end = time.time()
    print(f'TSV loaded ({(time_end - time_start):.2f} seconds)')
    print('----')


    start_time = time.time()
    queries = []
    BATCH_SIZE = 1000
    for i, row in df.iterrows():
        surreal_node_1 = nodes_dict.get(row['node1'])
        surreal_node_2 = nodes_dict.get(row['node2'])

        if not surreal_node_1:
            raise Exception(f"ERROR: Node 1 '{row['node1']}' not found in SurrealDB")

        if not surreal_node_2:
            raise Exception(f"ERROR: Node 2 '{row['node2']}' not found in SurrealDB")

        queries = []
        queries.append(f"RELATE {surreal_node_1['id']}->edge->{surreal_node_2['id']} SET relation = '{row["relation"]}';")

        db.query(" ".join(queries))

        if i % BATCH_SIZE == 0 and i != 0:
            db.query(" ".join(queries))
            queries = []


        if i % 10000 == 0:
            print(f"{i / df.shape[0] * 100:.2f}%")

    end_time = time.time()

    print("----")
    print(f"Made realtions between nodes ({(end_time - start_time)/60:.2f} minutes)")
    print("----")
