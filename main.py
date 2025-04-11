# Import the Surreal class
from surrealdb import Surreal
import pandas as pd
import time

with Surreal("ws://localhost:8000/rpc") as db:
    db.signin({"username": 'root', "password": 'root'})
    db.use("Databases2", "Graph")

    # get every entry in the table
    nodes_table = db.query("SELECT * FROM Nodes")
    print('----')
    print('Nodes table loaded')
    print('----')

    # optimizing shit
    nodes_dict = {item['node']: item for item in nodes_table}
    print('Nodes table converted to dictionary for faster access')
    print('----')

    # Load CSV
    csv_file = "cskg.tsv"

    df = pd.read_csv(
        csv_file,
        sep="\t",
        on_bad_lines='skip',
        dtype=str,
        low_memory=False
    )
    print('TSV loaded')
    print('----')


    start_time = time.time()
    for i, row in df.iterrows():
        surreal_node_1 = nodes_dict.get(row['node1'])
        surreal_node_2 = nodes_dict.get(row['node2'])

        if not surreal_node_1:
            print(f"Node 1 '{row['node1']}' not found in SurrealDB")

        if not surreal_node_2:
            print(f"Node 2 '{row['node2']}' not found in SurrealDB")

        # if i % 100000 == 0:
        #     print(f"{i / df.shape[0] * 100:.4f}%")

    end_time = time.time()

    print("----")
    print(f"Time taken to check nodes: {(end_time - start_time)/60:.2f} minutes")
    print("----")


    # Create array of data to insert to surreal
    # big_data = []

        # data = {
        #     "id": row["id"],
        #     "node1": row["node1"],
        #     "relation": row["relation"],
        #     "node2": row["node2"],
        #     "node1_label": row["node1;label"],
        #     "node2_label": row["node2;label"],
        #     "relation_label": row["relation;label"],
        #     "relation_dimension": row["relation;dimension"],
        #     "source": row["source"],
        #     "sentence": row["sentence"]
        # }

        # Append data to array
        # big_data.append(data)

    # print("done")
    # Insert data to surreal
    # db.insert('test1', big_data)