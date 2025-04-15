from surrealdb import Surreal
import time
from contextlib import redirect_stdout
from datetime import date, datetime

SOURCE_NODE_NAME = "/c/en/linux"


def print_node_info(input_node, show_full=False, indent=2):
    """
    Function to print node information.
    Args:
        input_node (dict): The node information to print.
        show_full (bool): Whether to show full information or not.
        indent (int): The indentation level for printing.
    Returns:
        None
    """
    if show_full:
        print(f"{indent * " "}ID: {input_node['id']}, Name: {input_node['node']},  Label: {input_node['label']}")
    else:
        print(f"{indent * " "}Node: {input_node['node']}")


def find_successors(source_node_id):
    """
    Function to find successors of a given node.
    Args:
        source_node_id (str): The ID of the source node from which to find successors.
    Returns:
        list: A list of successor nodes.
    """
    successors = db.query(f"SELECT * FROM {source_node_id}->edge->Nodes PARALLEL;")
    return successors


def find_predecessors(source_node_id):
    """
    Function to find predecessors of a given node.
    Args:
        source_node_id (str): The ID of the source node from which to find predecessors.
    Returns:
        list: A list of predecessor nodes.
    """
    predecessors = db.query(f"SELECT * FROM {source_node_id}<-edge<-Nodes PARALLEL;")
    return predecessors


def find_neighbours(source_node_id):
    """
    Function to find neighbours of a given node.
    Args:
        source_node_id (str): The ID of the source node from which to find neighbours.
    Returns:
        list: A list of neighbour nodes.
    """
    successors = find_successors(source_node_id)
    predecessors = find_predecessors(source_node_id)

    # Remove duplicates by converting to a dictionary and back to a list
    neighbours = list({str(item['id']): item for item in (successors + predecessors)}.values())
    return neighbours


def print_time(time_start, time_end):
    """
    Function to print elapsed time.
    Args:
        time_start (float): The start time.
        time_end (float): The end time.
    Returns:
        None
    """
    if time_end - time_start < 1:
        print(f"Time elapsed: {(time_end - time_start) * 1000:.2f} ms")
    elif time_end - time_start < 60:
        print(f"Time elapsed: {time_end - time_start:.2f} seconds")
    else:
        print(f"Time elapsed: {(time_end - time_start) / 60:.2f} minutes")


with Surreal("ws://localhost:8000/rpc") as db:
    db.signin({"username": 'root', "password": 'root'})
    db.use("Databases2", "Graph")

    source_node = db.query(f"SELECT * FROM Nodes WHERE node = '{SOURCE_NODE_NAME}' PARALLEL;")[0]
    if not source_node:
        raise Exception(f"ERROR: Node '{SOURCE_NODE_NAME}' not found in SurrealDB")

    with open(f"output_{datetime.now().strftime('%Y-%m-%d--%H-%M')}.txt", "w", 1) as file:
        with redirect_stdout(file):

            source_node_id = source_node['id']
            source_node_label = source_node['label']
            print(f"--- SOURCE NODE ---")

            print_node_info(source_node, show_full=True)

            ##############################
            ### Task 1: Find all successors of a given node.
            print(f"--- TASK 1 ---")
            time_start = time.time()

            successors = find_successors(source_node_id)

            print(f"Successors of {SOURCE_NODE_NAME}:")
            for successor in successors:
                print_node_info(successor)

            time_end = time.time()
            print_time(time_start, time_end)
            ###############################
            ### Task 2: Count all successors of a given node.
            print(f"--- TASK 2 ---")
            time_start = time.time()

            successor_count = len(successors)

            print(f"Total successors of {SOURCE_NODE_NAME}: {successor_count}")

            time_end = time.time()
            print_time(time_start, time_end)
            ###############################
            ### Task 3: Find all predecessors of a given node.
            print(f"--- TASK 3 ---")
            time_start = time.time()

            predecessors = find_predecessors(source_node_id)

            print(f"Predecessors of {SOURCE_NODE_NAME}:")
            for predecessor in predecessors:
                print_node_info(predecessor)

            time_end = time.time()
            print_time(time_start, time_end)
            ###############################
            ### Task 4: Count all predecessors of a given node.
            print(f"--- TASK 4 ---")
            time_start = time.time()

            predecessor_count = len(predecessors)

            print(f"Total predecessors of {SOURCE_NODE_NAME}: {predecessor_count}")

            time_end = time.time()
            print_time(time_start, time_end)
            ################################
            ### Task 5: Find all neighbors of a given node.
            print(f"--- TASK 5 ---")
            time_start = time.time()

            neighbours = find_neighbours(source_node_id)

            print(f"Neighbours of {SOURCE_NODE_NAME}:")
            for neighbour in neighbours:
                print_node_info(neighbour)

            time_end = time.time()
            print_time(time_start, time_end)
            #################################
            ### Task 6: Count all neighbours of a given node.
            print(f"--- TASK 6 ---")
            time_start = time.time()

            neighbour_count = len(neighbours)

            print(f"Total neighbours of {SOURCE_NODE_NAME}: {neighbour_count}")

            time_end = time.time()
            print_time(time_start, time_end)
            ##################################
            ### Task 7: Find all grandchildren (successors of successors) of a given node.
            print(f"--- TASK 7 ---")
            time_start = time.time()

            for successor in successors:
                grandchildren = find_successors(successor['id'])
                if grandchildren:
                    print(f"  Grandchildren of {successor['node']}:")
                    for grandchild in grandchildren:
                        print_node_info(grandchild, indent=4)

            time_end = time.time()
            print_time(time_start, time_end)
            ####################################
            ### Task 8: Find all grandparents (predecessors of predecessors) of a given node.
            print(f"--- TASK 8 ---")
            time_start = time.time()

            for predecessor in predecessors:
                grandparents = find_predecessors(predecessor['id'])
                if grandparents:
                    print(f"  Grandparents of {predecessor['node']}:")

                    for grandparent in grandparents:
                        print_node_info(grandparent, indent=4)

            time_end = time.time()
            print_time(time_start, time_end)
            ####################################
            ### Task 9: Count how many nodes there are.
            print(f"--- TASK 9 ---")
            time_start = time.time()

            node_count = db.query("RETURN count(SELECT * FROM Nodes PARALLEL);")
            # BARTOSZ - IS THERE FASTER WAY TO DO IT?
            # There's 'INFO FOR TABLE Nodes' query but it doesn't return the count of nodes in the table
            # maybe we could add some metadate there perhaps?

            print(f"Total nodes in the database: {node_count}")

            time_end = time.time()
            print_time(time_start, time_end)

            ####################################
            ### Task 10: Count nodes which do not have any successors.
            print(f"--- TASK 10 ---")

            # test 1
            # time: 186 minutes
            # result: 649,184
            time_start = time.time()

            no_successor_count = db.query("RETURN count(SELECT * FROM Nodes WHERE count(->edge->Nodes) = 0 PARALLEL);")

            print(f"Total nodes without successors: {no_successor_count}")

            time_end = time.time()
            print_time(time_start, time_end)

            """
            # test 2
            # time: 403 minutes
            # result: 649,184
            time_start = time.time()

            all_nodes = db.query("SELECT * FROM Nodes PARALLEL;")

            no_successor_count = 0
            for i, node in enumerate(all_nodes):
                if not find_successors(node['id']):
                    no_successor_count += 1

                # if i % 10000 == 0 and i != 0:
                    # print(f"Progress: {(i / len(all_nodes)) * 100:.2f}%")

            print(f"Total nodes without successors (2): {no_successor_count}")

            time_end = time.time()
            print_time(time_start, time_end)

            # what if we insert a count predecessors and successors fields into Nodes?
            """
            ########################################
            ### Task 11: Count nodes which do not have any predecessors.
            print(f"--- TASK 11 ---")
            time_start = time.time()

            no_predecessor_count = db.query("RETURN count(SELECT * FROM Nodes WHERE count(<-edge<-Nodes) = 0 PARALLEL);")

            print(f"Total nodes without predecessors: {no_predecessor_count}")

            time_end = time.time()
            print_time(time_start, time_end)

            ########################################
            ### Task 12: Find nodes with the most neighbours.
            print(f"--- TASK 12 ---")
            time_start = time.time()

            max_neighbours = 0
            max_neighbours_nodes = list()
            all_nodes = db.query("SELECT * FROM Nodes PARALLEL;")
            for i, node in enumerate(all_nodes):
                neighbours = find_neighbours(node['id'])
                if len(neighbours) > max_neighbours:
                    max_neighbours = len(neighbours)
                    max_neighbours_nodes = list()
                    max_neighbours_nodes.append(node['node'])
                elif len(neighbours) == max_neighbours:
                    max_neighbours_nodes.append(node['node'])

            print(f"Nodes with the most neighbours ({max_neighbours}):")
            for node in max_neighbours_nodes:
                print_node_info(node)

            time_end = time.time()
            print_time(time_start, time_end)

            ########################################
            ### Task 13: Count nodes with a single neighbour.
            print(f"--- TASK 13 ---")
            time_start = time.time()

            single_neighbour_count = 0
            all_nodes = db.query("SELECT * FROM Nodes PARALLEL;")
            for i, node in enumerate(all_nodes):
                neighbours = find_neighbours(node['id'])
                if len(neighbours) == 1:
                    single_neighbour_count += 1

            print(f"Total nodes with a single neighbour: {single_neighbour_count}")

            time_end = time.time()
            print_time(time_start, time_end)

