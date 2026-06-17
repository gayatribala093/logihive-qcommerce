import networkx as nx
from concurrent.futures import ThreadPoolExecutor

# Global executor
executor = ThreadPoolExecutor(max_workers=4)

# Supply chain graph
supply_chain = nx.DiGraph()

# Hierarchical nodes
nodes = [
    "FMCG_ROOT",
    "Mumbai_Mother_Warehouse",
    "Warehouse_Andheri",
    "Warehouse_Borivali",
    "DarkStore_Malad",
    "DarkStore_Kandivali",
    "DarkStore_Virar",
    "DarkStore_Vasai"
]

supply_chain.add_nodes_from(nodes)

# Weighted edges
edges = [
    ("FMCG_ROOT", "Mumbai_Mother_Warehouse", 5),

    ("Mumbai_Mother_Warehouse", "Warehouse_Andheri", 2),
    ("Mumbai_Mother_Warehouse", "Warehouse_Borivali", 3),

    ("Warehouse_Andheri", "DarkStore_Malad", 1),
    ("Warehouse_Andheri", "DarkStore_Kandivali", 2),

    ("Warehouse_Borivali", "DarkStore_Virar", 2),
    ("Warehouse_Borivali", "DarkStore_Vasai", 1)
]

for source, destination, weight in edges:
    supply_chain.add_edge(
        source,
        destination,
        weight=weight
    )


def find_shortest_route(source, destination):
    return nx.dijkstra_path(
        supply_chain,
        source,
        destination,
        weight="weight"
    )


def get_route_async(source, destination):
    future = executor.submit(
        find_shortest_route,
        source,
        destination
    )

    return future.result()


if __name__ == "__main__":

    route = get_route_async(
        "FMCG_ROOT",
        "DarkStore_Virar"
    )

    print("Shortest Route:")
    print(route)