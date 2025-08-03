import networkx as nx
from .graph_model import Node, Edge
from .core_interfaces import ProcessorInterface, ProcessorMeta
from typing import List, Dict, Any, Generator, Tuple

def build_nx_graph(nodes: List[Node], edges: List[Edge]) -> nx.DiGraph:
    """Build a networkx graph from the graph definition"""
    G = nx.DiGraph()
    
    # Add nodes using their unique name
    for node in nodes:
        G.add_node(node.processor_unique_name, data=node)

    # Add edges using their unique name
    for edge in edges:
        G.add_edge(edge.source_node_unique_name, edge.target_node_unique_name, key=edge.edge_unique_name, data=edge)

    return G


def get_previous_nodes(nx_graph: nx.DiGraph, node_unique_name: str) -> List[str]:
    """Get all nodes that come before the given node in the graph"""
    if node_unique_name not in nx_graph:
        raise ValueError(f"Node {node_unique_name} not found in graph")
    
    predecessors = list(nx_graph.predecessors(node_unique_name))
    predecessors_nodes = [nx_graph.nodes[node]['data'] for node in predecessors]
    return predecessors_nodes

def get_next_nodes(nx_graph: nx.DiGraph, node_unique_name: str) -> List[str]:
    """Get all nodes that come after the given node in the graph"""
    if node_unique_name not in nx_graph:
        raise ValueError(f"Node {node_unique_name} not found in graph")
    
    successors = list(nx_graph.successors(node_unique_name))
    successors_nodes = [nx_graph.nodes[node]['data'] for node in successors]
    return successors_nodes

def get_root_nodes(nx_graph: nx.DiGraph) -> List[str]:
    """Get all nodes that have no incoming edges (root nodes)"""
    root_nodes = [node for node in nx_graph.nodes() if nx_graph.in_degree(node) == 0]
    root_nodes_nodes = [nx_graph.nodes[node]['data'] for node in root_nodes]
    return root_nodes_nodes

def get_leaf_nodes(nx_graph: nx.DiGraph) -> List[str]:
    """Get all nodes that have no outgoing edges (leaf nodes)"""
    leaf_nodes = [node for node in nx_graph.nodes() if nx_graph.out_degree(node) == 0]
    leaf_nodes_nodes = [nx_graph.nodes[node]['data'] for node in leaf_nodes]
    return leaf_nodes_nodes

def get_node_paths(nx_graph: nx.DiGraph, source_node: str, target_node: str) -> List[List[str]]:
    """Get all possible paths from source to target node"""
    try:
        paths = list(nx.all_simple_paths(nx_graph, source_node, target_node))
        return paths
    except nx.NetworkXNoPath:
        return []

def is_acyclic(nx_graph: nx.DiGraph) -> bool:
    """Check if the graph is acyclic (no cycles)"""
    return nx.is_directed_acyclic_graph(nx_graph)

def get_topological_sort(nx_graph: nx.DiGraph) -> List[str]:
    """Get topological sort of the graph nodes"""
    if not is_acyclic(nx_graph):
        raise ValueError("Graph contains cycles, cannot perform topological sort")
    return list(nx.topological_sort(nx_graph))

def get_node_depth(nx_graph: nx.DiGraph, node_unique_name: str) -> int:
    """Get the depth of a node (number of edges from root)"""
    if node_unique_name not in nx_graph:
        raise ValueError(f"Node {node_unique_name} not found in graph")
    
    # Find the longest path from any root to this node
    root_nodes = get_root_nodes(nx_graph)
    if not root_nodes:
        return 0
    
    max_depth = 0
    for root in root_nodes:
        try:
            path_length = len(nx.shortest_path(nx_graph, root, node_unique_name)) - 1
            max_depth = max(max_depth, path_length)
        except nx.NetworkXNoPath:
            continue
    
    return max_depth

def get_nodes_at_depth(nx_graph: nx.DiGraph, depth: int) -> List[str]:
    """Get all nodes at a specific depth"""
    nodes_at_depth = []
    for node in nx_graph.nodes():
        if get_node_depth(nx_graph, node) == depth:
            nodes_at_depth.append(node)
    return nodes_at_depth

def get_subgraph_from_node(nx_graph: nx.DiGraph, node_unique_name: str) -> Tuple[List[Node], List[Edge]]:
    """Get a subgraph starting from the given node (including all reachable nodes)"""
    if node_unique_name not in nx_graph:
        raise ValueError(f"Node {node_unique_name} not found in graph")
    
    # Get all nodes reachable from the given node
    reachable_nodes = nx.descendants(nx_graph, node_unique_name)
    reachable_nodes.add(node_unique_name)
    
    # Create subgraph
    sub_nx_graph = nx_graph.subgraph(reachable_nodes)
    
    # Convert back to our graph format
    sub_nodes = []
    sub_edges = []
    
    for node_name in reachable_nodes:
        node_data = nx_graph.nodes[node_name]['data']
        sub_nodes.append(node_data)
    
    for source, target, key in sub_nx_graph.edges(keys=True):
        edge_data = nx_graph.edges[source, target, key]['data']
        sub_edges.append(edge_data)
    
    return sub_nodes, sub_edges

def get_execution_levels(nx_graph: nx.DiGraph) -> Dict[int, List[str]]:
    """Get all nodes organized by their execution level (depth)"""
    levels = {}
    for node in nx_graph.nodes():
        depth = get_node_depth(nx_graph, node)
        if depth not in levels:
            levels[depth] = []
        levels[depth].append(node)
    return levels

def get_parallel_execution_opportunities(nx_graph: nx.DiGraph) -> Dict[int, List[str]]:
    """Get nodes that can be executed in parallel at each level"""
    levels = get_execution_levels(nx_graph)
    parallel_opportunities = {}
    
    for depth, nodes in levels.items():
        if len(nodes) > 1:
            parallel_opportunities[depth] = nodes
    
    return parallel_opportunities

def validate_graph_connectivity(nx_graph: nx.DiGraph) -> bool:
    """Check if the graph is fully connected (all nodes are reachable from root)"""
    root_nodes = get_root_nodes(nx_graph)
    if not root_nodes:
        return False
    
    # Check if all nodes are reachable from at least one root
    all_nodes = set(nx_graph.nodes())
    reachable_nodes = set()
    
    for root in root_nodes:
        reachable_nodes.update(nx.descendants(nx_graph, root))
        reachable_nodes.add(root)
    
    return reachable_nodes == all_nodes


def traverse_graph_generator_dfs(nx_graph: nx.DiGraph, node_name: str) -> Generator[str, None, None]:
    """Traverse the graph from the given node name using depth-first search"""
    visited = set()
    stack = [node_name]
    
    while stack:
        current_node = stack.pop()
        if current_node not in visited:
            visited.add(current_node)
            yield current_node
            
            # Get next nodes and add them to stack in reverse order
            # (to maintain DFS left-to-right traversal)
            next_nodes_names = get_next_nodes_names(nx_graph, current_node)
            for next_node_name in reversed(next_nodes_names):
                if next_node_name not in visited:
                    stack.append(next_node_name)


def get_graph_statistics(nx_graph: nx.DiGraph) -> Dict[str, Any]:
    """Get comprehensive statistics about the graph"""
    return {
        'total_nodes': nx_graph.number_of_nodes(),
        'total_edges': nx_graph.number_of_edges(),
        'is_acyclic': is_acyclic(nx_graph),
        'root_nodes': get_root_nodes(nx_graph),
        'leaf_nodes': get_leaf_nodes(nx_graph),
        'max_depth': max(get_node_depth(nx_graph, node) for node in nx_graph.nodes()) if nx_graph.nodes() else 0,
        'is_connected': validate_graph_connectivity(nx_graph),
        'execution_levels': get_execution_levels(nx_graph),
        'parallel_opportunities': get_parallel_execution_opportunities(nx_graph)
    }


# python -m processor_pipeline.new_core.graph_utils
if __name__ == "__main__":
    # test the traverse_graph_generator_dfs
    from .test_graph import ChunkerProcessor, HasherProcessor
    nx_graph = build_nx_graph(nodes=[
        Node(processor_class_name="ChunkerProcessor", processor_unique_name="A"),
        Node(processor_class_name="HasherProcessor", processor_unique_name="AA"),
        Node(processor_class_name="ChunkerProcessor", processor_unique_name="AB"),
        Node(processor_class_name="HasherProcessor", processor_unique_name="AAA"),
        Node(processor_class_name="HasherProcessor", processor_unique_name="AAB"),
    ], edges=[
        Edge(source_node_unique_name="A", target_node_unique_name="AA", edge_unique_name="A--AA"),
        Edge(source_node_unique_name="A", target_node_unique_name="AB", edge_unique_name="A--AB"),
        Edge(source_node_unique_name="AA", target_node_unique_name="AAA", edge_unique_name="AA--AAA"),
        Edge(source_node_unique_name="AA", target_node_unique_name="AAB", edge_unique_name="AA--AAB"),
    ])


    for node_name in traverse_graph_generator_dfs(nx_graph, "A"):
        print(node_name)