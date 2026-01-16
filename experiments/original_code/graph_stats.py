import graph_tool.all as gt
import numpy as np

import os
import sys

BASE = 'enc_edgelist.csv'



def save_list(lst, filename):
    with open(filename, 'w') as f:
        for item in lst:
            f.write("%s\n" % item)
    
if __name__ == "__main__":

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]

    
    if not os.path.exists("graph_stats"):
        os.makedirs("graph_stats")
    

    graph = gt.load_graph_from_csv(BASE, directed=True, strip_whitespace=True)
    BASE = os.path.basename(BASE)
    print("Graph loaded")

    print("Number of nodes: ", graph.num_vertices())
    print("Number of edges: ", graph.num_edges())
    degrees = graph.get_total_degrees(list(graph.vertices()))
    save_list(degrees, f"graph_stats/{BASE.split('.')[0]}-degrees.txt")
    avg_deg = np.average(degrees)
    print("Average degree: ", avg_deg, 'std: ', np.std(degrees))
    print("Max degree: ", np.max(degrees))
    print("Min degree: ", np.min(degrees))
    del degrees
    indegs = graph.get_in_degrees(graph.get_vertices())
    outdegs = graph.get_out_degrees(graph.get_vertices())
    save_list(indegs, f"graph_stats/{BASE.split('.')[0]}-indegrees.txt")
    save_list(outdegs, f"graph_stats/{BASE.split('.')[0]}-outdegrees.txt")
    print('max indegree: ', np.max(indegs))
    print('max outdegree: ', np.max(outdegs))
    print('min indegree: ', np.min(indegs))
    print('min outdegree: ', np.min(outdegs))
    print('avg indegree: ', np.average(indegs), 'std: ', np.std(indegs))
    print('avg outdegree: ', np.average(outdegs), 'std: ', np.std(outdegs))

    print('Edge reciprocity: ', gt.edge_reciprocity(graph))

    print('Calculating connected components')

    comp, hist = gt.label_components(graph)
    print('Number of connected components: ', len(hist))
    
    save_list(hist, f"graph_stats/{BASE.split('.')[0]}-conncomps.txt")
    print("Largest connected component: ", np.max(hist))
    print("Smallest connected component: ", np.min(hist))
    print("Average connected component size: ", np.average(hist), 'std: ', np.std(hist))
    

