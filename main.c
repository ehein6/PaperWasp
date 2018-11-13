#include "graph_from_edge_list.h"
#include "graph.h"
#include "bfs.h"

int main(int argc, char ** argv) {
    if (argc != 3) {
        LOG("Usage: %s <graph_file> <heavy_threshold>\n", argv[0]);
        exit(1);
    }

    mw_replicated_init(&heavy_threshold, atoi(argv[2]));

    load_graph_from_edge_list(argv[1]);

    bfs_run(0);

    return 0;
}