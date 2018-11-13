#include "graph_from_edge_list.h"
#include "bfs.h"

int main(int argc, char ** argv)
{
    if (argc != 3) {
        LOG("Usage: %s <graph_file> <heavy_threshold>\n", argv[0]);
        exit(1);
    }

    mw_replicated_init(&heavy_threshold, atoi(argv[2]));

    load_graph_from_edge_list(argv[1]);

    LOG("Initializing BFS data structures...\n");
    bfs_init(true);

    long source = 0;
    LOG("Doing breadth-first search from vertex %li\n", source);
    hooks_set_attr_i64("source_vertex", source);
    hooks_region_begin("bfs");
    bfs_run(0);
    double time_ms = hooks_region_end();
    LOG("Completed in %3.2f ms, %3.2f MTEPS \n",
        time_ms, (1e-6 * num_edges) / (time_ms / 1000)
    );

    return 0;
}