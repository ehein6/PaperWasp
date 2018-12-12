#include "graph_from_edge_list.h"
#include "bfs.h"


#define LCG_MUL64 6364136223846793005ULL
#define LCG_ADD64 1

void
lcg_init(unsigned long * x, unsigned long step)
{
    unsigned long mul_k, add_k, ran, un;

    mul_k = LCG_MUL64;
    add_k = LCG_ADD64;

    ran = 1;
    for (un = step; un; un >>= 1) {
        if (un & 1)
            ran = mul_k * ran + add_k;
        add_k *= (mul_k + 1);
        mul_k *= mul_k;
    }

    *x = ran;
}

unsigned long
lcg_rand(unsigned long * x) {
    *x = LCG_MUL64 * *x + LCG_ADD64;
    return *x;
}

int main(int argc, char ** argv)
{
    if (argc != 4) {
        LOG("Usage: %s <graph_file> <heavy_threshold> <num_samples>\n", argv[0]);
        exit(1);
    }

    const char* active_region = getenv("HOOKS_ACTIVE_REGION");
    if (active_region != NULL) {
        hooks_set_active_region(active_region);
    }

    mw_replicated_init(&G.heavy_threshold, atol(argv[2]));
    long num_samples = atol(argv[3]);
    assert(num_samples > 0);

    load_graph_from_edge_list(argv[1]);
    print_graph_distribution();

    LOG("Initializing BFS data structures...\n");
    bool use_remote_writes = true;
    bfs_init(use_remote_writes);

    unsigned long lcg_state = 0;
    lcg_init(&lcg_state, 0); // deterministic seed
    long source;
    for (long s = 0; s < num_samples; ++s) {
        // Randomly pick a source vertex with positive degree
        do {
            source = lcg_rand(&lcg_state) % G.num_vertices;
        } while (G.vertex_out_degree[source] == 0);
        LOG("Doing breadth-first search from vertex %li (sample %li of %li)\n",
            source, s + 1, num_samples);
        // Run the BFS
        hooks_set_attr_i64("source_vertex", source);
        hooks_region_begin("bfs");
        bfs_run(source);
        double time_ms = hooks_region_end();
        // Output results
        LOG("Completed in %3.2f ms, %3.2f MTEPS \n",
            time_ms, (1e-6 * G.num_edges) / (time_ms / 1000)
        );
        // Reset for next run
        bfs_data_clear();
    }
    return 0;
}