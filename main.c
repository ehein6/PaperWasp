#include <string.h>
#include <getopt.h>
#include <limits.h>

#include "load_edge_list.h"
#include "graph_from_edge_list.h"
#include "hybrid_bfs.h"

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

const struct option long_options[] = {
    {"graph_filename"   ,  required_argument},
    {"heavy_threshold"  , required_argument},
    {"num_samples"      , required_argument},
    {"algorithm"        , required_argument},
    {"alpha"            , required_argument},
    {"beta"            , required_argument},
    {"help"             , no_argument},
    {NULL}
};

void
print_help(const char* argv0)
{
    LOG( "Usage: %s [OPTIONS]\n", argv0);
    LOG("\t--graph_filename     Path to graph file to load\n");
    LOG("\t--heavy_threshold    Vertices with this many neighbors will be spread across nodelets\n");
    LOG("\t--num_samples        Run BFS against this many random vertices\n");
    LOG("\t--algorithm          Select BFS implementation to run\n");
    LOG("\t--alpha              Alpha parameter for direction-optimizing BFS\n");
    LOG("\t--beta               Beta parameter for direction-optimizing BFS\n");
    LOG("\t--help               Print command line help\n");
}

typedef struct bfs_args {
    const char* graph_filename;
    long heavy_threshold;
    long num_samples;
    const char* algorithm;
    long alpha;
    long beta;
} bfs_args;

struct bfs_args
parse_args(int argc, char *argv[])
{
    bfs_args args;
    args.graph_filename = NULL;
    args.heavy_threshold = LONG_MAX;
    args.num_samples = 1;
    args.algorithm = "remote_writes";
    args.alpha = 15;
    args.beta = 18;

    int option_index;
    while (true)
    {
        int c = getopt_long(argc, argv, "", long_options, &option_index);
        // Done parsing
        if (c == -1) { break; }
        // Parse error
        if (c == '?') {
            LOG( "Invalid arguments\n");
            print_help(argv[0]);
            exit(1);
        }
        const char* option_name = long_options[option_index].name;

        if (!strcmp(option_name, "graph_filename")) {
            args.graph_filename = optarg;
        } else if (!strcmp(option_name, "heavy_threshold")) {
            args.heavy_threshold = atol(optarg);
        } else if (!strcmp(option_name, "num_samples")) {
            args.num_samples = atol(optarg);
        } else if (!strcmp(option_name, "algorithm")) {
            args.algorithm = optarg;
        } else if (!strcmp(option_name, "alpha")) {
            args.alpha = atol(optarg);
        } else if (!strcmp(option_name, "beta")) {
            args.beta = atol(optarg);
        } else if (!strcmp(option_name, "help")) {
            print_help(argv[0]);
            exit(1);
        }
    }
    if (args.graph_filename == NULL) { LOG( "Missing graph filename\n"); exit(1); }
    if (args.heavy_threshold <= 0) { LOG( "heavy_threshold must be > 0\n"); exit(1); }
    if (args.num_samples <= 0) { LOG( "num_samples must be > 0\n"); exit(1); }
    if (args.alpha <= 0) { LOG( "alpha must be > 0\n"); exit(1); }
    if (args.beta <= 0) { LOG( "beta must be > 0\n"); exit(1); }
    return args;
}

int main(int argc, char ** argv)
{
    // Set active region for hooks
    const char* active_region = getenv("HOOKS_ACTIVE_REGION");
    if (active_region != NULL) {
        hooks_set_active_region(active_region);
    }

    // Parse command-line argumetns
    bfs_args args = parse_args(argc, argv);
    hooks_set_attr_i64("heavy_threshold", args.heavy_threshold);

    // Load the graph
    load_edge_list(args.graph_filename);
    LOG("Constructing graph...\n");
    construct_graph_from_edge_list(args.heavy_threshold);
    print_graph_distribution();

    // Initialize the algorithm
    LOG("Initializing BFS data structures...\n");
    hooks_set_attr_str("algorithm", args.algorithm);
    bool use_remote_writes;
    if (!strcmp(args.algorithm, "remote_writes")) {
        use_remote_writes = true;
    } else if (!strcmp(args.algorithm, "migrating_threads")) {
        use_remote_writes = false;
    } else {
        LOG("Algorithm '%s' not implemented!\n", args.algorithm);
        exit(1);
    }
    hybrid_bfs_init(use_remote_writes);

    // Initialize RNG with deterministic seed
    unsigned long lcg_state = 0;
    lcg_init(&lcg_state, 0);

    long source;
    for (long s = 0; s < args.num_samples; ++s) {
        // Randomly pick a source vertex with positive degree
        do {
            source = lcg_rand(&lcg_state) % G.num_vertices;
        } while (G.vertex_out_degree[source] == 0);
        LOG("Doing breadth-first search from vertex %li (sample %li of %li)\n",
            source, s + 1, args.num_samples);
        // Run the BFS
        hooks_set_attr_i64("source_vertex", source);
        hooks_region_begin("bfs");
        hybrid_bfs_run(source, args.alpha, args.beta);
        double time_ms = hooks_region_end();

        LOG("Checking results...");
        if (hybrid_bfs_check(source)) {
            LOG("PASS\n")
        } else {
            LOG("FAIL\n");
            hybrid_bfs_print_tree();
        }
        // Output results
        LOG("Completed in %3.2f ms, %3.2f MTEPS \n",
            time_ms, (1e-6 * G.num_edges) / (time_ms / 1000)
        );
        // Reset for next run
        hybrid_bfs_data_clear();
    }
    return 0;
}