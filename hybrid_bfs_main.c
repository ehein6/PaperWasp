#include <string.h>
#include <getopt.h>
#include <limits.h>

#include "load_edge_list.h"
#include "graph_from_edge_list.h"
#include "hybrid_bfs.h"

#define LCG_MUL64 6364136223846793005ULL
#define LCG_ADD64 1

unsigned long lcg_state = 0;

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
    {"graph_filename"   , required_argument},
    {"distributed_load" , no_argument},
    {"heavy_threshold"  , required_argument},
    {"num_trials"       , required_argument},
    {"source_vertex"    , required_argument},
    {"algorithm"        , required_argument},
    {"alpha"            , required_argument},
    {"beta"             , required_argument},
    {"dump_edge_list"   , no_argument},
    {"check_graph"      , no_argument},
    {"dump_graph"       , no_argument},
    {"check_results"    , no_argument},
    {"help"             , no_argument},
    {NULL}
};

void
print_help(const char* argv0)
{
    LOG( "Usage: %s [OPTIONS]\n", argv0);
    LOG("\t--graph_filename     Path to graph file to load\n");
    LOG("\t--distributed_load   Load the graph from all nodes at once (File must exist on all nodes, use absolute path).\n");
    LOG("\t--heavy_threshold    Vertices with this many neighbors will be spread across nodelets\n");
    LOG("\t--num_trials         Run BFS this many times.\n");
    LOG("\t--source_vertex      Use this as the source vertex. If unspecified, pick random vertices.\n");
    LOG("\t--algorithm          Select BFS implementation to run\n");
    LOG("\t--alpha              Alpha parameter for direction-optimizing BFS\n");
    LOG("\t--beta               Beta parameter for direction-optimizing BFS\n");
    LOG("\t--dump_edge_list     Print the edge list to stdout after loading (slow)\n");
    LOG("\t--check_graph        Validate the constructed graph against the edge list (slow)\n");
    LOG("\t--dump_graph         Print the graph to stdout after construction (slow)\n");
    LOG("\t--check_results      Validate the BFS results (slow)\n");
    LOG("\t--help               Print command line help\n");
}

typedef struct bfs_args {
    const char* graph_filename;
    bool distributed_load;
    long heavy_threshold;
    long num_trials;
    long source_vertex;
    const char* algorithm;
    long alpha;
    long beta;
    bool dump_edge_list;
    bool check_graph;
    bool dump_graph;
    bool check_results;
} bfs_args;

struct bfs_args
parse_args(int argc, char *argv[])
{
    bfs_args args;
    args.graph_filename = NULL;
    args.distributed_load = false;
    args.heavy_threshold = LONG_MAX;
    args.num_trials = 1;
    args.source_vertex = -1;
    args.algorithm = "remote_writes_hybrid";
    args.alpha = 15;
    args.beta = 18;
    args.dump_edge_list = false;
    args.check_graph = false;
    args.dump_graph = false;
    args.check_results = false;

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
        } else if (!strcmp(option_name, "distributed_load")) {
            args.distributed_load = true;
        } else if (!strcmp(option_name, "heavy_threshold")) {
            args.heavy_threshold = atol(optarg);
        } else if (!strcmp(option_name, "num_trials")) {
            args.num_trials = atol(optarg);
        } else if (!strcmp(option_name, "source_vertex")) {
            args.source_vertex = atol(optarg);
        } else if (!strcmp(option_name, "algorithm")) {
            args.algorithm = optarg;
        } else if (!strcmp(option_name, "alpha")) {
            args.alpha = atol(optarg);
        } else if (!strcmp(option_name, "beta")) {
            args.beta = atol(optarg);
        } else if (!strcmp(option_name, "dump_edge_list")) {
            args.dump_edge_list = true;
        } else if (!strcmp(option_name, "check_graph")) {
            args.check_graph = true;
        } else if (!strcmp(option_name, "dump_graph")) {
            args.dump_graph = true;
        } else if (!strcmp(option_name, "check_results")) {
            args.check_results = true;
        } else if (!strcmp(option_name, "help")) {
            print_help(argv[0]);
            exit(1);
        }
    }
    if (args.graph_filename == NULL) { LOG( "Missing graph filename\n"); exit(1); }
    if (args.heavy_threshold <= 0) { LOG( "heavy_threshold must be > 0\n"); exit(1); }
    if (args.num_trials <= 0) { LOG( "num_trials must be > 0\n"); exit(1); }
    if (args.alpha <= 0) { LOG( "alpha must be > 0\n"); exit(1); }
    if (args.beta <= 0) { LOG( "beta must be > 0\n"); exit(1); }
    return args;
}

long
pick_random_vertex()
{
    long source;
    do {
        source = lcg_rand(&lcg_state) % G.num_vertices;
    } while (G.vertex_out_degree[source] == 0);
    return source;
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

    // Load the edge list
    if (args.distributed_load) {
        load_edge_list_distributed(args.graph_filename);
    } else {
        load_edge_list(args.graph_filename);
    }
    if (args.dump_edge_list) {
        LOG("Dumping edge list...\n");
        dump_edge_list();
    }

    // Build the graph
    LOG("Constructing graph...\n");
    construct_graph_from_edge_list(args.heavy_threshold);
    print_graph_distribution();
    if (args.check_graph) {
        LOG("Checking graph...");
        if (check_graph()) {
            LOG("PASS\n");
        } else {
            LOG("FAIL\n");
        };
    }
    if (args.dump_graph) {
        LOG("Dumping graph...\n");
        dump_graph();
    }

    // Check for valid source vertex
    if (args.source_vertex >= G.num_vertices) {
        LOG("Source vertex %li out of range.\n", args.source_vertex);
    }

    // Initialize the algorithm
    LOG("Initializing BFS data structures...\n");
    hooks_set_attr_str("algorithm", args.algorithm);
    hybrid_bfs_alg alg;
    if        (!strcmp(args.algorithm, "remote_writes")) {
        alg = REMOTE_WRITES;
    } else if (!strcmp(args.algorithm, "migrating_threads")) {
        alg = MIGRATING_THREADS;
    } else if (!strcmp(args.algorithm, "remote_writes_hybrid")) {
        alg = REMOTE_WRITES_HYBRID;
    } else if (!strcmp(args.algorithm, "beamer_hybrid")) {
        alg = BEAMER_HYBRID;
    } else {
        LOG("Algorithm '%s' not implemented!\n", args.algorithm);
        exit(1);
    }
    hybrid_bfs_init();

    // Initialize RNG with deterministic seed
    lcg_init(&lcg_state, 0);

    long source;
    for (long s = 0; s < args.num_trials; ++s) {
        // Randomly pick a source vertex with positive degree
        if (args.source_vertex >= 0) {
            source = args.source_vertex;
        } else {
            source = pick_random_vertex();
        }

        LOG("Doing breadth-first search from vertex %li (sample %li of %li)\n",
            source, s + 1, args.num_trials);
        // Run the BFS
        hooks_set_attr_i64("source_vertex", source);
        hooks_region_begin("bfs");
        hybrid_bfs_run(alg, source, args.alpha, args.beta);
        double time_ms = hooks_region_end();
        if (args.check_results) {
            LOG("Checking results...\n");
            if (hybrid_bfs_check(source)) {
                LOG("PASS\n");
            } else {
                LOG("FAIL\n");
                hybrid_bfs_print_tree();
            }
        }
        // Output results
        long num_edges_traversed = hybrid_bfs_count_num_traversed_edges();
        LOG("Traversed %li edges in %3.2f ms, %3.2f MTEPS \n",
            num_edges_traversed,
            time_ms,
            (1e-6 * num_edges_traversed) / (time_ms / 1000)
        );
        // Reset for next run
        if (s+1 < args.num_trials) {
            hybrid_bfs_data_clear();
        }
    }
    return 0;
}