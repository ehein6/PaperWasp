#include <string.h>
#include <getopt.h>
#include <limits.h>

#include "load_edge_list.h"
#include "graph_from_edge_list.h"

const struct option long_options[] = {
    {"graph_filename"   , required_argument},
    {"heavy_threshold"  , required_argument},
    {"dump_edge_list"   , no_argument},
    {"check_graph"      , no_argument},
    {"dump_graph"       , no_argument},
    {"help"             , no_argument},
    {NULL}
};

void
print_help(const char* argv0)
{
    LOG( "Usage: %s [OPTIONS]\n", argv0);
    LOG("\t--graph_filename     Path to graph file to load\n");
    LOG("\t--heavy_threshold    Vertices with this many neighbors will be spread across nodelets\n");
    LOG("\t--dump_edge_list     Print the edge list to stdout after loading (slow)\n");
    LOG("\t--check_graph        Validate the constructed graph against the edge list (slow)\n");
    LOG("\t--dump_graph         Print the graph to stdout after construction (slow)\n");
    LOG("\t--help               Print command line help\n");
}

typedef struct graph_args {
    const char* graph_filename;
    long heavy_threshold;
    bool dump_edge_list;
    bool check_graph;
    bool dump_graph;
} graph_args;

struct graph_args
parse_args(int argc, char *argv[])
{
    graph_args args;
    args.graph_filename = NULL;
    args.heavy_threshold = LONG_MAX;
    args.dump_edge_list = false;
    args.check_graph = false;
    args.dump_graph = false;

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
        } else if (!strcmp(option_name, "dump_edge_list")) {
            args.dump_edge_list = true;
        } else if (!strcmp(option_name, "check_graph")) {
            args.check_graph = true;
        } else if (!strcmp(option_name, "dump_graph")) {
            args.dump_graph = true;
        } else if (!strcmp(option_name, "help")) {
            print_help(argv[0]);
            exit(1);
        }
    }
    if (args.graph_filename == NULL) { LOG( "Missing graph filename\n"); exit(1); }
    if (args.heavy_threshold <= 0) { LOG( "heavy_threshold must be > 0\n"); exit(1); }
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
    graph_args args = parse_args(argc, argv);
    hooks_set_attr_i64("heavy_threshold", args.heavy_threshold);

    // Load the edge list
    load_edge_list(args.graph_filename);
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

    return 0;
}
