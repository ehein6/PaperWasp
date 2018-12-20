#include "load_edge_list.h"
#include "common.h"
#include <getopt.h>
#include <stdio.h>
#include <string.h>

// Single global instance of the distributed edge list
replicated dist_edge_list EL;

static const struct option header_options[] = {
    {"num_vertices"     , required_argument},
    {"num_edges"        , required_argument},
    {"is_sorted"        , no_argument},
    {"is_deduped"       , no_argument},
    {"format"           , required_argument},
    {NULL}
};

typedef struct edge_list_file_header {
    // Number of vertices in the file
    //  There can be fewer actual unique vertex ID's, but
    //  every vertex ID must be < num_vertices
    long num_vertices;
    // Number of edges in the file, including duplicates
    long num_edges;
    // Is the edge list sorted?
    bool is_sorted;
    // Have duplicate edges been removed?
    bool is_deduped;
    // Format of the edge list: (for example, el64)
    //   el   : src, dst
    //   wel  : src, dst, weight
    //   welt : src, dst, weight, timestamp
    // Suffixes:
    //       : text, delimited by spaces and newlines
    //   32  : binary, 32 bits per field
    //   64  : binary, 64 bits per field
    char* format;
} edge_list_file_header;

void
dump_edge_list()
{
    for (long i = 0; i < EL.num_edges; ++i) {
        long src = EL.src[i];
        long dst = EL.dst[i];
        LOG ("%li -> %li\n", src, dst);
    }
}

void
parse_edge_list_file_header(FILE* fp, edge_list_file_header *header)
{
    // Read the first line of the file
    char * line = NULL;
    size_t line_len = 0;
    size_t rc = getline(&line, &line_len, fp);
    if (!rc) return;

    // Split on spaces
    //    Assumption: never more than 32 arguments in header
    const int max_argc = 32;
    char * argv[max_argc];
    int argc = 0;
    char * token = strtok(line, " ");
    while (token && argc < max_argc) {
        argv[argc++] = token;
        token = strtok(NULL, " ");
    }

    // Assume default values
    header->num_vertices = -1;
    header->num_edges = -1;
    header->is_sorted = false;
    header->is_deduped = false;
    header->format = NULL;

    // Parse header like an option string
    int option_index;
    while (true)
    {
        int c = getopt_long(argc, argv, "", header_options, &option_index);
        // Done parsing
        if (c == -1) { break; }
        // Invalid arg
        if (c == '?') {
            LOG( "Invalid edge list header\n");
            exit(1);
        }
        const char* option_name = header_options[option_index].name;

        if (!strcmp(option_name, "num_vertices")) {
            header->num_vertices = atol(optarg);
        } else if (!strcmp(option_name, "num_edges")) {
            header->num_edges = atol(optarg);
        } else if (!strcmp(option_name, "is_sorted")) {
            header->is_sorted = true;
        } else if (!strcmp(option_name, "is_deduped")) {
            header->is_deduped = true;
        } else if (!strcmp(option_name, "format")) {
            header->format = optarg;
        }
    }

    free(line);
    // It's up to the caller to validate and interpret the arguments
}

void
load_edge_list_local(const char* path, edge_list * el)
{
    LOG("Opening %s...\n", path);
    FILE* fp = fopen(path, "rb");
    if (fp == NULL) {
        LOG("Unable to open %s\n", path);
        exit(1);
    }

    edge_list_file_header header;
    parse_edge_list_file_header(fp, &header);

    if (header.num_vertices <= 0 || header.num_edges <= 0) {
        LOG("Invalid graph size in header\n");
        exit(1);
    }
    // TODO add support for other formats
    if (!strcmp(header.format, "el64")) {
        LOG("Unsuppported edge list format %s\n", header.format);
        exit(1);
    }
    // Future implementations may be able to handle duplicates
    if (!header.is_deduped) {
        LOG("Edge list must be sorted and deduped.");
        exit(1);
    }

    el->num_edges = header.num_edges;
    el->num_vertices = header.num_vertices;
    el->edges = mw_localmalloc(sizeof(edge) * header.num_edges, el);
    if (el->edges == NULL) {
        LOG("Failed to allocate memory for %ld edges\n", header.num_edges);
        exit(1);
    }

    LOG("Loading %li edges from %s...\n", header.num_edges, path);
    size_t rc = fread(&el->edges[0], sizeof(edge), header.num_edges, fp);
    if (rc != header.num_edges) {
        LOG("Failed to load edge list from %s\n", path);
        exit(1);
    }
    fclose(fp);
}

void
init_dist_edge_list(long num_edges)
{
    // Create distributed edge list
    LOG("Allocating distributed edge list...\n");
    mw_replicated_init(&EL.num_edges, num_edges);
    init_striped_array(&EL.src, num_edges);
    init_striped_array(&EL.dst, num_edges);
}

void
scatter_edge_list_worker(long begin, long end, va_list args)
{
    edge_list * el = va_arg(args, edge_list*);
    for (long i = begin; i < end; ++i) {
        EL.src[i] = el->edges[i].src;
        EL.dst[i] = el->edges[i].dst;
    }
}

void
scatter_edges(edge_list * el)
{
    LOG("Initializing distributed edge list...\n");
    // Scatter from local to distributed edge list
    hooks_region_begin("scatter_edge_list");
    emu_local_for(0, el->num_edges, LOCAL_GRAIN_MIN(el->num_edges, 256),
        scatter_edge_list_worker, el
    );
    hooks_region_end();
}

// Initializes the distributed edge list EL from the file
void load_edge_list(const char* filename)
{
    edge_list el;
    load_edge_list_local(filename, &el);
    init_dist_edge_list(el.num_edges);
    scatter_edges(&el);
}

// void
// load_edge_list_distributed(const char* filename)
// {
//     LOG("Not implemented yet!");
//     exit(1);

//     init_dist_edge_list(num_edges);
//     // FIXME
// }




