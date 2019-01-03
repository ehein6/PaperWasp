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
    {"is_permuted"      , no_argument},
    {"is_directed"      , no_argument},
    {"is_undirected"    , no_argument},
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
    // Number of bytes in the file header.
    // Includes the newline character
    // There is no null terminator
    size_t header_length;
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
    // Get the first line of text from the file
    char line[256];
    fgets(line, 256, fp);
    size_t line_len = strlen(line);
    header->header_length = line_len;
    // Strip endline character
    if (line[line_len-1] != '\n') {
        LOG("Invalid edge list file header\n");
        exit(1);
    }
    line[--line_len] = '\0';

    // Split on spaces
    //    Assumption: never more than 32 arguments in header
    const int max_argc = 32;
    char * argv[max_argc];
    // Leave empty slot for expected program name
    argv[0] = "edge list header";
    int argc = 1;
    char * token = strtok(line, " ");
    while (token && argc < max_argc) {
        argv[argc++] = token;
        token = strtok(NULL, " ");
    }
    // Null terminator for argv
    argv[argc] = NULL;

    // Assume default values
    header->num_vertices = -1;
    header->num_edges = -1;
    header->is_sorted = false;
    header->is_deduped = false;
    header->format = NULL;

    // Reset getopt
    optind = 1;
    // Parse header like an option string
    int option_index;
    while (true)
    {
        int c = getopt_long(argc, argv, "", header_options, &option_index);
        // Done parsing
        if (c == -1) { break; }
        // Invalid arg
        if (c == '?') {
            LOG( "Invalid field edge list header\n");
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
    if (!header.format || !!strcmp(header.format, "el64")) {
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
        LOG("Failed to load edge list from %s ", path);
        if (feof(fp)) {
            LOG("unexpected EOF\n");
        } else if (ferror(fp)) {
            perror("fread returned error\n");
        }
        exit(1);
    }
    fclose(fp);
}

void
init_dist_edge_list(long num_vertices, long num_edges)
{
    // Create distributed edge list
    mw_replicated_init(&EL.num_vertices, num_vertices);
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
    // Scatter from local to distributed edge list
    emu_local_for(0, el->num_edges, LOCAL_GRAIN_MIN(el->num_edges, 256),
        scatter_edge_list_worker, el
    );
    hooks_region_end();
}

// Initializes the distributed edge list EL from the file
void load_edge_list(const char* filename)
{
    edge_list el;

    hooks_region_begin("load_graph");
    load_edge_list_local(filename, &el);
    hooks_region_end();

    init_dist_edge_list(el.num_vertices, el.num_edges);

    hooks_region_begin("scatter_edge_list");
    scatter_edges(&el);
    hooks_region_end();
}

// void
// load_edge_list_distributed(const char* filename)
// {
//     LOG("Not implemented yet!");
//     exit(1);

// TODO add these to emu_c_utils
#ifndef __le64__
static inline FILE *
mw_fopen(const char *path, const char *mode, void *local_ptr)
{
    (void)local_ptr;
    return fopen(path, mode);
}
static inline int
mw_fclose(FILE * fp)
{
    return fclose(fp);
}
static inline size_t
mw_fread(void *ptr, size_t size, size_t nmemb, FILE *fp)
{
    return (int)fread(ptr, size, nmemb, fp);
}
static inline size_t
mw_fwrite(void *ptr, size_t size, size_t nmemb, FILE *fp)
{
    return (int)fwrite(ptr, size, nmemb, fp);
}
#endif

void
buffered_edge_list_reader(long * array, long begin, long end, va_list args)
{
    /*
     * We can't read directly into the distributed edge list.
     * Need to convert to in-memory struct-of-arrays from the file's array-of-structs.
     * So we read a large chunk at a time (minimize # of calls to fread).
     * And then unpack it into the local portion of the distributed EL
     */
    // Open the file
    char * filename = va_arg(args, char*);
    size_t file_header_len = va_arg(args, size_t);
    FILE * fp = mw_fopen(filename, "rb", &EL.src[begin]);
    if (fp == NULL) {
        LOG("Error opening %s on nodelet %li\n", filename, NODE_ID());
        exit(1);
    }
    // Skip past the header and jump to this threads portion of the edge list
    int rc = fseek(fp, file_header_len + sizeof(edge) * begin, SEEK_SET);
    if (rc) {
        LOG("Error loading %li-th edge from nodelet %li\n", begin, NODE_ID());
        exit(1);
    }

    size_t buffer_size = 32768;
    edge buffer[buffer_size];
    for (size_t pos = begin; pos < end;) {

        // Fill the buffer with edges from the file
        size_t n = pos + buffer_size;
        if (n > end) { n = end - pos; }
        size_t rc = mw_fread(buffer, sizeof(edge), n, fp);
        if (rc != n) {
            LOG("Error during graph loading");
            exit(1);
        }

        // Copy into the edge list
        for (size_t i = 0; i < n; ++i) {
            EL.src[pos] = buffer[i].src;
            EL.dst[pos] = buffer[i].dst;
            // Next edge on this nodelet is at NODELETS stride away
            pos += NODELETS();
        }
    }
    // Clean up
    mw_fclose(fp);
}

void
load_edge_list_distributed(const char* filename)
{
    // Open the file just to check the header
    LOG("Opening %s...\n", filename);
    FILE* fp = fopen(filename, "rb");
    if (fp == NULL) {
        LOG("Unable to open %s\n", filename);
        exit(1);
    }
    edge_list_file_header header;
    parse_edge_list_file_header(fp, &header);
    fclose(fp);

    if (header.num_vertices <= 0 || header.num_edges <= 0) {
        LOG("Invalid graph size in header\n");
        exit(1);
    }
    // TODO add support for other formats
    if (!header.format || !!strcmp(header.format, "el64")) {
        LOG("Unsuppported edge list format %s\n", header.format);
        exit(1);
    }
    // Future implementations may be able to handle duplicates
    if (!header.is_deduped) {
        LOG("Edge list must be deduped.");
        exit(1);
    }

    init_dist_edge_list(header.num_vertices, header.num_edges);

    emu_1d_array_apply(EL.src, EL.num_edges, GLOBAL_GRAIN_MIN(EL.num_edges, 32768),
        buffered_edge_list_reader, filename, header.header_length
    );
}




