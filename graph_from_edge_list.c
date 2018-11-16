#include <cilk/cilk.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <emu_c_utils/emu_c_utils.h>

#include "graph.h"

// Global, replicated struct for storing pointers to graph data structures
replicated graph G;

int64_t
count_edges(const char* path)
{
    struct stat st;
    if (stat(path, &st) != 0)
    {
        LOG("Failed to stat %s\n", path);
        exit(1);
    }
    int64_t num_edges = st.st_size / sizeof(edge);
    return num_edges;
}

edge_list
load_edge_list_binary(const char* path)
{
    LOG("Checking file size of %s...\n", path);
    FILE* fp = fopen(path, "rb");
    if (fp == NULL) {
        LOG("Unable to open %s\n", path);
        exit(1);
    }
    int64_t num_edges = count_edges(path);
    edge_list el;
    el.num_edges = num_edges;
    // HACK assuming nv
    el.num_vertices = el.num_edges / 16;
    el.edges = mw_localmalloc(sizeof(edge) * num_edges, &el);
    if (el.edges == NULL) {
        LOG("Failed to allocate memory for %ld edges\n", num_edges);
        exit(1);
    }

    LOG("Preloading %li edges from %s...\n", num_edges, path);
    size_t rc = fread(&el.edges[0], sizeof(edge), num_edges, fp);
    if (rc != num_edges) {
        LOG("Failed to load edge list from %s\n",path);
        exit(1);
    }
    fclose(fp);
    return el;
}


void
clear_striped_array_worker(long * array, long begin, long end, va_list args)
{
    for (long i = begin; i < end; i += NODELETS()) {
        array[i] = 0;
    }
}


void
calculate_degrees_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    // For each edge, increment the degree of its source vertex using a remote atomic
    for (long i = begin; i < end; i += NODELETS()) {
        long src = G.dist_edge_list_src[i];
        REMOTE_ADD(&G.vertex_out_degree[src], 1);
    }
}

void
compute_edge_blocks_sizes_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    // For each edge that belongs to a heavy vertex...
    for (long i = begin; i < end; i += NODELETS()) {
        long src = G.dist_edge_list_src[i];
        if (is_heavy(src)) {
            // Find the edge block near the destination vertex
            long dst = G.dist_edge_list_dst[i];
            edge_block * local_eb = mw_get_localto(G.vertex_edge_block[src], &G.vertex_edge_block[dst]);
            // Increment the edge count
            ATOMIC_ADDMS(&local_eb->num_edges, 1);
        }
    }
}

void
count_local_edges_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    // Sum up size of all local edge blocks
    for (long v = begin; v < end; v += NODELETS()) {
        if (is_heavy(v)) {
            // Heavy vertices have an edge block on each nodelet
            // TODO could avoid some migrations here by giving each local edge block to a local iteration
            for (long nlet = 0; nlet < NODELETS(); ++nlet) {
                // Get the size of the edge block on this nodelet
                edge_block * local_eb = mw_get_nth(G.vertex_edge_block[v], nlet);
                // Add to the counter on this nodelet
                ATOMIC_ADDMS(&G.num_local_edges, local_eb->num_edges);
            }
        } else {
            // For light vertices, we can assume all edges are local
            ATOMIC_ADDMS(&G.num_local_edges, G.vertex_out_degree[v]);
        }
    }
}

void
allocate_edge_blocks_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    // For each vertex...
    for (long v = begin; v < end; v += NODELETS()) {
        if (is_heavy(v)) {
            // Allocate an edge block on each nodelet for the heavy vertex
            // Use replication so we can do locality lookups later
            edge_block * repl_eb = mw_mallocrepl(sizeof(edge_block));
            assert(repl_eb);
            // Zero out all copies
            for (long i = 0; i < NODELETS(); ++i) {
                memset(mw_get_nth(repl_eb, i), 0, sizeof(edge_block));
            }
            G.vertex_local_edges[v] = NULL;
            G.vertex_edge_block[v] = repl_eb;
        }
    }
}

long compute_max_edges_per_nodelet()
{
    // Run around and compute the largest number of edges on any nodelet
    long max_edges_per_nodelet = 0;
    long check_total_edges = 0;
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        long num_edges_on_nodelet = *(long*)mw_get_nth(&G.num_local_edges, nlet);
        REMOTE_MAX(&max_edges_per_nodelet, num_edges_on_nodelet);
        REMOTE_ADD(&check_total_edges, num_edges_on_nodelet);
    }

    // Double-check that we haven't lost any edges
    assert(check_total_edges == G.num_edges);
    return max_edges_per_nodelet;
}

static inline long *
grab_edges(long * volatile * ptr, long num_edges)
{
    // Atomic add only works on long integers, we need to use it on a long*
    return (long*)ATOMIC_ADDMS((volatile long *)ptr, num_edges * sizeof(long));
}

void
carve_edge_storage_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    for (long v = begin; v < end; v += NODELETS()) {
        if (is_heavy(v)) {
            // Heavy vertices have an edge block on each nodelet
            // TODO could avoid some migrations here by giving each local edge block to a local iteration
            for (long nlet = 0; nlet < NODELETS(); ++nlet) {
                edge_block * local_eb = mw_get_nth(G.vertex_edge_block[v], nlet);
                // Carve out a chunk for myself
                local_eb->edges = grab_edges(&G.next_edge_storage, local_eb->num_edges);
                // HACK Prepare to fill
                local_eb->num_edges = 0;
            }
        } else {
            // Local vertices have one edge block on the local nodelet
            G.vertex_local_edges[v] = grab_edges(&G.next_edge_storage, G.vertex_out_degree[v]);
            // HACK Prepare to fill
            G.vertex_out_degree[v] = 0;
        }
    }
}

void
fill_edge_blocks_worker(long * array, long begin, long end, va_list args)
{
    // For each edge...
    for (long i = begin; i < end; i += NODELETS()) {
        long src = G.dist_edge_list_src[i];
        long dst = G.dist_edge_list_dst[i];
        // Pointer to local edge array for this vertex
        long * edges;
        // Pointer to current size of local edge array for this vertex
        long * num_edges_ptr;
        if (is_heavy(src)) {
            // Get the edge block that is colocated with the destination vertex
            edge_block * eb = mw_get_localto(G.vertex_edge_block[src], &G.vertex_edge_block[dst]);
            edges = eb->edges;
            num_edges_ptr = &eb->num_edges;
        } else {
            // Get the local edge array
            edges = G.vertex_local_edges[src];
            num_edges_ptr = &G.vertex_out_degree[src];
        }
        // Atomically claim a position in the edge list
        // NOTE: Relies on all edge counters being set to zero in the previous step
        long pos = ATOMIC_ADDMS(num_edges_ptr, 1);
        // Insert the edge
        edges[pos] = dst;
    }
}

void scatter_edge_list_worker(long begin, long end, va_list args)
{
    edge_list * el = va_arg(args, edge_list*);
    for (long i = begin; i < end; ++i) {
        G.dist_edge_list_src[i] = el->edges[i].src;
        G.dist_edge_list_dst[i] = el->edges[i].dst;
    }
}

void
load_graph_from_edge_list(const char* filename)
{
    // Load edges from disk into local array
    edge_list el = load_edge_list_binary(filename);

    mw_replicated_init(&G.num_edges, el.num_edges);
    mw_replicated_init(&G.num_vertices, el.num_vertices);

    // Create distributed edge list
    LOG("Allocating distributed edge list...\n");
    init_striped_array(&G.dist_edge_list_src, G.num_edges);
    init_striped_array(&G.dist_edge_list_dst, G.num_edges);

    LOG("Initializing distributed edge list...\n");
    // Scatter from local to distributed edge list
    hooks_region_begin("scatter_edge_list");
    emu_local_for(0, G.num_edges, LOCAL_GRAIN_MIN(G.num_edges, 256),
        scatter_edge_list_worker, &el
    );
    hooks_region_end();

    LOG("Initializing distributed vertex list...\n");
    // Create and initialize distributed vertex list
    init_striped_array(&G.vertex_out_degree, G.num_vertices);
    init_striped_array((long**)&G.vertex_local_edges, G.num_vertices);
    init_striped_array((long**)&G.vertex_edge_block, G.num_vertices);

    // TODO set grain more intelligently
    // Grain size to use when scanning the edge list
    long edge_list_grain = GLOBAL_GRAIN_MIN(G.num_edges, 64);
    // Grain size to use when scanning the vertex list
    long vertex_list_grain = GLOBAL_GRAIN_MIN(G.num_vertices, 64);

    // Compute degree of each vertex
    LOG("Computing degree of each vertex...\n");
    hooks_region_begin("calculate_degrees");
    // Initialize the degree of each vertex to zero
    emu_1d_array_apply(G.vertex_out_degree, G.num_vertices, vertex_list_grain,
        clear_striped_array_worker
    );
    // Scan the edge list and do remote atomic adds into vertex_out_degree
    emu_1d_array_apply(G.dist_edge_list_src, G.num_edges, edge_list_grain,
        calculate_degrees_worker
    );
    hooks_region_end();

    // Allocate edge blocks at each vertex
    // Heavy edges get an edge block on each nodelet
    // Edge storage is not allocated yet
    LOG("Allocating edge blocks...\n");
    hooks_region_begin("allocate_edge_blocks");
    emu_1d_array_apply((long*)G.vertex_edge_block, G.num_vertices, vertex_list_grain,
        allocate_edge_blocks_worker
    );
    hooks_region_end();

    // Determine the size of each edge block
    // Light edges are easy, just use the G.vertex_out_degree
    // For heavy edges, we scan the edge list again
    // and do atomic adds into the local edge block
    LOG("Computing edge block sizes...\n");
    hooks_region_begin("compute_edge_block_sizes");
    emu_1d_array_apply(G.dist_edge_list_src, G.num_edges, edge_list_grain,
        compute_edge_blocks_sizes_worker
    );
    hooks_region_end();

    // Count how many edges will need to be stored on each nodelet
    // This is in preparation for the next step, so we can do one big allocation
    // instead of a bunch of tiny ones.
    LOG("Counting local edges...\n");
    hooks_region_begin("count_local_edges");
    mw_replicated_init(&G.num_local_edges, 0);
    emu_1d_array_apply(G.vertex_out_degree, G.num_vertices, vertex_list_grain,
        count_local_edges_worker
    );
    hooks_region_end();

    LOG("Allocating edge storage...\n");
    // Run around and compute the largest number of edges on any nodelet
    long max_edges_per_nodelet = compute_max_edges_per_nodelet();
    LOG("Will use %li MiB on each nodelet\n", (max_edges_per_nodelet * sizeof(long)) >> 20);

    // Allocate a big stripe, such that there is enough room for the nodelet
    // with the most local edges
    // There will be wasted space on the other nodelets
    G.edge_storage = mw_mallocstripe(sizeof(long) * max_edges_per_nodelet);
    replicated_init_ptr(&G.edge_storage, G.edge_storage);
    assert(G.edge_storage);

    // Assign each edge block a position within the big array
    LOG("Carving edge storage...\n");
    replicated_init_ptr(&G.next_edge_storage, G.edge_storage);
    hooks_region_begin("carve_edge_storage");
    emu_1d_array_apply((long*)G.vertex_edge_block, G.num_vertices, vertex_list_grain,
        carve_edge_storage_worker
    );
    hooks_region_end();
    // Populate the edge blocks with edges
    // Scan the edge list one more time
    // For each edge, find the right edge block, then
    // atomically increment eb->nedgesblk to find out where it goes
    LOG("Filling edge blocks...\n");
    hooks_region_begin("fill_edge_blocks");
    emu_1d_array_apply(G.dist_edge_list_src, G.num_edges, edge_list_grain,
        fill_edge_blocks_worker
    );
    hooks_region_end();

    LOG("...Done\n");
}
