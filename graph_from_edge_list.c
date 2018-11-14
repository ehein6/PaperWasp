#include <cilk/cilk.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <emu_c_utils/emu_c_utils.h>

#include "graph.h"

replicated long num_edges;
replicated long num_vertices;

// Global data structures

// Distributed edge list that the graph will be created from.
// First array stores source vertex ID, second array stores dest vertex ID
replicated long * dist_edge_list_src;
replicated long * dist_edge_list_dst;

// Distributed vertex array
// number of vertex_neighbors for this vertex (in the en
replicated long * vertex_out_degree;
// Pointer to edge block for this vertex
// Light vertices: points to a local edge block
// Heavy vertices: points to a stripe
replicated edge_block ** vertex_neighbors;

// Total number of edges stored on each nodelet
replicated long num_local_edges;
// Pointer to stripe of memory where edges are stored
replicated long * edge_storage;
// Pointer to un-reserved edge storage in local stripe
replicated long * next_edge_storage;

replicated long heavy_threshold;

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
        long src = dist_edge_list_src[i];
        REMOTE_ADD(&vertex_out_degree[src], 1);
    }
}

void
compute_edge_blocks_sizes_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    // For each edge that belongs to a heavy vertex...
    for (long i = begin; i < end; i += NODELETS()) {
        long src = dist_edge_list_src[i];
        if (is_heavy(src)) {
            // Find the edge block near the destination vertex
            long dst = dist_edge_list_dst[i];
            edge_block * local_eb = mw_get_localto(vertex_neighbors[src], &vertex_neighbors[dst]);
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
    for (long vertex_id = begin; vertex_id < end; vertex_id += NODELETS()) {
        if (is_heavy(vertex_id)) {
            // Heavy vertices have an edge block on each nodelet
            // TODO could avoid some migrations here by giving each local edge block to a local iteration
            for (long nodelet_id = 0; nodelet_id < NODELETS(); ++nodelet_id) {
                // Get the size of the edge block on this nodelet
                edge_block * local_eb = mw_get_nth(vertex_neighbors[vertex_id], nodelet_id);
                long my_local_edges = local_eb->num_edges;
                // Add to the counter on this nodelet
                ATOMIC_ADDMS(&num_local_edges, my_local_edges);
            }
        } else {
            // Local vertices have one edge block on the local nodelet
            long my_local_edges = vertex_neighbors[vertex_id]->num_edges;
            ATOMIC_ADDMS(&num_local_edges, my_local_edges);
        }
    }
}

void
allocate_edge_blocks_for_light_vertex(long vertex_id)
{
    // Make local edge block for light vertex
    MIGRATE(&vertex_neighbors[vertex_id]);
    edge_block * eb = malloc(sizeof(edge_block));
    assert(eb); // TODO make sure eb is on the right nodelet
    // As long as we're here, set the edge block size
    eb->num_edges = vertex_out_degree[vertex_id];
    eb->edges = NULL;
    vertex_neighbors[vertex_id] = eb;
}

void
allocate_edge_blocks_for_heavy_vertex(long vertex_id)
{
    // Allocate an edge block on each nodelet for the heavy vertex
    // Use replication so we can do locality lookups later
    edge_block * repl_eb = mw_mallocrepl(sizeof(edge_block));
    assert(repl_eb);
    // Zero out all copies
    for (long i = 0; i < NODELETS(); ++i) {
        memset(mw_get_nth(repl_eb, i), 0, sizeof(edge_block));
    }
    vertex_neighbors[vertex_id] = repl_eb;
}

void
allocate_edge_blocks_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    // For each vertex...
    for (long vertex_id = begin; vertex_id < end; vertex_id += NODELETS()) {
        if (is_heavy(vertex_id)) {
            allocate_edge_blocks_for_heavy_vertex(vertex_id);
        } else {
            allocate_edge_blocks_for_light_vertex(vertex_id);
        }
    }
}

long compute_max_edges_per_nodelet()
{
    // Run around and compute the largest number of edges on any nodelet
    long max_edges_per_nodelet = 0;
    long check_total_edges = 0;
    for (long nodelet_id = 0; nodelet_id < NODELETS(); ++nodelet_id) {
        long num_edges_on_nodelet = *(long*)mw_get_nth(&num_local_edges, nodelet_id);
        REMOTE_MAX(&max_edges_per_nodelet, num_edges_on_nodelet);
        REMOTE_ADD(&check_total_edges, num_edges_on_nodelet);
    }

    // Double-check that we haven't lost any edges
    assert(check_total_edges == num_edges);
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
    for (long vertex_id = begin; vertex_id < end; vertex_id += NODELETS()) {
        if (is_heavy(vertex_id)) {
            // Heavy vertices have an edge block on each nodelet
            // TODO could avoid some migrations here by giving each local edge block to a local iteration
            for (long nodelet_id = 0; nodelet_id < NODELETS(); ++nodelet_id) {
                edge_block * local_eb = mw_get_nth(vertex_neighbors[vertex_id], nodelet_id);
                // Carve out a chunk for myself
                local_eb->edges = grab_edges(&next_edge_storage, local_eb->num_edges);
                // HACK Prepare to fill
                local_eb->num_edges = 0;
            }
        } else {
            // Local vertices have one edge block on the local nodelet
            edge_block * local_eb = vertex_neighbors[vertex_id];
            local_eb->edges = grab_edges(&next_edge_storage, local_eb->num_edges);
            // HACK Prepare to fill
            local_eb->num_edges = 0;
        }
    }
}

void
fill_edge_blocks_worker(long * array, long begin, long end, va_list args)
{
    // For each edge...
    for (long i = begin; i < end; i += NODELETS()) {
        long src = dist_edge_list_src[i];
        long dst = dist_edge_list_dst[i];
        edge_block * eb;
        if (is_heavy(src)) {
            // Get the edge block that is colocated with the destination vertex
            eb = mw_get_localto(vertex_neighbors[src], &vertex_neighbors[dst]);
        } else {
            // Get the local edge block
            eb = vertex_neighbors[src];
        }
        // Atomically claim a position in the edge list
        // NOTE: Relies on eb->nedgesblk being set to zero in the previous step
        long pos = ATOMIC_ADDMS(&eb->num_edges, 1);

        // Insert the edge
        eb->edges[pos] = dst;
    }
}

void scatter_edge_list_worker(long begin, long end, va_list args)
{
    edge_list * el = va_arg(args, edge_list*);
    for (long i = begin; i < end; ++i) {
        dist_edge_list_src[i] = el->edges[i].src;
        dist_edge_list_dst[i] = el->edges[i].dst;
    }
}

void
load_graph_from_edge_list(const char* filename)
{
    // Load edges from disk into local array
    edge_list el = load_edge_list_binary(filename);

    mw_replicated_init(&num_edges, el.num_edges);
    mw_replicated_init(&num_vertices, el.num_vertices);

    // Create distributed edge list
    LOG("Allocating distributed edge list...\n");
    init_striped_array(&dist_edge_list_src, num_edges);
    init_striped_array(&dist_edge_list_dst, num_edges);

    LOG("Initializing distributed edge list...\n");
    // Scatter from local to distributed edge list
    hooks_region_begin("scatter_edge_list");
    emu_local_for(0, num_edges, LOCAL_GRAIN_MIN(num_edges, 256),
        scatter_edge_list_worker, &el
    );
    hooks_region_end();

    LOG("Initializing distributed vertex list...\n");
    // Create and initialize distributed vertex list
    init_striped_array(&vertex_out_degree, num_vertices);
    init_striped_array((long**)&vertex_neighbors, num_vertices);

    // TODO set grain more intelligently
    // Grain size to use when scanning the edge list
    long edge_list_grain = GLOBAL_GRAIN_MIN(num_edges, 64);
    // Grain size to use when scanning the vertex list
    long vertex_list_grain = GLOBAL_GRAIN_MIN(num_vertices, 64);

    // Compute degree of each vertex
    LOG("Computing degree of each vertex...\n");
    hooks_region_begin("calculate_degrees");
    // Initialize the degree of each vertex to zero
    emu_1d_array_apply(vertex_out_degree, num_vertices, vertex_list_grain,
        clear_striped_array_worker
    );
    // Scan the edge list and do remote atomic adds into vertex_out_degree
    emu_1d_array_apply(dist_edge_list_src, num_edges, edge_list_grain,
        calculate_degrees_worker
    );
    hooks_region_end();

    // Allocate edge blocks at each vertex
    // Light edges get a single local edge block
    // Heavy edges get an edge block on each nodelet
    // Edge storage is not allocated yet
    LOG("Allocating edge blocks...\n");
    hooks_region_begin("allocate_edge_blocks");
    emu_1d_array_apply((long*)vertex_neighbors, num_vertices, vertex_list_grain,
        allocate_edge_blocks_worker
    );
    hooks_region_end();

    // Determine the size of each edge block
    // Light edges are easy, just use the vertex_out_degree
    // For heavy edges, we scan the edge list again
    // and do atomic adds into the local edge block
    LOG("Computing edge block sizes...\n");
    hooks_region_begin("compute_edge_block_sizes");
    emu_1d_array_apply(dist_edge_list_src, num_edges, edge_list_grain,
        compute_edge_blocks_sizes_worker
    );
    hooks_region_end();

    // Count how many edges will need to be stored on each nodelet
    // This is in preparation for the next step, so we can do one big allocation
    // instead of a bunch of tiny ones.
    LOG("Counting local edges...\n");
    hooks_region_begin("count_local_edges");
    mw_replicated_init(&num_local_edges, 0);
    emu_1d_array_apply(vertex_out_degree, num_vertices, vertex_list_grain,
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
    edge_storage = mw_mallocstripe(sizeof(long) * max_edges_per_nodelet);
    replicated_init_ptr(&edge_storage, edge_storage);
    assert(edge_storage);

    // Assign each edge block a position within the big array
    LOG("Carving edge storage...\n");
    replicated_init_ptr(&next_edge_storage, edge_storage);
    hooks_region_begin("carve_edge_storage");
    emu_1d_array_apply((long*)vertex_neighbors, num_vertices, vertex_list_grain,
        carve_edge_storage_worker
    );
    hooks_region_end();
    // Populate the edge blocks with edges
    // Scan the edge list one more time
    // For each edge, find the right edge block, then
    // atomically increment eb->nedgesblk to find out where it goes
    LOG("Filling edge blocks...\n");
    hooks_region_begin("fill_edge_blocks");
    emu_1d_array_apply(dist_edge_list_src, num_edges, edge_list_grain,
        fill_edge_blocks_worker
    );
    hooks_region_end();

    LOG("...Done\n");
}
