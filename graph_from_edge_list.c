#include <cilk/cilk.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <emu_c_utils/emu_c_utils.h>

#include "graph.h"
#include "load_edge_list.h"

// Global, replicated struct for storing pointers to graph data structures
replicated graph G;


void
init_degrees_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    for (long i = begin; i < end; i += NODELETS()) {
        G.vertex_out_degree[i] = 0;
    }
}


void
calculate_degrees_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    // For each edge, increment the degree of its source vertex using a remote atomic
    for (long i = begin; i < end; i += NODELETS()) {
        long src = EL.src[i];
        long dst = EL.dst[i];
        REMOTE_ADD(&G.vertex_out_degree[src], 1);
        REMOTE_ADD(&G.vertex_out_degree[dst], 1);
    }
}

static inline edge_block *
get_remote_edge_block(long src, long dst)
{
    return mw_get_localto(
        G.vertex_out_neighbors[src].repl_edge_block,
        &G.vertex_out_neighbors[dst]
    );
}

void
compute_edge_blocks_sizes_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    // For each edge that belongs to a heavy vertex...
    for (long i = begin; i < end; i += NODELETS()) {
        long src = EL.src[i];
        long dst = EL.dst[i];
        // src->dst
        if (is_heavy_out(src)) {
            // Find the edge block near the destination vertex
            edge_block * local_eb = get_remote_edge_block(src, dst);
            // Increment the edge count
            ATOMIC_ADDMS(&local_eb->num_edges, 1);
        }
        // dst->src
        if (is_heavy_out(dst)) {
            // Find the edge block near the destination vertex
            edge_block * local_eb = get_remote_edge_block(dst, src);
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
        if (is_heavy_out(v)) {
            // Heavy vertices have an edge block on each nodelet
            // TODO could avoid some migrations here by giving each local edge block to a local iteration
            for (long nlet = 0; nlet < NODELETS(); ++nlet) {
                // Get the size of the edge block on this nodelet
                edge_block * local_eb = mw_get_nth(G.vertex_out_neighbors[v].repl_edge_block, nlet);
                // Add to the counter on this nodelet
                ATOMIC_ADDMS(&G.num_local_edges, local_eb->num_edges);
            }
        } else {
            // For light vertices, we can assume all edges are local
            ATOMIC_ADDMS(&G.num_local_edges, G.vertex_out_degree[v]);
        }
    }
}

edge_block *
allocate_heavy_edge_block()
{
    edge_block * repl_eb = mw_mallocrepl(sizeof(edge_block));
    assert(repl_eb);
    // Zero out all copies
    for (long i = 0; i < NODELETS(); ++i) {
        memset(mw_get_nth(repl_eb, i), 0, sizeof(edge_block));
    }
    return repl_eb;
}

void
allocate_edge_blocks_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    // For each vertex...
    for (long v = begin; v < end; v += NODELETS()) {
        // Allocate an edge block on each nodelet for the heavy vertex
        // Use replication so we can do locality lookups later
        if (is_heavy_out(v)) {
            G.vertex_out_neighbors[v].repl_edge_block = allocate_heavy_edge_block();
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
    assert(check_total_edges == 2 * G.num_edges);
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
        // Empty vertices don't need storage
        if (is_heavy_out(v)) {
            // Heavy vertices have an edge block on each nodelet
            // TODO could avoid some migrations here by giving each local edge block to a local iteration
            for (long nlet = 0; nlet < NODELETS(); ++nlet) {
                edge_block * local_eb = mw_get_nth(G.vertex_out_neighbors[v].repl_edge_block, nlet);
                // Carve out a chunk for myself
                local_eb->edges = grab_edges(&G.next_edge_storage, local_eb->num_edges);
                // HACK Prepare to fill
                local_eb->num_edges = 0;
            }
        } else if (G.vertex_out_degree[v] > 0) {
            // Local vertices have one edge block on the local nodelet
            G.vertex_out_neighbors[v].local_edges = grab_edges(&G.next_edge_storage, G.vertex_out_degree[v]);
            // HACK Prepare to fill
            G.vertex_out_degree[v] = 0;
        }
    }
}

/**
 * This is NOT a general purpose edge insert function, it relies on assumptions
 * - The edge block for this vertex (local or remote) has enough space for the edge
 * - The out-degree for this vertex is counting up from zero, representing the number of edges stored
 */
void
insert_edge(long src, long dst)
{
    // Pointer to local edge array for this vertex
    long * edges;
    // Pointer to current size of local edge array for this vertex
    long * num_edges_ptr;
    // Insert the out-edge
    if (is_heavy_out(src)) {
        // Get the edge block that is colocated with the destination vertex
        edge_block * eb = get_remote_edge_block(src, dst);
        edges = eb->edges;
        num_edges_ptr = &eb->num_edges;
    } else {
        // Get the local edge array
        edges = G.vertex_out_neighbors[src].local_edges;
        num_edges_ptr = &G.vertex_out_degree[src];
    }
    // Atomically claim a position in the edge list and insert the edge
    // NOTE: Relies on all edge counters being set to zero in the previous step
    edges[ATOMIC_ADDMS(num_edges_ptr, 1)] = dst;
}

void
fill_edge_blocks_worker(long * array, long begin, long end, va_list args)
{
    // For each edge...
    for (long i = begin; i < end; i += NODELETS()) {
        long src = EL.src[i];
        long dst = EL.dst[i];
        // Insert both ways for undirected graph
        insert_edge(src, dst);
        insert_edge(dst, src);
    }
}

bool
out_edge_exists(long src, long dst)
{
    // Find the edge block that would contain this neighbor
    long * edges_begin;
    long * edges_end;
    if (is_heavy_out(src)) {
        edge_block * eb = mw_get_localto(
            G.vertex_out_neighbors[src].repl_edge_block,
            &G.vertex_out_neighbors[dst]
        );
        edges_begin = eb->edges;
        edges_end = edges_begin + eb->num_edges;
    } else {
        edges_begin = G.vertex_out_neighbors[src].local_edges;
        edges_end = edges_begin + G.vertex_out_degree[src];
    }

    // Search for the neighbor
    for (long * e = edges_begin; e < edges_end; ++e) {
        assert(*e >= 0);
        assert(*e < G.num_vertices);
        if (*e == dst) { return true; }
    }

    // Neighbor not found
    return false;
}

void check_graph_worker(long * array, long begin, long end, va_list args)
{
    long * ok = va_arg(args, long*);
    for (long i = begin; i < end; i += NODELETS()) {
        long src = EL.src[i];
        long dst = EL.dst[i];
        if (!out_edge_exists(src, dst)) {
            LOG("Missing out edge for %li->%li\n", src, dst);
            *ok = 0;
        }
        if (!out_edge_exists(dst, src)) {
            LOG("Missing out edge for %li->%li\n", src, dst);
            *ok = 0;
        }
    }
}

// Compare the edge list with the constructed graph
// VERY SLOW, use only for testing
bool
check_graph() {
    long ok = 1;
    emu_1d_array_apply(EL.src, G.num_edges, GLOBAL_GRAIN(G.num_edges),
        check_graph_worker, &ok
    );
    return (bool)ok;
}

void dump_graph()
{
    for (long src = 0; src < G.num_vertices; ++src) {
        long * edges_begin;
        long * edges_end;
        if (G.vertex_out_degree[src] == 0) {
            continue;
        } else if (is_heavy_out(src)) {
            LOG("%li ", src);
            for (long nlet = 0; nlet < NODELETS(); ++nlet) {
                LOG("\n    nlet[%02li] ->", nlet);
                edge_block * eb = mw_get_nth(G.vertex_out_neighbors[src].repl_edge_block, nlet);
                edges_begin = eb->edges;
                edges_end = edges_begin + eb->num_edges;
                for (long * e = edges_begin; e < edges_end; ++e) { LOG(" %li", *e); }
            }
        } else {
            LOG("%li ->", src);
            edges_begin = G.vertex_out_neighbors[src].local_edges;
            edges_end = edges_begin + G.vertex_out_degree[src];
            for (long * e = edges_begin; e < edges_end; ++e) { LOG(" %li", *e); }
        }
        LOG("\n");
    }
}

void
construct_graph_from_edge_list(long heavy_threshold)
{
    mw_replicated_init(&G.num_edges, EL.num_edges);
    mw_replicated_init(&G.num_vertices, EL.num_vertices);
    mw_replicated_init(&G.heavy_threshold, heavy_threshold);

    LOG("Initializing distributed vertex list...\n");
    // Create and initialize distributed vertex list
    init_striped_array(&G.vertex_out_degree, G.num_vertices);
    init_striped_array((long**)&G.vertex_out_neighbors, G.num_vertices);

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
        init_degrees_worker
    );
    // Scan the edge list and do remote atomic adds into vertex_out_degree
    emu_1d_array_apply(EL.src, G.num_edges, edge_list_grain,
        calculate_degrees_worker
    );
    hooks_region_end();

    // Allocate edge blocks at each vertex
    // Heavy edges get an edge block on each nodelet
    // Edge storage is not allocated yet
    LOG("Allocating edge blocks...\n");
    hooks_region_begin("allocate_edge_blocks");
    emu_1d_array_apply((long*)G.vertex_out_neighbors, G.num_vertices, vertex_list_grain,
        allocate_edge_blocks_worker
    );
    hooks_region_end();

    // Determine the size of each edge block
    // Light edges are easy, just use the G.vertex_out_degree
    // For heavy edges, we scan the edge list again
    // and do atomic adds into the local edge block
    LOG("Computing edge block sizes...\n");
    hooks_region_begin("compute_edge_block_sizes");
    emu_1d_array_apply(EL.src, G.num_edges, edge_list_grain,
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
    long ** edge_storage = mw_malloc2d(NODELETS(), sizeof(long) * max_edges_per_nodelet);
    assert(edge_storage);
    // Initialize each copy of G.edge_storage to point to the local chunk
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        *(long**)mw_get_nth(&G.edge_storage, nlet) = edge_storage[nlet];
        *(long**)mw_get_nth(&G.next_edge_storage, nlet) = edge_storage[nlet];
    }

    // Assign each edge block a position within the big array
    LOG("Carving edge storage...\n");
    hooks_region_begin("carve_edge_storage");
    emu_1d_array_apply((long*)G.vertex_out_neighbors, G.num_vertices, vertex_list_grain,
        carve_edge_storage_worker
    );
    hooks_region_end();
    // Populate the edge blocks with edges
    // Scan the edge list one more time
    // For each edge, find the right edge block, then
    // atomically increment eb->nedgesblk to find out where it goes
    LOG("Filling edge blocks...\n");
    hooks_region_begin("fill_edge_blocks");
    emu_1d_array_apply(EL.src, G.num_edges, edge_list_grain,
        fill_edge_blocks_worker
    );
    hooks_region_end();

    // LOG("Checking graph...\n");
    // check_graph();
    // dump_graph();

    LOG("...Done\n");
}

void
count_num_heavy_vertices_worker(long * array, long begin, long end, long * sum, va_list args)
{
    long local_sum = 0;
    for (long v = begin; v < end; v += NODELETS()) {
        if (is_heavy_out(v)) {
            local_sum += 1;
        }
    }
    REMOTE_ADD(sum, local_sum);
}

long count_num_heavy_vertices() {
    return emu_1d_array_reduce_sum(G.vertex_out_degree, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 128),
        count_num_heavy_vertices_worker
    );
}

void print_graph_distribution()
{
    long num_heavy_vertices = count_num_heavy_vertices();
    LOG("Heavy vertices: %li / %li (%3.0f%%)\n",
        num_heavy_vertices,
        G.num_vertices,
        100.0 * ((double)num_heavy_vertices / (double)G.num_vertices)
    );
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        LOG("nlet[%li]: %20li edges\n", nlet, *(long*)mw_get_nth(&G.num_local_edges, nlet));
    }
}
