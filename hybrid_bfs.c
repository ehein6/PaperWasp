#include "hybrid_bfs.h"
#include <stdlib.h>
#include <assert.h>
#include <cilk/cilk.h>
#include <emu_c_utils/emu_c_utils.h>
#include <stdio.h>
#include "ack_control.h"
#include "hybrid_bfs.h"

// Global replicated struct with BFS data pointers
replicated hybrid_bfs_data HYBRID_BFS;


static void
queue_to_bitmap(sliding_queue * q, bitmap * b)
{
    // TODO parallelize with emu_local_for
    for (long i = q->start; i < q->end; ++i) {
        bitmap_set_bit(b, q->buffer[i]);
    }
}

static void
bitmap_to_queue_worker(long * array, long begin, long end, va_list args)
{
    bitmap * b = va_arg(args, bitmap*);
    sliding_queue * q = va_arg(args, sliding_queue *);
    for (long v = begin; v < end; v += NODELETS()){
        if (bitmap_get_bit(b, v)) {
            sliding_queue_push_back(q, v);
        }
    }
}

static void
bitmap_to_queue(bitmap * b, sliding_queue * q)
{
    // TODO parallelize with emu_local_for
    emu_1d_array_apply(G.vertex_out_degree, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 64),
        bitmap_to_queue_worker, b, q
    );
}


static void
clear_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    const long nodelets = NODELETS();
    for (long i = begin; i < end; i += nodelets) {
        HYBRID_BFS.parent[i] = -1;
        HYBRID_BFS.new_parent[i] = -1;
    }
}

void
bfs_data_clear()
{
//    emu_1d_array_set_long(&self->level, -1);
//    emu_1d_array_set_long(&self->marks, 0);
    emu_1d_array_apply(HYBRID_BFS.parent, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 256),
        clear_worker
    );
    sliding_queue_replicated_reset(&HYBRID_BFS.queue);
}


void bfs_init(long use_remote_writes)
{
    mw_replicated_init(&HYBRID_BFS.use_remote_writes, use_remote_writes);
    init_striped_array(&HYBRID_BFS.parent, G.num_vertices);
    init_striped_array(&HYBRID_BFS.new_parent, G.num_vertices);
    sliding_queue_replicated_init(&HYBRID_BFS.queue, G.num_vertices);
    bfs_data_clear();
    ack_control_init();
}

void
bfs_deinit()
{
    mw_free(HYBRID_BFS.parent);
    mw_free(HYBRID_BFS.new_parent);
    sliding_queue_replicated_deinit(&HYBRID_BFS.queue);
}

// Core of the remote-writes BFS variant
// Fire off a remote write for each edge in the frontier
// This write travels to the home node for the destination vertex,
// setting the source vertex as its parent
static inline void
mark_neighbors(long src, long * edges_begin, long * edges_end)
{
    for (long * e = edges_begin; e < edges_end; ++e) {
        long dst = *e;
        HYBRID_BFS.new_parent[dst] = src; // Remote write
    }
}

static inline long
MY_LOCAL_GRAIN(long n)
{
    long local_num_threads = 64 * 1;
    return n > local_num_threads ? (n/local_num_threads) : 1;
}

/**
 * Similar to LOCAL_GRAIN, except the final grain size can never be smaller than @c min_grain
 * @param n number of loop iterations
 * @param min_grain minimum grain size to return
 * @return grain size
 */
static inline long
MY_LOCAL_GRAIN_MIN(long n, long min_grain)
{
    long grain = MY_LOCAL_GRAIN(n);
    return grain > min_grain ? grain : min_grain;
}

static inline void
mark_neighbors_parallel(long src, long * edges_begin, long * edges_end)
{
    long degree = edges_end - edges_begin;
    long grain = MY_LOCAL_GRAIN_MIN(degree, 128);
    if (degree <= grain) {
        // Low-degree local vertex, handle in this thread
        mark_neighbors(src, edges_begin, edges_end);
    } else {
        // High-degree local vertex, spawn local threads
        for (long * e1 = edges_begin; e1 < edges_end; e1 += grain) {
            long * e2 = e1 + grain;
            if (e2 > edges_end) { e2 = edges_end; }
            cilk_spawn mark_neighbors(src, e1, e2);
        }
    }
}

// Wrapper for emu_local_for to call mark_neighbors
void
mark_neighbors_worker(long begin, long end, va_list args)
{
    long src = va_arg(args, long);
    long * edges_begin = va_arg(args, long*);
    long * edges_end = edges_begin + (end - begin);
    mark_neighbors(src, edges_begin, edges_end);
}

// Marks all neighbors in an edge block in parallel
void
mark_neighbors_in_eb(long src, edge_block * eb)
{
    // emu_local_for(0, eb->num_edges, LOCAL_GRAIN_MIN(eb->num_edges, 1024),
    //     mark_neighbors_worker, src, eb->edges
    // );
    mark_neighbors_parallel(src, eb->edges, eb->edges + eb->num_edges);
}

// Spawns threads to call mark_neighbors in parallel over a slice of the frontier
void
mark_queue_neighbors_worker(long begin, long end, va_list args)
{
    // For each vertex in our slice of the queue...
    long * vertex_queue = va_arg(args, long*);
    for (long v = begin; v < end; ++v) {
        long src = vertex_queue[v];
        // How big is this vertex?
        if (is_heavy_out(src)) {
            // Heavy vertex, spawn a thread for each remote edge block
            edge_block * eb = G.vertex_out_neighbors[src].repl_edge_block;
            for (long i = 0; i < NODELETS(); ++i) {
                edge_block * remote_eb = mw_get_nth(eb, i);
                cilk_spawn_at(remote_eb) mark_neighbors_in_eb(src, remote_eb);
            }
        } else {
            long * edges_begin = G.vertex_out_neighbors[src].local_edges;
            long * edges_end = edges_begin + G.vertex_out_degree[src];
            mark_neighbors_parallel(src, edges_begin, edges_end);
        }
    }
}

// Spawns threads to call mark_neighbors over the entire local frontier
//
void
mark_queue_neighbors(sliding_queue * queue)
{
    ack_control_disable_acks();
    emu_local_for(queue->start, queue->end, MY_LOCAL_GRAIN_MIN(queue->end - queue->start, 8),
        mark_queue_neighbors_worker, queue->buffer
    );
    ack_control_reenable_acks();
}

// Core of the migrating-threads BFS variant
// For each edge in the frontier, migrate to the home vertex
// If the dst vertex doesn't have a parent, set src as parent
// Then append to local queue for next frontier
// Using noinline to minimize the size of the migrating context
static noinline void
frontier_visitor(long src, long * edges_begin, long * edges_end)
{
    for (long * e = edges_begin; e < edges_end; ++e) {
        long dst = *e;
        // Look up the parent of the vertex we are visiting
        long * parent = &HYBRID_BFS.parent[dst];
        // If we are the first to visit this vertex
        if (*parent == -1L) {
            // Set self as parent of this vertex
            if (ATOMIC_CAS(parent, src, -1L) == -1L) {
                // Add it to the queue
                sliding_queue_push_back(&HYBRID_BFS.queue, dst);
            }
        }
    }
}

// Runs frontier_visitor over a range of edges in parallel
void
explore_frontier_parallel(long src, long * edges_begin, long * edges_end)
{
    long degree = edges_end - edges_begin;
    long grain = MY_LOCAL_GRAIN_MIN(degree, 128);
    if (degree <= grain) {
        // Low-degree local vertex, handle in this thread
        // TODO spawn here to separate from parent thread?
        frontier_visitor(src, edges_begin, edges_end);
    } else {
        // High-degree local vertex, spawn local threads
        for (long * e1 = edges_begin; e1 < edges_end; e1 += grain) {
            long * e2 = e1 + grain;
            if (e2 > edges_end) { e2 = edges_end; }
            cilk_spawn frontier_visitor(src, e1, e2);
        }
    }
}

// Calls explore_frontier_parallel over a remote edge block
void
explore_frontier_in_eb(long src, edge_block * eb)
{
    explore_frontier_parallel(src, eb->edges, eb->edges + eb->num_edges);
}

// Spawns threads to call frontier_visitor in parallel over a slice of the frontier
void
explore_frontier_spawner(long begin, long end, va_list args)
{
    // For each vertex in our slice of the queue...
    long * vertex_queue = va_arg(args, long*);
    for (long v = begin; v < end; ++v) {
        long src = vertex_queue[v];
        // How big is this vertex?
        if (is_heavy_out(src)) {
            // Heavy vertex, spawn a thread for each remote edge block
            edge_block * eb = G.vertex_out_neighbors[src].repl_edge_block;
            for (long i = 0; i < NODELETS(); ++i) {
                edge_block * remote_eb = mw_get_nth(eb, i);
                cilk_spawn_at(remote_eb) explore_frontier_in_eb(src, remote_eb);
            }
        } else {
            long * edges_begin = G.vertex_out_neighbors[src].local_edges;
            long * edges_end = edges_begin + G.vertex_out_degree[src];
            explore_frontier_parallel(src, edges_begin, edges_end);
        }
    }
}

// Spawn threads to call explore_frontier_worker on each vertex in the local queue
void
explore_local_frontier(sliding_queue * queue)
{
    emu_local_for(queue->start, queue->end, MY_LOCAL_GRAIN_MIN(queue->end - queue->start, 8),
        explore_frontier_spawner, queue->buffer
    );
}


void
dump_queue_stats()
{
    fprintf(stdout, "Frontier size per nodelet: ");
    for (long n = 0; n < NODELETS(); ++n) {
        sliding_queue * local_queue = mw_get_nth(&HYBRID_BFS.queue, n);
        fprintf(stdout, "%li ", local_queue->end - local_queue->start);
    }
    fprintf(stdout, "\n");
    fflush(stdout);

    fprintf(stdout, "Total out-degree per nodelet: ");
    for (long n = 0; n < NODELETS(); ++n) {
        long degree_sum = 0;
        sliding_queue * local_queue = mw_get_nth(&HYBRID_BFS.queue, n);
        for (long i = local_queue->start; i < local_queue->end; ++i) {
            long v = local_queue->buffer[i];
            degree_sum += G.vertex_out_degree[v];
        }
        fprintf(stdout, "%li ", degree_sum);
    }

    fprintf(stdout, "\n");
    fflush(stdout);
}

// For each vertex in the graph, detect if it was assigned a parent in this iteration
static void
populate_next_frontier(long * array, long begin, long end, va_list args)
{
    for (long i = begin; i < end; i += NODELETS()) {
        if (HYBRID_BFS.parent[i] == -1 && HYBRID_BFS.new_parent[i] != -1) {
            HYBRID_BFS.parent[i] = HYBRID_BFS.new_parent[i];
            sliding_queue_push_back(&HYBRID_BFS.queue, i);
        }
    }
}

void
bfs_dump()
{
    for (long v = 0; v < G.num_vertices; ++v) {
        long parent = HYBRID_BFS.parent[v];
        long new_parent = HYBRID_BFS.new_parent[v];
        if (parent != -1) {
            printf("parent[%li] = %li\n", v, parent);
        }
        if (new_parent != -1) {
            printf("new_parent[%li] = %li\n", v, new_parent);
        }
    }
}


static bool
search_for_parent(long child, long * edges_begin, long * edges_end)
{
    bool found_parent = false;
    // For each vertex connected to me...
    for (long * e = edges_begin; e < edges_end; ++e) {
        long parent = *e;
        // If the vertex is in the frontier...
        if (bitmap_get_bit(&HYBRID_BFS.frontier, parent)) {
            // Claim as a parent
            HYBRID_BFS.parent[child] = parent;
            found_parent = true;
            // Put myself in the frontier
            bitmap_set_bit(&HYBRID_BFS.next_frontier, child);
            // No need to keep looking for a parent
            break;
        }
    }
    return found_parent;
}

static inline void
search_for_parent_parallel(long child, long * edges_begin, long * edges_end)
{
    long degree = edges_end - edges_begin;
    long grain = MY_LOCAL_GRAIN_MIN(degree, 128);
    if (degree <= grain) {
        // Low-degree local vertex, handle in this thread
        search_for_parent(child, edges_begin, edges_end);
    } else {
        // High-degree local vertex, spawn local threads
        for (long * e1 = edges_begin; e1 < edges_end; e1 += grain) {
            long * e2 = e1 + grain;
            if (e2 > edges_end) { e2 = edges_end; }
            cilk_spawn search_for_parent(child, e1, e2);
        }
    }
}

// Calls search_for_parent_parallel over a remote edge block
void
search_for_parent_in_eb(long child, edge_block * eb)
{
    search_for_parent_parallel(child, eb->edges, eb->edges + eb->num_edges);
}

// Spawns threads to call search_for_parent in parallel for a part of the vertex list
void
search_for_parent_worker(long * array, long begin, long end, va_list args)
{
    // For each vertex in our slice of the queue...
    for (long v = begin; v < end; v += NODELETS()) {
        if (HYBRID_BFS.parent[v] > 0) {
            // How big is this vertex?
            if (is_heavy_in(v)) {
                // Heavy vertex, spawn a thread for each remote edge block
                edge_block * eb = G.vertex_in_neighbors[v].repl_edge_block;
                for (long i = 0; i < NODELETS(); ++i) {
                    edge_block * remote_eb = mw_get_nth(eb, i);
                    cilk_spawn_at(remote_eb) search_for_parent_in_eb(v, remote_eb);
                }
            } else {
                long * edges_begin = G.vertex_in_neighbors[v].local_edges;
                long * edges_end = edges_begin + G.vertex_in_degree[v];
                search_for_parent_parallel(v, edges_begin, edges_end);
            }
        }
    }
}

void
hybrid_bfs_bottom_up_step()
{
    // TODO we can do the clear in parallel with other stuff
    bitmap_replicated_clear(&HYBRID_BFS.next_frontier);
    emu_1d_array_apply((long*)&G.vertex_in_degree, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 64),
        search_for_parent_worker
    );
    // TODO we can start the sync early at each nodelet once all local vertices are done
    bitmap_replicated_sync(&HYBRID_BFS.next_frontier);
}

bool
hybrid_bfs_should_do_bottom_up()
{
    // FIXME
    return false;
}

bool
hybrid_bfs_should_keep_doing_bottom_up()
{
    // FIXME
    return false;
}



void
hybrid_bfs_run (long source)
{
    assert(source < G.num_vertices);

    // Start with the source vertex in the first frontier, at level 0, and mark it as visited
    sliding_queue_push_back(mw_get_nth(&HYBRID_BFS.queue, 0), source);
    sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
    HYBRID_BFS.parent[source] = source;

    // While there are vertices in the queue...
    while (!sliding_queue_all_empty(&HYBRID_BFS.queue)) {

        if (hybrid_bfs_should_do_bottom_up()) {
            // Convert sliding queue to bitmap
            hooks_region_begin("queue_to_bitmap");
            queue_to_bitmap(&HYBRID_BFS.queue, &HYBRID_BFS.frontier);
            hooks_region_end();
            long awake_count = sliding_queue_combined_size(&HYBRID_BFS.queue);
            sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
            // Do bottom-up steps for a while
            do {
                hooks_region_begin("bottom_up_step");
                hybrid_bfs_bottom_up_step();
                hooks_region_end();
                bitmap_swap(&HYBRID_BFS.frontier, &HYBRID_BFS.next_frontier);
            } while (hybrid_bfs_should_keep_doing_bottom_up());
            // Convert back to a queue
            hooks_region_begin("bitmap_to_queue");
            bitmap_to_queue(&HYBRID_BFS.frontier, &HYBRID_BFS.queue);
            hooks_region_end();

        } else {
            // Use the remote write implementation to explore the frontier
            if (HYBRID_BFS.use_remote_writes) {
                // Spawn a thread on each nodelet to process the local queue
                // For each neighbor, write your vertex ID to new_parent
                for (long n = 0; n < NODELETS(); ++n) {
                    sliding_queue * local_queue = mw_get_nth(&HYBRID_BFS.queue, n);
                    cilk_spawn mark_queue_neighbors(local_queue);
                }
                cilk_sync;
                // Add to the queue all vertices that didn't have a parent before
                emu_1d_array_apply(HYBRID_BFS.parent, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 256),
                    populate_next_frontier
                );
                // Spawn migrating threads to explore the frontier
            } else {
                // Spawn a thread on each nodelet to process the local queue
                // For each neighbor without a parent, add self as parent and append to queue
                for (long n = 0; n < NODELETS(); ++n) {
                    sliding_queue * local_queue = mw_get_nth(&HYBRID_BFS.queue, n);
                    cilk_spawn explore_local_frontier(local_queue);
                }
                cilk_sync;
            }
            // Slide all queues to explore the next frontier
            sliding_queue_slide_all_windows(&HYBRID_BFS.queue);
        }
    }
}

void
bfs_print_tree()
{
    for (long v = 0; v < G.num_vertices; ++v) {
        long parent = HYBRID_BFS.parent[v];
        long new_parent = HYBRID_BFS.new_parent[v];
        if (parent != -1) {
            printf("parent[%li] = %li\n", v, parent);
        }
        if (new_parent != -1) {
            printf("new_parent[%li] = %li\n", v, new_parent);
        }
    }
}

static void
compute_num_traversed_edges_worker(long * array, long begin, long end, long * partial_sum, va_list args)
{
    long local_sum = 0;
    const long nodelets = NODELETS();
    for (long v = begin; v < end; v += nodelets) {
        if (HYBRID_BFS.parent[v] >= 0) {
            local_sum += G.vertex_out_degree[v];
        }
    }
    REMOTE_ADD(partial_sum, local_sum);
}

long
bfs_count_num_traversed_edges()
{
    return emu_1d_array_reduce_sum(HYBRID_BFS.parent, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 256),
        compute_num_traversed_edges_worker
    );
}
