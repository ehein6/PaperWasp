#include "bfs.h"
#include <stdlib.h>
#include <assert.h>
#include <cilk/cilk.h>
#include <emu_c_utils/emu_c_utils.h>
#include <stdio.h>

// Global replicated struct with BFS data pointers
replicated bfs_data BFS;

void sliding_queue_replicated_init(sliding_queue * self, long size);
void sliding_queue_replicated_deinit(sliding_queue * self);
void sliding_queue_replicated_reset(sliding_queue * self);
void sliding_queue_slide_window(sliding_queue * self);
void sliding_queue_slide_all_windows(sliding_queue *self);
void sliding_queue_push_back(sliding_queue * self, long v);
bool sliding_queue_is_empty(sliding_queue * self);

void
sliding_queue_replicated_init(sliding_queue * self, long size)
{
    mw_replicated_init((long*)&self->buffer, (long)mw_mallocrepl(size * sizeof(long)));
    mw_replicated_init((long*)&self->heads, (long)mw_mallocrepl(size * sizeof(long)));
    sliding_queue_replicated_reset(self);
}

void
sliding_queue_replicated_deinit(sliding_queue * self)
{
    mw_free(self->buffer);
    mw_free(self->heads);
}

void
sliding_queue_replicated_reset(sliding_queue * self)
{
    mw_replicated_init(&self->next, 0);
    mw_replicated_init(&self->start, 0);
    mw_replicated_init(&self->end, 0);
    mw_replicated_init(&self->window, 0);
}

void
sliding_queue_slide_window(sliding_queue * self)
{
    self->start = self->window == 0 ? 0 : self->heads[self->window - 1];
    self->end = self->next;
    self->heads[self->window] = self->end;
    self->window += 1;
}

void
sliding_queue_slide_all_windows(sliding_queue *self)
{
    for (long n = 0; n < NODELETS(); ++n) {
        sliding_queue * local = mw_get_nth(self, n);
        sliding_queue_slide_window(local);
    }
}


void
sliding_queue_push_back(sliding_queue * self, long v)
{
    long pos = ATOMIC_ADDMS(&self->next, 1);
    self->buffer[pos] = v;
}

bool
sliding_queue_is_empty(sliding_queue * self)
{
    return self->start == self->end;
}

long
sliding_queue_size(sliding_queue * self)
{
    return self->end - self->start;
}

bool
sliding_queue_all_empty(sliding_queue * self)
{
    for (long n = 0; n < NODELETS(); ++n) {
        sliding_queue * local = mw_get_nth(self, n);
        if (!sliding_queue_is_empty(local)) {
            return false;
        }
    }
    return true;
}

long
sliding_queue_combined_size(sliding_queue * self)
{
    long size = 0;
    for (long n = 0; n < NODELETS(); ++n) {
        sliding_queue * local = mw_get_nth(self, n);
        REMOTE_ADD(&size, sliding_queue_size(local));
    }
    return size;
}

static void
clear_worker(long * array, long begin, long end, va_list args)
{
    (void)array;
    const long nodelets = NODELETS();
    for (long i = begin; i < end; i += nodelets) {
        BFS.parent[i] = -1;
        BFS.new_parent[i] = -1;
    }
}

void
bfs_data_clear()
{
//    emu_1d_array_set_long(&self->level, -1);
//    emu_1d_array_set_long(&self->marks, 0);
    emu_1d_array_apply(BFS.parent, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 256),
        clear_worker
    );
    sliding_queue_replicated_reset(&BFS.queue);
}


void bfs_init(long use_remote_writes)
{
    mw_replicated_init(&BFS.use_remote_writes, use_remote_writes);
    init_striped_array(&BFS.parent, G.num_vertices);
    init_striped_array(&BFS.new_parent, G.num_vertices);
    sliding_queue_replicated_init(&BFS.queue, G.num_vertices);
    bfs_data_clear();
}

void
bfs_deinit()
{
    mw_free(BFS.parent);
    mw_free(BFS.new_parent);
    sliding_queue_replicated_deinit(&BFS.queue);
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
        BFS.new_parent[dst] = src; // Remote write
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
        if (is_heavy(src)) {
            // Heavy vertex, spawn a thread for each remote edge block
            edge_block * eb = G.vertex_neighbors[src].repl_edge_block;
            for (long i = 0; i < NODELETS(); ++i) {
                edge_block * remote_eb = mw_get_nth(eb, i);
                cilk_spawn_at(remote_eb) mark_neighbors_in_eb(src, remote_eb);
            }
        } else {
            long * edges_begin = G.vertex_neighbors[src].local_edges;
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
    emu_local_for(queue->start, queue->end, LOCAL_GRAIN_MIN(queue->end - queue->start, 8),
        mark_queue_neighbors_worker, queue->buffer
    );
}

static noinline void
frontier_visitor(long src, long * edges_begin, long * edges_end)
{
    for (long * e = edges_begin; e < edges_end; ++e) {
        long dst = *e;
        // Look up the parent of the vertex we are visiting
        long * parent = &BFS.parent[dst];
        // If we are the first to visit this vertex
        if (*parent == -1L) {
            // Set self as parent of this vertex
            if (ATOMIC_CAS(parent, -1L, src)) {
                // Add it to the queue
                sliding_queue_push_back(&BFS.queue, dst);
            }
        }
    }
}

void
explore_frontier_spawner(long begin, long end, va_list args)
{
    // For each vertex in our slice of the queue...
    long * vertex_queue = va_arg(args, long*);
    for (long v = begin; v < end; ++v) {
        long src = vertex_queue[v];
        // How big is this vertex?
        if (is_heavy(src)) {
            edge_block * eb = G.vertex_neighbors[src].repl_edge_block;
            // Spawn a thread for each remote edge block
            for (long i = 0; i < NODELETS(); ++i) {
                edge_block * remote_eb = mw_get_nth(eb, i);
                long * edges_begin = remote_eb->edges;
                long * edges_end = edges_begin + remote_eb->num_edges;
                /* cilk_spawn_at(remote_eb) */ frontier_visitor(src, edges_begin, edges_end);
            }
        } else {
            // Handle the local edge block in this thread
            // TODO may want to spawn some threads here
            long * edges_begin = G.vertex_neighbors[src].local_edges;
            long * edges_end = edges_begin + G.vertex_out_degree[src];
            frontier_visitor(src, edges_begin, edges_end);
        }
    }
}

// Spawn threads to call explore_frontier_worker on each vertex in the local queue
void
explore_local_frontier(sliding_queue * queue)
{
    emu_local_for(queue->start, queue->end, LOCAL_GRAIN(queue->end - queue->start),
        explore_frontier_spawner, queue->buffer
    );
}


void
dump_queue_stats()
{
    fprintf(stdout, "Frontier size per nodelet: ");
    for (long n = 0; n < NODELETS(); ++n) {
        sliding_queue * local_queue = mw_get_nth(&BFS.queue, n);
        fprintf(stdout, "%li ", local_queue->end - local_queue->start);
    }
    fprintf(stdout, "\n");
    fflush(stdout);

    fprintf(stdout, "Total out-degree per nodelet: ");
    for (long n = 0; n < NODELETS(); ++n) {
        long degree_sum = 0;
        sliding_queue * local_queue = mw_get_nth(&BFS.queue, n);
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
        if (BFS.parent[i] == -1 && BFS.new_parent[i] != -1) {
            BFS.parent[i] = BFS.new_parent[i];
            sliding_queue_push_back(&BFS.queue, i);
        }
    }
}

void
bfs_dump()
{
    for (long v = 0; v < G.num_vertices; ++v) {
        long parent = BFS.parent[v];
        long new_parent = BFS.new_parent[v];
        if (parent != -1) {
            printf("parent[%li] = %li\n", v, parent);
        }
        if (new_parent != -1) {
            printf("new_parent[%li] = %li\n", v, new_parent);
        }
    }
}

void
bfs_run (long source)
{
    assert(source < G.num_vertices);

    // Start with the source vertex in the first frontier, at level 0, and mark it as visited
    sliding_queue_push_back(mw_get_nth(&BFS.queue, 0), source);
    sliding_queue_slide_all_windows(&BFS.queue);
    BFS.parent[source] = source;

    // While there are vertices in the queue...
    while (!sliding_queue_all_empty(&BFS.queue)) {
        // If the queue is large, use the remote write implementation to explore the frontier
        if (BFS.use_remote_writes) {

            // Spawn a thread on each nodelet to process the local queue
            // For each neighbor, write your vertex ID to new_parent
            for (long n = 0; n < NODELETS(); ++n) {
                sliding_queue * local_queue = mw_get_nth(&BFS.queue, n);
                cilk_spawn mark_queue_neighbors(local_queue);
            }
            cilk_sync;

            // Add to the queue all vertices that didn't have a parent before
            emu_1d_array_apply(BFS.parent, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 256),
                populate_next_frontier
            );

            // If the queue is small, spawn migrating threads to explore the frontier
        } else {
            // Spawn a thread on each nodelet to process the local queue
            // For each neighbor without a parent, add self as parent and append to queue
            for (long n = 0; n < NODELETS(); ++n) {
                sliding_queue * local_queue = mw_get_nth(&BFS.queue, n);
                cilk_spawn explore_local_frontier(local_queue);
            }
            cilk_sync;
        }

        // dump_queue_stats();

        // Slide all queues to explore the next frontier
        sliding_queue_slide_all_windows(&BFS.queue);
    }
}

void
bfs_print_tree()
{
    for (long v = 0; v < G.num_vertices; ++v) {
        long parent = BFS.parent[v];
        long new_parent = BFS.new_parent[v];
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
        if (BFS.parent[v] >= 0) {
            local_sum += G.vertex_out_degree[v];
        }
    }
    REMOTE_ADD(partial_sum, local_sum);
}

long
bfs_count_num_traversed_edges()
{
    return emu_1d_array_reduce_sum(BFS.parent, G.num_vertices, GLOBAL_GRAIN_MIN(G.num_vertices, 256),
        compute_num_traversed_edges_worker
    );
}
