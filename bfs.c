#include "bfs.h"
#include <stdlib.h>
#include <assert.h>
#include <cilk/cilk.h>
#include <emu_c_utils/emu_c_utils.h>
#include <stdio.h>

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
        bfs_parent[i] = -1;
        bfs_new_parent[i] = -1;
    }
}

void
bfs_data_clear()
{
//    emu_1d_array_set_long(&self->level, -1);
//    emu_1d_array_set_long(&self->marks, 0);
    emu_1d_array_apply(bfs_parent, num_vertices, GLOBAL_GRAIN_MIN(num_vertices, 256),
        clear_worker
    );
    sliding_queue_replicated_reset(&bfs_queue);
}


void bfs_data_replicated_init(long nv, long use_remote_writes)
{
    mw_replicated_init(&bfs_use_remote_writes, use_remote_writes);
    init_striped_array(&bfs_parent, num_vertices);
    init_striped_array(&bfs_new_parent, num_vertices);
    sliding_queue_replicated_init(&bfs_queue, num_vertices);
    bfs_data_clear();
}

void
bfs_data_replicated_deinit()
{
    mw_free(bfs_parent);
    mw_free(bfs_new_parent);
    sliding_queue_replicated_deinit(&bfs_queue);
}

// Each vertex in the queue tries to set itself as the parent of each of its vertex_neighbors
void
mark_neighbors_spawner(long begin, long end, va_list args)
{
    long * buffer = va_arg(args, long*);
    for (long i = begin; i < end; ++i) {
        // Get the local edge block
        long src = buffer[i];
        edge_block * eb = vertex_neighbors[src];
        // Fire off a remote write for each edge in the block
        // TODO can parallelize this loop
        for (long j = 0; j < eb->num_edges; ++j) {
            long dst = eb->edges[j];
            bfs_new_parent[dst] = src;
        }

    }
}

// Spawn threads to call mark_neighbors_worker for each vertex in the local queue
void
mark_queue_neighbors(sliding_queue * queue)
{
    emu_local_for(queue->start, queue->end, LOCAL_GRAIN(queue->end - queue->start),
        mark_neighbors_spawner, queue->buffer
    );
}

static inline void
frontier_visitor(long src, long dst)
{
    // Look up the parent of the vertex we are visiting
    long * parent = &bfs_parent[dst];
    // If we are the first to visit this vertex
    if (*parent == -1L) {
        // Set self as parent of this vertex
        if (ATOMIC_CAS(parent, -1L, src)) {
            // Add it to the queue
            sliding_queue_push_back(&bfs_queue, dst);
        }
    }
}

void
explore_frontier_spawner(long begin, long end, va_list args)
{
    long * buffer = va_arg(args, long *);
    // For each vertex in this thread's slice of the queue...
    for (long i = begin; i < end; ++i) {
        // Get the local edge block
        long src = buffer[i];
        edge_block * eb = vertex_neighbors[src];
        // Visit each edge in the block
        // TODO can parallelize this loop
        for (long j = 0; j < eb->num_edges; ++j) {
            long dst = eb->edges[j];
            frontier_visitor(src, dst);
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
        sliding_queue * local_queue = mw_get_nth(&bfs_queue, n);
        fprintf(stdout, "%li ", local_queue->end - local_queue->start);
    }
    fprintf(stdout, "\n");
    fflush(stdout);

    fprintf(stdout, "Total out-degree per nodelet: ");
    for (long n = 0; n < NODELETS(); ++n) {
        long degree_sum = 0;
        sliding_queue * local_queue = mw_get_nth(&bfs_queue, n);
        for (long i = local_queue->start; i < local_queue->end; ++i) {
            long v = local_queue->buffer[i];
            degree_sum += vertex_out_degree[v];
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
        if (bfs_parent[i] == -1 && bfs_new_parent[i] != -1) {
            bfs_parent[i] = bfs_new_parent[i];
            sliding_queue_push_back(&bfs_queue, i);
        }
    }
}

void
bfs_dump()
{
    for (long v = 0; v < num_vertices; ++v) {
        long parent = bfs_parent[v];
        long new_parent = bfs_new_parent[v];
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
    assert(source < num_vertices);

    // Start with the source vertex in the first frontier, at level 0, and mark it as visited
    sliding_queue_push_back(mw_get_nth(&bfs_queue, 0), source);
    sliding_queue_slide_all_windows(&bfs_queue);
    bfs_parent[source] = source;

    // While there are vertices in the queue...
    while (!sliding_queue_all_empty(&bfs_queue)) {
        // If the queue is large, use the remote write implementation to explore the frontier
        if (bfs_use_remote_writes) {

            // Spawn a thread on each nodelet to process the local queue
            // For each neighbor, write your vertex ID to new_parent
            for (long n = 0; n < NODELETS(); ++n) {
                sliding_queue * local_queue = mw_get_nth(&bfs_queue, n);
                cilk_spawn mark_queue_neighbors(local_queue);
            }
            cilk_sync;

            // Add to the queue all vertices that didn't have a parent before
            emu_1d_array_apply(bfs_parent, num_vertices, GLOBAL_GRAIN_MIN(num_vertices, 256),
                populate_next_frontier
            );

            // If the queue is small, spawn migrating threads to explore the frontier
        } else {
            // Spawn a thread on each nodelet to process the local queue
            // For each neighbor without a parent, add self as parent and append to queue
            for (long n = 0; n < NODELETS(); ++n) {
                sliding_queue * local_queue = mw_get_nth(&bfs_queue, n);
                cilk_spawn explore_local_frontier(local_queue);
            }
            cilk_sync;
        }

//        dump_queue_stats(self, graph);

        // Slide all queues to explore the next frontier
        sliding_queue_slide_all_windows(&bfs_queue);
    }
}

void
bfs_print_tree()
{
    for (long v = 0; v < num_vertices; ++v) {
        long parent = bfs_parent[v];
        long new_parent = bfs_new_parent[v];
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
        if (bfs_parent[v] >= 0) {
            local_sum += vertex_out_degree[v];
        }
    }
    REMOTE_ADD(partial_sum, local_sum);
}

long
bfs_count_num_traversed_edges()
{
    return emu_1d_array_reduce_sum(bfs_parent, num_vertices, GLOBAL_GRAIN_MIN(num_vertices, 256),
        compute_num_traversed_edges_worker
    );
}
