#pragma once

#include "graph.h"
#include "sliding_queue.h"
#include "bitmap.h"
typedef struct hybrid_bfs_data {
    // Tracks the sum of the degrees of vertices in the frontier
    long scout_count;
    // For each vertex, parent in the BFS tree.
    long * parent;
    // Temporary copy of parent array
    long * new_parent;
    // Used to store vertices to visit in the next frontier
    sliding_queue queue;
    // Bitmap representation of the current frontier
    long * frontier;
    // Bitmap representation of the next frontier
    long * next_frontier;
} hybrid_bfs_data;

// Global replicated struct with BFS data pointers
extern replicated hybrid_bfs_data HYBRID_BFS;

typedef enum hybrid_bfs_alg {
    REMOTE_WRITES,
    MIGRATING_THREADS,
    REMOTE_WRITES_HYBRID,
    BEAMER_HYBRID,
} hybrid_bfs_alg;

void hybrid_bfs_init();
void hybrid_bfs_run(hybrid_bfs_alg alg, long source, long alpha, long beta);
long hybrid_bfs_count_num_traversed_edges();
bool hybrid_bfs_check(long source);
void hybrid_bfs_print_tree();
void hybrid_bfs_data_clear();
void hybrid_bfs_deinit();

