#pragma once

#include "graph.h"
#include "sliding_queue.h"
#include "bitmap.h"
typedef struct hybrid_bfs_data {
    // Switch to remote write algorithm
    long use_remote_writes;
    // For each vertex, parent in the BFS tree.
    long * parent;
    // Temporary copy of parent array
    long * new_parent;
    // Used to store vertices to visit in the next frontier
    sliding_queue queue;
    // Bitmap representation of the current frontier
    bitmap frontier;
    // Bitmap representation of the next frontier
    bitmap next_frontier;
} hybrid_bfs_data;

// Global replicated struct with BFS data pointers
extern replicated hybrid_bfs_data BFS;

void hybrid_bfs_init(long use_remote_writes);
void hybrid_bfs_run (long source, long alpha, long beta);
long hybrid_bfs_count_num_traversed_edges();
bool hybrid_bfs_check(long source);
void hybrid_bfs_print_tree();
void hybrid_bfs_data_clear();
void hybrid_bfs_deinit();

