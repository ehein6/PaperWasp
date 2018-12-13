#include "graph.h"
#include "sliding_queue.h"

typedef struct bfs_data {
    // Switch to remote write algorithm
    long use_remote_writes;
    // For each vertex, parent in the BFS tree.
    long * parent;
    // Temporary copy of parent array
    long * new_parent;
    // Used to store vertices to visit in the next frontier
    sliding_queue queue;
} bfs_data;

// Global replicated struct with BFS data pointers
extern replicated bfs_data BFS;

void bfs_init(long use_remote_writes);
void bfs_run (long source);
void bfs_data_clear();
void bfs_deinit();

