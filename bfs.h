#include "graph.h"

typedef struct sliding_queue
{
    // Next available slot in the queue
    long next;
    // Start and end of the current window
    long start;
    long end;
    // Index of the current window
    long window;
    // Storage for items in the queue
    long * buffer;
    // Starting positions of each window
    long * heads;
} sliding_queue;

// Switch to remote write algorithm
extern replicated long bfs_use_remote_writes;
// For each vertex, parent in the BFS tree.
extern replicated long * bfs_parent;
// Temporary copy of parent array
extern replicated long * bfs_new_parent;
// Used to store vertices to visit in the next frontier
extern replicated sliding_queue bfs_queue;


void bfs_run (long source);

