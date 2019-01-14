#pragma once

#include <emu_c_utils/emu_c_utils.h>

typedef struct edge {
    long src;
    long dst;
} edge;

// Local edge list
typedef struct edge_list
{
    // Number of edges in the array
    long num_edges;
    // Number of vertices in the edge list;
    // All vertex ID's are guaranteed to be < num_vertices
    long num_vertices;
    // Pointer to local array of edges
    edge * edges;
} edge_list;

// Distributed edge list that the graph will be created from.
// First array stores source vertex ID, second array stores dest vertex ID
typedef struct dist_edge_list
{
    // Largest vertex ID + 1
    long num_vertices;
    // Length of both arrays
    long num_edges;
    // Striped array of source vertex ID's
    long * src;
    // Striped array of dest vertex ID's
    long * dst;
} dist_edge_list;

// Initializes the distributed edge list EL from the file
void load_edge_list(const char* filename);
// Initializes the distributed edge list EL
// Reads from all nodelets at once
void load_edge_list_distributed(const char* filename);

// Print the edge list to stdout for debugging
void dump_edge_list();

// Single global instance of the distributed edge list
extern replicated dist_edge_list EL;
