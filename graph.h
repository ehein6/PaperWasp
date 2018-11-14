#pragma once

#include <emu_c_utils/emu_c_utils.h>
#include "common.h"

typedef struct edge {
    long src;
    long dst;
} edge;

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

typedef struct edge_block {
    long num_edges;
    long * edges;
} edge_block;

extern replicated long num_edges;
extern replicated long num_vertices;

// Global data structures

// Distributed edge list that the graph will be created from.
// First array stores source vertex ID, second array stores dest vertex ID
extern replicated long * dist_edge_list_src;
extern replicated long * dist_edge_list_dst;

// Distributed vertex array
// number of vertex_neighbors for this vertex (in the en
extern replicated long * vertex_out_degree;
// Pointer to edge block for this vertex
// Light vertices: points to a local edge block
// Heavy vertices: points to a stripe
extern replicated edge_block ** vertex_neighbors;

// Total number of edges stored on each nodelet
extern replicated long num_local_edges;
// Pointer to stripe of memory where edges are stored
extern replicated long * edge_storage;
// Pointer to un-reserved edge storage in local stripe
extern replicated long * next_edge_storage;

extern replicated long heavy_threshold;

static inline bool
is_heavy(long vertex_id)
{
    // TODO it would be great if this were a local query, maybe a replicated bitmap?
    return vertex_out_degree[vertex_id] >= heavy_threshold;
}
