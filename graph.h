#pragma once

#include <emu_c_utils/emu_c_utils.h>
#include "common.h"

typedef struct edge_block {
    long num_edges;
    long * edges;
} edge_block;

typedef union neighbors
{
    // Pointer to a local array of edges
    // Array size stored in vertex_out_degree[v]
    long * local_edges;
    // View-0 pointer to an edge block on each nodelet
    edge_block * repl_edge_block;
} neighbors;

// Global data structures
typedef struct graph {
    // Total number of edges in the graph
    long num_edges;
    // Total number of vertices in the graph (max vertex ID + 1)
    long num_vertices;

    // Distributed vertex array
    // number of neighbors for this vertex (on all nodelets)
    long * vertex_out_degree;
    long * vertex_in_degree;

    // Pointer to local edge array (light vertices only)
    // OR replicated edge block pointer (heavy vertices only)
    neighbors * vertex_out_neighbors;
    neighbors * vertex_in_neighbors;

    // Total number of edges stored on each nodelet
    long num_local_edges;
    // Pointer to stripe of memory where edges are stored
    long * edge_storage;
    // Pointer to un-reserved edge storage in local stripe
    long * next_edge_storage;

    long heavy_threshold;
} graph;

// Single global instance of the graph
extern replicated graph G;

static inline bool
is_heavy_out(long vertex_id)
{
    // TODO it would be great if this were a local query, maybe a replicated bitmap?
    return G.vertex_out_degree[vertex_id] >= G.heavy_threshold;
}

static inline bool
is_heavy_in(long vertex_id)
{
    return G.vertex_in_degree[vertex_id] >= G.heavy_threshold;
}
