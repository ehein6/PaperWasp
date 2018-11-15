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

// Global data structures
typedef struct graph {
    // Distributed edge list that the graph will be created from.
    // First array stores source vertex ID, second array stores dest vertex ID
    long * dist_edge_list_src;
    long * dist_edge_list_dst;

    // Total number of edges in the graph
    long num_edges;
    // Total number of vertices in the graph (max vertex ID + 1)
    long num_vertices;

    // Distributed vertex array
    // number of neighbors for this vertex (on all nodelets)
    long * vertex_out_degree;
    // Pointer to edge block for this vertex
    // Light vertices: points to a local edge block
    // Heavy vertices: points to a stripe
    edge_block ** vertex_neighbors;

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
is_heavy(long vertex_id)
{
    // TODO it would be great if this were a local query, maybe a replicated bitmap?
    return G.vertex_out_degree[vertex_id] >= G.heavy_threshold;
}
