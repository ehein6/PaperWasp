#include "graph.h"

typedef struct cursor
{
    // Pointer to current edge
    long * e;
    // Pointer to last edge in block
    long * end;
    // Pointer to current edge block (ignored for light vertex)
    edge_block * eb;
    // Index of current nodelet (ignored for light vertex)
    long nlet;
} cursor;

static inline void
cursor_init_out(cursor * c, long src)
{
    if (is_heavy_out(src)) {
        c->nlet = 0;
        c->eb = mw_get_nth(G.vertex_out_neighbors[src].repl_edge_block, 0);
        c->e = c->eb->edges;
        c->end = c->e + c->eb->num_edges;
    } else {
        c->eb = NULL;
        c->e = G.vertex_out_neighbors[src].local_edges;
        c->end = c->e + G.vertex_out_degree[src];
    }
}

static inline bool
cursor_valid(cursor * c)
{
    return c->e && c->e < c->end;
}

// Move to next edge
static inline void
cursor_next(cursor * c)
{
    if (!c->e) { return; }
    // Move to the next edge
    c->e++;
    // Check for end of array
    if (c->e >= c->end) {
        // If this was a light vertex, we're done
        if (!c->eb) {
            c->e = NULL;
            // If this was a heavy vertex, move to the next edge block
        } else {
            c->nlet++;
            if (c->nlet >= NODELETS()) {
                c->e = NULL;
                return;
            }
            edge_block * eb = mw_get_nth(c->eb, c->nlet);
            c->e = eb->edges;
            c->end = c->e + eb->num_edges;
        }
    }
}
