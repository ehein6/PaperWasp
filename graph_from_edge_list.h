#pragma once

#include "graph.h"

bool
check_graph();

void
construct_graph_from_edge_list(long heavy_threshold);

void
print_graph_distribution();

// Print graph to stdout for debugging
void dump_graph();
