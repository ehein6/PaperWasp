## Overview 

PaperWasp is an implementation of breadth-first search (BFS) written in Cilk 
and optimized for the Emu architecture. It is intended to demonstrate both the
best performance and the best practices for writing good Emu C code, but be
warned that both of these tend to change as the system is developed. 

All of my graph benchmarks are named with a yellow jacket theme, to honor my
alma mater Georgia Tech. See also MeatBee (an early streaming graph benchmark
for Emu) and Beedrill (an upcoming C++ port of PaperWasp). 

## Building the code

Build for Emu hardware/simulator:
```
mkdir build-hw && cd build-hw
cmake .. \
-DCMAKE_TOOLCHAIN_FILE=../cmake/emu-toolchain.cmake
make -j4
```

Thanks to the `memoryweb_x86` header, it is also possible to build and run 
PaperWasp on an x86 system. This allows the use of a debugger and 
facilitates more rapid development.  

Build for testing on x86 (requires a Cilk-enabled compiler like gcc5):
```
mkdir build-x86 && cd build-x86
cmake .. \
-DCMAKE_BUILD_TYPE=Debug
make -j4
```


## Generating graph inputs

Source code for an input generator is provided in the `generator` subdirectory. 
This is a separate subproject, which uses C++/OpenMP and is meant to run on an 
x86 system.  

The `rmat_dataset_dump` binary accepts a single argument, the name of the graph
file to generate. Two formats are allowed:

* `A-B-C-D-num_edges-num_vertices.rmat`: Generates a random graph using the RMAT
algorithm, with input parameters A, B, C, D, and the specified number of edges 
and vertices. Suffixes K/M/G/T can be used in place of 2^10, 2^20, 2^30, 2^40.   
* `graph500-scaleN`: Generates a random graph suitable for running the graph500
benchmark at scale N (but see caveots below). Uses the RMAT algorithm with 
parameters A=0.57, B=0.19, C=0.19, D=0.05, num_edges=16*2^N, num_vertices=2^N. 

The graph generation algorithm benefits from multiple cores and uses a lot of 
memory. Be careful when generating graphs at scale greater than 20 on a personal 
computer or laptop. 


## Running the benchmark

Quick start: `./hybrid_bfs.mwx --alg beamer_hybrid --graph graph500-scale20`


## Command line arguments
```
--graph_filename     Path to graph file to load
--distributed_load   Load the graph from all nodes at once (File must exist on all nodes, use absolute path).
--heavy_threshold    Vertices with this many neighbors will be spread across nodelets
--num_trials         Run BFS this many times.
--source_vertex      Use this as the source vertex. If unspecified, pick random vertices.
--algorithm          Select BFS implementation to run
--alpha              Alpha parameter for direction-optimizing BFS
--beta               Beta parameter for direction-optimizing BFS
--sort_edge_blocks   Sort edge blocks to group neighbors by home nodelet.
--dump_edge_list     Print the edge list to stdout after loading (slow)
--check_graph        Validate the constructed graph against the edge list (slow)
--dump_graph         Print the graph to stdout after construction (slow)
--check_results      Validate the BFS results (slow)
--help               Print command line help
```
Note: command line arguments can be abbreviated as long as a unique 
prefix is used. So for example `--n` works in place of `--num_trials`.

## BFS algorithms

Four different BFS algorithms are implemented. They are based on three step 
types:

### Step types:
Top-down (with migrating threads): Threads migrate to visit each neighbor. 
Top-down (with remote writes): Threads mark each neighbor with remote writes, 
then scan the vertex list to find which vertices were added to the frontier.
Bottom-up: Threads scan the vertex list: for each unconnected vertex, 
migrate to each neighbor until a valid parent is found.

### Algorithm types:
- `migrating_threads`: All steps are top-down with migrating threads.
- `remote_writes`: All steps are top-down with remote writes.
- `beamer_hybrid`: Uses the direction-optimizing algorithm to switch between
top-down steps with migrating threads and bottom-up steps. See 
[Beamer2012](http://www.scottbeamer.net/pubs/beamer-sc2012.pdf) for more details. 
- `remote_writes_hybrid`: Does top-down steps with migrating threads until the
frontier grows large, then uses top-down steps with remote writes until 
 switching back to top-down with migrating threads. 
 Uses the same switching criterion as `beamer_hybrid`.

## [Graph500](http://graph500.org/)

This effort is optimized towards implementing Kernel 2 (BFS) of Graph500.
In order to simplify the task of graph construction, the generator 
removes duplicate edges, guarantees no self-edges, and permutes
the vertex ID space to ensure a more even data distribution. Thus the build
time is not a fair implementation of Kernel 1 (Graph construction).

To run Kernel2, use a graph500 input and set `--num_trials=64`. A different 
random source vertex will be automatically chosen for each trial, and the
aggregate performance will be reported at the end of the run.

Performance calculations assume a fixed clock rate on Emu (currently 175MHz). If
this changes, set `CORE_CLK_MHZ` in your environment or else results will be 
skewed.  


## Known issues

- The initial implementation allowed "heavy" vertices that could distribute their
edge lists across the system instead of using a local array. This feature is not
fully supported by all BFS implementations, and should be left disabled.
 