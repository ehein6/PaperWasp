#pragma once

// Helper functions

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <emu_c_utils/emu_c_utils.h>
#include <cilk/cilk.h>

// HACK so we can compile with old toolchain
#ifndef cilk_spawn_at
#define cilk_spawn_at(X) cilk_spawn
#endif

// Logging macro. Flush right away since Emu hardware usually doesn't
#define LOG(...) fprintf(stdout, __VA_ARGS__); fflush(stdout);

// Like mw_replicated_init, but for replicated long*
static inline void
replicated_init_ptr(long ** ptr, long * value)
{
    mw_replicated_init((long*)ptr, (long)value);
}

// Initializes all copies of a replicated long* with mw_malloc1dlong after checking the return code
static inline void
init_striped_array(long ** ptr, long n)
{
    long * tmp = mw_malloc1dlong((size_t)n);
    assert(tmp);
    replicated_init_ptr(ptr, tmp);
}

// Inlineable version of mw_get_nth
#ifndef __le64__
static inline void *
get_nth(void * repladdr, long n)
{ return repladdr; }
#else
static inline void *
get_nth(void * repladdr, long n)
{
    if (n < 0 || n >= NODELETS()) { return NULL; }
    unsigned long ptr = (unsigned long)repladdr;
    return (void*)(ptr
        // Change to view 1
        | (0x1UL<<__MW_VIEW_SHIFT__)
        // Set node number
        | (n <<__MW_NODE_BITS__)
        // Preserve shared bit
        | ((ptr) & __MW_VIEW_SHARED__)
    );
}
#endif
