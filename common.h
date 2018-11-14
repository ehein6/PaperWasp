#pragma once

// Helper functions

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <emu_c_utils/emu_c_utils.h>

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
