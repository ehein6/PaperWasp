#pragma once

#ifdef __le64__
#include <memoryweb.h>
#else
#include <emu_c_utils/memoryweb_x86.h>
#define DISABLE_ACKS()
#define ENABLE_ACKS()
#endif

// One element on each nodelet
volatile replicated long * ack_control_data;

static inline void
ack_control_init()
{
    long * tmp = mw_malloc1dlong(NODELETS());
    assert(tmp);
    mw_replicated_init((long*)&ack_control_data, (long)tmp);
}

static inline void
ack_control_disable_acks()
{
    DISABLE_ACKS();
}

static inline void
ack_control_reenable_acks()
{
    // Re-enable ack's
    ENABLE_ACKS();
    // Do a remote write to each nodelet.
    // These will be queued behind all previous remotes
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        ack_control_data[nlet] = 1;
    }
    // Wait for them to complete
    FENCE();
}

