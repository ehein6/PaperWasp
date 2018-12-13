#pragma once

typedef struct sliding_queue
{
    // Next available slot in the queue
    long next;
    // Start and end of the current window
    long start;
    long end;
    // Index of the current window
    long window;
    // Storage for items in the queue
    long * buffer;
    // Starting positions of each window
    long * heads;
} sliding_queue;

static inline void
sliding_queue_replicated_reset(sliding_queue * self)
{
    mw_replicated_init(&self->next, 0);
    mw_replicated_init(&self->start, 0);
    mw_replicated_init(&self->end, 0);
    mw_replicated_init(&self->window, 0);
}

static inline void
sliding_queue_replicated_init(sliding_queue * self, long size)
{
    mw_replicated_init((long*)&self->buffer, (long)mw_mallocrepl(size * sizeof(long)));
    mw_replicated_init((long*)&self->heads, (long)mw_mallocrepl(size * sizeof(long)));
    sliding_queue_replicated_reset(self);
}

static inline void
sliding_queue_replicated_deinit(sliding_queue * self)
{
    mw_free(self->buffer);
    mw_free(self->heads);
}

static inline void
sliding_queue_slide_window(sliding_queue * self)
{
    self->start = self->window == 0 ? 0 : self->heads[self->window - 1];
    self->end = self->next;
    self->heads[self->window] = self->end;
    self->window += 1;
}

static inline void
sliding_queue_slide_all_windows(sliding_queue *self)
{
    for (long n = 0; n < NODELETS(); ++n) {
        sliding_queue * local = mw_get_nth(self, n);
        sliding_queue_slide_window(local);
    }
}

static inline void
sliding_queue_push_back(sliding_queue * self, long v)
{
    long pos = ATOMIC_ADDMS(&self->next, 1);
    self->buffer[pos] = v;
}

static inline bool
sliding_queue_is_empty(sliding_queue * self)
{
    return self->start == self->end;
}

static inline long
sliding_queue_size(sliding_queue * self)
{
    return self->end - self->start;
}

static inline bool
sliding_queue_all_empty(sliding_queue * self)
{
    for (long n = 0; n < NODELETS(); ++n) {
        sliding_queue * local = mw_get_nth(self, n);
        if (!sliding_queue_is_empty(local)) {
            return false;
        }
    }
    return true;
}

static inline long
sliding_queue_combined_size(sliding_queue * self)
{
    long size = 0;
    for (long n = 0; n < NODELETS(); ++n) {
        sliding_queue * local = mw_get_nth(self, n);
        REMOTE_ADD(&size, sliding_queue_size(local));
    }
    return size;
}
