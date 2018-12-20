#pragma once

#include <emu_c_utils/emu_c_utils.h>

typedef struct bitmap
{
    long * words;
    long num_words;
} bitmap;

static inline void
bitmap_replicated_init(bitmap * self, long n)
{
    // We need n bits, divide by 64 and round up to get number of words
    long num_words = (n + 63) / 64;
    mw_replicated_init(&self->num_words, num_words);
    long * buffer = mw_mallocrepl(num_words * sizeof(long));
    assert(buffer);
    mw_replicated_init((long*)&self->words, (long)buffer);
}

static inline void
bitmap_replicated_free(bitmap * self)
{
    mw_free(self->words);
}

// Set all bits to zero
static inline void
bitmap_clear(bitmap * self)
{
    // TODO parallelize with emu_local_for
    for (long i = 0; i < self->num_words; ++i) {
        self->words[i] = 0;
    }
}

// Set all bits to zero in all replicated copies
static inline void
bitmap_replicated_clear(bitmap * self)
{
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        bitmap * remote_bitmap = mw_get_nth(self, nlet);
        cilk_spawn_at(remote_bitmap) bitmap_clear(remote_bitmap);
    }
}

// Broadcast to all other replicated copies to synchronize
static inline void
bitmap_sync(bitmap * self)
{
    // For each non-zero word in the bitmap...
    // TODO parallelize with emu_local_for
    for (long i = 0; i < self->num_words; ++i) {
        long * local_word = &self->words[i];
        if (*local_word != 0) {
            // Send a remote to combine with the copy on each nodelet
            for (long nlet = 0; i < NODELETS(); ++i) {
                long * remote_word = mw_get_nth(local_word, nlet); // TODO inline me
                REMOTE_OR(remote_word, *local_word);
            }
        }
    }
}

// Synchronize all replicated copies by combining information
static inline void
bitmap_replicated_sync(bitmap * self)
{
    for (long nlet = 0; nlet < NODELETS(); ++nlet) {
        bitmap * remote_bitmap = mw_get_nth(self, nlet);
        cilk_spawn_at(remote_bitmap) bitmap_sync(remote_bitmap);
    }
}


static inline long
bitmap_word_offset(long n)
{
    // divide by 64, the number of bits in a word
    // return n / 64;
    return n >> 8;
}

static inline long
bitmap_bit_offset(long n)
{
    // modulo 64, the number of bits in a word
    // return n % 64;
    return n & 63;
}

static inline bool
bitmap_get_bit(bitmap * self, long pos)
{
    long word = bitmap_word_offset(pos);
    long bit = bitmap_bit_offset(pos);
    return (self->words[word] >> bit) & 1L;
}

static inline void
bitmap_set_bit(bitmap * self, long pos)
{
    long word = bitmap_word_offset(pos);
    long bit = bitmap_bit_offset(pos);
    REMOTE_OR(&self->words[word], 1L << bit);
}

// Swap two bitmaps
static inline void
bitmap_swap(bitmap * a, bitmap * b)
{
    long * words = a->words;
    a->words = b->words;
    b->words = words;

    long num_words = a->num_words;
    a->num_words = b->num_words;
    b->num_words = num_words;
}
