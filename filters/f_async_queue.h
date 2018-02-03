#pragma once

#include "filter.h"

// A thread safe queue, which buffers a configurable number of frames like a
// FIFO. It's part of the filter framework, and intended to provide such a
// queue between filters. Since a filter graph can't be used from multiple
// threads without synchronization, this provides 2 filters, which are
// implicitly connected. (This seemed much saner than having special thread
// safe mp_pins or such in the filter framework.)
struct mp_async_queue;

// Create a blank queue. Can be freed with talloc_free(). To use it, you need
// to create input and output filters with mp_async_queue_create_filter().
// Note that freeing it will only unref it. (E.g. you can free it once you've
// created the input and output filters.)
struct mp_async_queue *mp_async_queue_create(void);

// Clear all queued data. Naturally does not call reset on the access filters
// (but note that calling reset on the access filters clears the queue).
void mp_async_queue_clear(struct mp_async_queue *queue);

// Test whether the queue is full (buffered data reaches the maximum configured
// size). Return true if that is the case. Otherwise, return false and cause
// notify_cb(ctx) to be called once it changes.
// If there was a previous such call, the previous callback is unset and never
// called.
// If true is returned, the callback is never called.
bool mp_async_queue_check_full(struct mp_async_queue *queue,
                               void (*notify_cb)(void *ctx), void *ctx);

// Create a filter to access the queue, and connect it. It's not allowed to
// connect an already connected end of the queue. The filter can be freed at
// any time.
//  parent: filter graph the filter should be part of (or for standalone use,
//          create one with mp_filter_create_root())
//  dir: MP_PIN_IN for a filter to add to the queue, MP_PIN_OUT to read
//  queue: queue to attach to (which end of it depends on dir)
// The returned filter will have exactly 1 pin with the requested dir.
struct mp_filter *mp_async_queue_create_filter(struct mp_filter *parent,
                                               enum mp_pin_dir dir,
                                               struct mp_async_queue *queue);

enum async_queue_unit {
    AQUEUE_UNIT_FRAME, // a frame counts as 1
    AQUEUE_UNIT_SAMPLES, // number of audio samples
};

// Configure the queue size. By default, the queue size is 1. The sizes are in
// the given unit. The definition of "frame" in the comments below depends on
// the actual unit used.
//  max_size: queue size, maximum number of frames allowed to be buffered at a
//            time (if unit!=AQUEUE_UNIT_FRAME, can be overshot by the contents
//            of 1 mp_frame)
//  min_read_wakeup: if the current buffered amount of frames is below this
//                   number, cause the producer to add more frames. Must be in
//                   range [1, max_count].
//  min_write_wakeup: if the buffer is refilled due to min_read_wakeup, then
//                    don't wake up the consumer until this number of frames
//                    has been buffered. (The consumer can still read - only
//                    the wake up callback is skipped.) Must be in [1, max_count].
// The min_*_wakeup parameters can be used to avoid too frequent wakeups by
// delaying wakeups, and then making the producer to filter multiple frames at
// once.
// In all cases, the filters can still read/write if the producer/consumer got
// woken up by something else.
void mp_async_queue_set_queue_size(struct mp_async_queue *queue,
                                   enum async_queue_unit unit,
                                   int64_t max_size, int64_t min_read_wakeup,
                                   int64_t min_write_wakeup);
