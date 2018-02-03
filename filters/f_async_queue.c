#include <pthread.h>

#include "audio/aframe.h"
#include "common/common.h"
#include "common/msg.h"
#include "osdep/atomic.h"

#include "f_async_queue.h"
#include "filter_internal.h"

struct mp_async_queue {
    // This is just a wrapper, so the API user can talloc_free() it, instead of
    // having to call a special unref function.
    struct async_queue *q;
};

struct async_queue {
    atomic_uint refcount;

    pthread_mutex_t lock;

    // -- protected by lock
    enum async_queue_unit unit; // unit used for max_size, frames_size, etc.
    int64_t  max_size;
    int64_t min_read_wakeup;
    int64_t min_write_wakeup;
    int64_t frames_size; // queue size in a chosen unit
    int num_frames;
    struct mp_frame *frames;
    int eof_count; // number of MP_FRAME_EOF in frames[], for draining
    struct mp_filter *conn[2]; // filters: in (0), out (1)
    bool signaled[2]; // wakeup sent (in/out)
    void (*full_notify_cb)(void *ctx);
    void *full_notify_cb_ctx;
};

static void clear_queue(struct async_queue *q)
{
    pthread_mutex_lock(&q->lock);
    for (int n = 0; n < q->num_frames; n++)
        mp_frame_unref(&q->frames[n]);
    q->num_frames = 0;
    q->eof_count = 0;
    q->frames_size = 0;
    for (int n = 0; n < 2; n++) {
        if (q->conn[n])
            mp_filter_wakeup(q->conn[n]);
    }
    pthread_mutex_unlock(&q->lock);
}

static void unref_queue(struct async_queue *q)
{
    if (!q)
        return;
    int count = atomic_fetch_add(&q->refcount, -1) - 1;
    assert(count >= 0);
    if (count == 0) {
        for (int n = 0; n < q->num_frames; n++)
            mp_frame_unref(&q->frames[n]);
        pthread_mutex_destroy(&q->lock);
        talloc_free(q);
    }
}

static void on_free_queue(void *p)
{
    struct mp_async_queue *q = p;
    unref_queue(q->q);
}

struct mp_async_queue *mp_async_queue_create(void)
{
    struct mp_async_queue *r = talloc_zero(NULL, struct mp_async_queue);
    r->q = talloc_zero(NULL, struct async_queue);
    *r->q = (struct async_queue){
        .refcount = ATOMIC_VAR_INIT(1),
        .max_size = 1,
        .min_read_wakeup = 1,
        .min_write_wakeup = 1,
    };
    pthread_mutex_init(&r->q->lock, NULL);
    talloc_set_destructor(r, on_free_queue);
    return r;
}

static int64_t frame_get_size(struct async_queue *q, struct mp_frame frame)
{
    int64_t res = 1;
    if (frame.type == MP_FRAME_AUDIO && q->unit == AQUEUE_UNIT_SAMPLES) {
        struct mp_aframe *aframe = frame.data;
        res = mp_aframe_get_size(aframe);
    }
    return res;
}

static bool is_full(struct async_queue *q)
{
    return q->frames_size >= q->max_size;
}

bool mp_async_queue_check_full(struct mp_async_queue *queue,
                               void (*notify_cb)(void *ctx), void *ctx)
{
    struct async_queue *q = queue->q;

    pthread_mutex_lock(&q->lock);
    q->full_notify_cb = NULL;
    bool full = is_full(q);
    if (!full) {
        q->full_notify_cb = notify_cb;
        q->full_notify_cb_ctx = ctx;
    }
    pthread_mutex_unlock(&q->lock);
    return full;
}

static void recompute_sizes(struct async_queue *q)
{
    q->frames_size = 0;
    for (int n = 0; n < q->num_frames; n++)
        q->frames_size += frame_get_size(q, q->frames[n]);
}

void mp_async_queue_set_queue_size(struct mp_async_queue *queue,
                                   enum async_queue_unit unit,
                                   int64_t max_size, int64_t min_read_wakeup,
                                   int64_t min_write_wakeup)
{
    struct async_queue *q = queue->q;

    assert(max_size > 0);
    assert(min_read_wakeup >= 1 && min_read_wakeup <= max_size);
    assert(min_write_wakeup >= 1 && min_write_wakeup <= max_size);

    pthread_mutex_lock(&q->lock);
    q->max_size = max_size;
    q->min_read_wakeup = min_read_wakeup;
    q->min_write_wakeup = min_write_wakeup;
    if (q->unit != unit) {
        q->unit = unit;
        recompute_sizes(q);
    }
    pthread_mutex_unlock(&q->lock);
}

void mp_async_queue_clear(struct mp_async_queue *queue)
{
    clear_queue(queue->q);
}

struct priv {
    struct async_queue *q;
};

static void destroy(struct mp_filter *f)
{
    struct priv *p = f->priv;
    struct async_queue *q = p->q;

    pthread_mutex_lock(&q->lock);
    for (int n = 0; n < 2; n++) {
        if (q->conn[n] == f)
            q->conn[n] = NULL;
    }
    pthread_mutex_unlock(&q->lock);

    unref_queue(q);
}

static void process_in(struct mp_filter *f)
{
    struct priv *p = f->priv;
    struct async_queue *q = p->q;

    pthread_mutex_lock(&q->lock);
    if (!is_full(q) && mp_pin_out_request_data(f->ppins[0])) {
        bool below_wakeup = q->frames_size < q->min_write_wakeup;
        struct mp_frame frame = mp_pin_out_read(f->ppins[0]);
        if (frame.type == MP_FRAME_EOF)
            q->eof_count += 1;
        q->frames_size += frame_get_size(q, frame);
        MP_TARRAY_INSERT_AT(q, q->frames, q->num_frames, 0, frame);
        // Notify reader that we have new frames.
//        MP_WARN(f, "w\n");
        if (q->conn[1] && !q->signaled[0]) {
            if ((below_wakeup && q->frames_size >= q->min_write_wakeup) ||
                q->eof_count)
            {
                mp_filter_wakeup(q->conn[1]);
//                MP_WARN(f, "w1\n");
            } else {
                mp_filter_mark_async_progress(q->conn[1]);
//                MP_WARN(f, "w2\n");
            }
            q->signaled[0] = true;
        }
        // We've actually done something, so new wakeups may be needed.
        q->signaled[1] = false;
        if ((is_full(q) || q->eof_count) && q->full_notify_cb) {
            q->full_notify_cb(q->full_notify_cb_ctx);
            q->full_notify_cb = NULL;
        }
        if (!is_full(q))
            mp_pin_out_request_data_next(f->ppins[0]);
    }
    pthread_mutex_unlock(&q->lock);
}

static void process_out(struct mp_filter *f)
{
    struct priv *p = f->priv;
    struct async_queue *q = p->q;

    pthread_mutex_lock(&q->lock);
    if (mp_pin_in_needs_data(f->ppins[0]) && q->num_frames) {
        struct mp_frame frame = q->frames[q->num_frames - 1];
        q->num_frames -= 1;
        if (frame.type == MP_FRAME_EOF)
            q->eof_count -= 1;
        q->frames_size -= frame_get_size(q, frame);
        assert(q->frames_size >= 0);
        mp_pin_in_write(f->ppins[0], frame);
        // Notify writer that we need new frames.
        if (q->conn[0] && !q->signaled[1]) {
            if (q->frames_size < q->min_read_wakeup) {
                mp_filter_wakeup(q->conn[0]);
            } else {
                mp_filter_mark_async_progress(q->conn[0]);
            }
            q->signaled[1] = true;
        }
        q->signaled[0] = false;
    }
    pthread_mutex_unlock(&q->lock);
}

static void reset(struct mp_filter *f)
{
    struct priv *p = f->priv;
    struct async_queue *q = p->q;

    clear_queue(q);
}

static const struct mp_filter_info info_in = {
    .name = "async_queue_in",
    .priv_size = sizeof(struct priv),
    .destroy = destroy,
    .process = process_in,
    .reset = reset,
};

static const struct mp_filter_info info_out = {
    .name = "async_queue_out",
    .priv_size = sizeof(struct priv),
    .destroy = destroy,
    .process = process_out,
    .reset = reset,
};

struct mp_filter *mp_async_queue_create_filter(struct mp_filter *parent,
                                               enum mp_pin_dir dir,
                                               struct mp_async_queue *queue)
{
    bool is_in = dir == MP_PIN_IN;
    assert(queue);

    struct mp_filter *f = mp_filter_create(parent, is_in ? &info_in : &info_out);
    if (!f)
        return NULL;

    struct priv *p = f->priv;

    struct async_queue *q = queue->q;

    mp_filter_add_pin(f, dir, is_in ? "in" : "out");

    atomic_fetch_add(&q->refcount, 1);
    p->q = q;

    pthread_mutex_lock(&q->lock);
    int slot = is_in ? 0 : 1;
    assert(!q->conn[slot]); // fails if already connected on this end
    q->conn[slot] = f;
    pthread_mutex_unlock(&q->lock);

    return f;
}
