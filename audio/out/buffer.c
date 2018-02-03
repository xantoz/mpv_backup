#include "audio/aframe.h"
#include "audio/format.h"
#include "common/msg.h"
#include "filters/f_async_queue.h"
#include "filters/filter.h"
#include "osdep/timer.h"

#include "internal.h"


void ao_buffer_create(struct ao *ao)
{
    pthread_mutex_init(&ao->buffer_lock, NULL);

    ao->in_queue = mp_async_queue_create();
    talloc_steal(ao, ao->in_queue);

    // Create the filter graph. Note that all filters must be essentially
    // realtime capable. They must not random amounts of data and filtering
    // must not take "too long", or there will be underruns.
    ao->root = mp_filter_create_root(ao->global);
    if (!ao->root)
        abort();
    talloc_steal(ao, ao->root);
    ao->in_filter = mp_async_queue_create_filter(ao->root, MP_PIN_OUT,
                                                 ao->in_queue);
    if (!ao->in_filter)
        abort();

    ao->in_pin = ao->in_filter->pins[0];

    mp_async_queue_set_queue_size(ao->in_queue, AQUEUE_UNIT_SAMPLES,
                                  ao->buffer, ao->buffer / 2, ao->buffer / 2);

    // TODO:
    // - we need this when getting "ready" (playloop is the only thing which
    //   would drive this, via ao_is_ready()).
    // - we don't need this during normal playback (pull AOs will just call
    //   ao_read_data regularly)
    mp_filter_root_set_wakeup_cb(ao->root, ao->wakeup_cb, ao->wakeup_ctx);
}

// The underlying queue, for which the caller is supposed to create a filter to
// connect with.
struct mp_async_queue *ao_get_queue(struct ao *ao)
{
    return ao->in_queue;
}

// Make sure ao->input is set and has at least 1 sample. In particular, this
// function clips the data against the start time. It also rechecks an already
// set input to ensure the start time is honored, and can modify or replace it.
// Must be called locked.
static void fill_buffer(struct ao *ao)
{
    while (1) {
        if (!ao->input) {
            struct mp_frame frame = mp_pin_out_read(ao->in_pin);
            // Note: no data usually happens on underruns or true EOF, because
            // the queue buffer connected to the filter should usually have
            // enough data ready.
            if (frame.type == MP_FRAME_EOF) {
                ao->input_eof = true;
                continue; // get more data to possibly close the gap
            } else if (frame.type == MP_FRAME_AUDIO) {
                ao->input = frame.data;
                // Lazy check - normally user has to recreate AO on changes.
                if (mp_aframe_get_format(ao->input) != ao->format ||
                    mp_aframe_get_channels(ao->input) != ao->channels.num)
                {
                    MP_ERR(ao, "incompatible format\n");
                    TA_FREEP(&ao->input);
                }
            } else if (frame.type) {
                MP_ERR(ao, "unexpected frame type\n");
                mp_frame_unref(&frame);
            }
            if (!ao->input)
                break;
        }

        if (ao->start_pts != MP_NOPTS_VALUE) {
            // We don't clip ao->end_pts here and instead do it in the buffer
            // read function (the frontend can unset or reduce the end_pts, and
            // also we shouldn't read&skip data forever if end_pts is reached).
            int samples = mp_aframe_get_size(ao->input);
            mp_aframe_clip_timestamps(ao->input, ao->start_pts, MP_NOPTS_VALUE);
            if (samples > 0 && samples == mp_aframe_get_size(ao->input))
                ao->start_pts = MP_NOPTS_VALUE; // disable as soon as reached
        }

        if (mp_aframe_get_size(ao->input))
            break;

        TA_FREEP(&ao->input);
    }

    if (ao->input)
        ao->input_eof = false;
}

// Return whether there is enough data in the internal buffer so that playback
// can be started safely.
bool ao_is_ready(struct ao *ao)
{
    pthread_mutex_lock(&ao->buffer_lock);
    bool r = ao->playing;
    if (!r) {
        // We don't want a situation where we start playback with 1 sample
        // queued and the AO will immediately underrun. So we're trying to start
        // playback with a full buffer, and hold it back until this is the case.
        fill_buffer(ao);
        r = mp_async_queue_check_full(ao->in_queue, ao->wakeup_cb, ao->wakeup_ctx)
            && (ao->input || ao->input_eof);
        MP_WARN(ao, "checkr %d %p\n", r, ao->input);
    }
    pthread_mutex_unlock(&ao->buffer_lock);
    return r;
}

// Start playback. Should be called only once ao_is_ready() returns true.
void ao_start_playing(struct ao *ao)
{
    pthread_mutex_lock(&ao->buffer_lock);
    if (!ao->playing && !ao->paused && ao->driver->resume) {
        MP_WARN(ao, "start\n");
        ao->driver->resume(ao);
    }
    ao->playing = true;
    pthread_mutex_unlock(&ao->buffer_lock);
}

// Set minimum PTS. This typically used for getting the correct position with
// hr-seek, or initial syncing to the video. This is disabled automatically once
// the position is reached. Setting it to a lower value than before will not
// lead to useful results (although unsetting is fone).
void ao_set_start_pts(struct ao *ao, double start)
{
    pthread_mutex_lock(&ao->buffer_lock);
    ao->start_pts = start;
    pthread_mutex_unlock(&ao->buffer_lock);
}

// Set maximum PTS. Usually used for playing a smaller range of a file. Changing
// this to a higher value than before is allowed (e.g. when unsetting A-B loops
// in the player), but can lead to messy results.
void ao_set_end_pts(struct ao *ao, double end)
{
    pthread_mutex_lock(&ao->buffer_lock);
    ao->end_pts = end;
    pthread_mutex_unlock(&ao->buffer_lock);
}

static int64_t us_to_samples(struct ao *ao, int64_t us)
{
    return us * ao->samplerate / 1000000LL;
}

static int64_t get_sample_time(struct ao *ao)
{
    return us_to_samples(ao, mp_time_us());
}

// Return the current actual playback position based on the current time.
static void update_time(struct ao *ao)
{
    int64_t now = get_sample_time(ao);

    // We assume time never goes backwards (if it does, just clamp the time). So
    // prune outdated ranges until the current position is reached. On the
    // other hand, if the only remaining segment is in the past, we keep that.
    while (ao->num_pts_ranges >= 2 && now >= ao->pts_ranges[1].start)
        MP_TARRAY_REMOVE_AT(ao->pts_ranges, ao->num_pts_ranges, 0);

    ao->last_pts = MP_NOPTS_VALUE;
    if (ao->num_pts_ranges) {
        struct pts_range *r = &ao->pts_ranges[0];
        int64_t t = MPCLAMP(now - r->start, 0, r->duration);
        if (r->pts != MP_NOPTS_VALUE)
            ao->last_pts = r->pts + t / (double)ao->samplerate * r->speed;
        //MP_WARN(ao, "r %ld %ld %ld %f %f -> %f\n", (long)now, (long)r->start, (long)r->duration,r->pts, r->speed, ao->last_pts);
    }
}

// Return the PTS of the sample that was played last. (To be treated as an
// approximation, since this will change with time anyway.) This uses source
// frame PTS, interpolated by current time and playback speed.
double ao_get_pts(struct ao *ao)
{
    pthread_mutex_lock(&ao->buffer_lock);
    update_time(ao);
    double pts = ao->last_pts;
    pthread_mutex_unlock(&ao->buffer_lock);
    return pts;
}

// Stop playback and empty buffers. Essentially go back to the state after
// ao->init().
// Note that this does not change the pause state.
void ao_reset(struct ao *ao)
{
    pthread_mutex_lock(&ao->buffer_lock);
    mp_filter_reset(ao->root);
    ao->playing = false;
    ao->num_pts_ranges = 0;
    TA_FREEP(&ao->input);
    ao->input_eof = false;
    ao->start_pts = MP_NOPTS_VALUE;
    ao->end_pts = MP_NOPTS_VALUE;
    ao->last_pts = MP_NOPTS_VALUE;
    ao->last_eof = false;
    if (ao->driver->reset)
        ao->driver->reset(ao);
    pthread_mutex_unlock(&ao->buffer_lock);
}

// Change pause state. If playback was not started, this merely sets the initial
// pause state.
void ao_set_paused(struct ao *ao, bool pause)
{
    pthread_mutex_lock(&ao->buffer_lock);
    if (ao->paused != pause) {
        ao->paused = pause;
        if(ao->playing) {
            if (pause && !ao->stream_silence && ao->driver->reset)
                ao->driver->reset(ao);
            if (!pause && ao->driver->resume)
                ao->driver->resume(ao);
        }
    }
    pthread_mutex_unlock(&ao->buffer_lock);
}

// encountered: EOF was passed on by decoder
// reached: EOF was passed on, and all remaining data was played
// recover: there is new data after an EOF
void ao_get_eof_status(struct ao *ao, bool *encountered, bool *reached,
                       bool *recover)
{
    pthread_mutex_lock(&ao->buffer_lock);
    update_time(ao);
    *reached = ao->playing && ao->last_eof;
    *encountered = ao->playing && ao->input_eof;
    *recover = false; // TODO: ???
    pthread_mutex_unlock(&ao->buffer_lock);
}

// *out_samples is set to the number of returned valid samples (if it's lower
// than samples, the rest is silence). Always in range [0, samples].
// *out_eof is set to true if the end of the stream was reached.
// *out_underrun is set to true if there was not enough input data.
// out_stime is in samples, with the base and current time determined by
// ao_driver.get_sample_time().
void ao_read_buffer(struct ao *ao, void **data, int samples, int64_t out_stime,
                    int *out_samples, bool *out_eof, bool *out_underrun)
{
    int samples_written = 0;
    int bytes_written = 0;
    bool got_eof = false;

    pthread_mutex_lock(&ao->buffer_lock);

    // Play silence if playback was not started yet or if we're paused.
    if (!ao->playing || ao->paused || !samples)
        goto end;

    while (samples_written < samples && !got_eof) {
        fill_buffer(ao);
        if (!ao->input)
            goto end; // EOF or underflow

        double pts = mp_aframe_get_pts(ao->input);
        int copy = mp_aframe_get_size(ao->input);
        copy = MPMIN(copy, samples - samples_written);

        if (ao->end_pts != MP_NOPTS_VALUE && pts != MP_NOPTS_VALUE) {
            double rate = mp_aframe_get_effective_rate(ao->input);
            int use = MPCLAMP((ao->end_pts - pts) * rate, 0, copy);
            if (use < copy) {
                got_eof = true;
                copy = use;
            }
        }

        uint8_t **src = mp_aframe_get_data_ro(ao->input);
        if (!src)
            abort();

        int copy_bytes = copy * ao->sstride;
        for (int n = 0; n < ao->num_planes; n++)
            memcpy((uint8_t *)data[n] + bytes_written, src[n], copy_bytes);

        if (copy > 0) {
            struct pts_range r = {
                .start = out_stime - (samples - samples_written),
                .duration = copy,
                .pts = pts,
                .speed = mp_aframe_get_speed(ao->input),
            };
            MP_TARRAY_APPEND(ao, ao->pts_ranges, ao->num_pts_ranges, r);
        }

        samples_written += copy;
        bytes_written += copy_bytes;
        mp_aframe_skip_samples(ao->input, copy);
    }

end:

    got_eof |= ao->input_eof;

    ao->last_eof = got_eof;

    *out_samples = samples_written;
    *out_eof = got_eof;
    *out_underrun = samples_written < samples && !got_eof && !ao->paused;

    pthread_mutex_unlock(&ao->buffer_lock);

    // pad with silence (underflow/paused/eof)
    int bytes_left = samples * ao->sstride - bytes_written;
    for (int n = 0; n < ao->num_planes; n++)
        af_fill_silence((char *)data[n] + bytes_written, bytes_left, ao->format);

    ao_post_process_data(ao, data, samples);
}

// Read the given amount of samples in the user-provided data buffer. Returns
// the number of samples copied. If there is not enough data (buffer underrun
// or EOF), return the number of samples that could be copied, and fill the
// rest of the user-provided buffer with silence.
// This basically assumes that the audio device doesn't care about underruns.
// If this is called in paused mode, it will always return 0.
// The caller should set out_time_us to the expected delay until the last sample
// reaches the speakers, in microseconds, using mp_time_us() as reference.
int ao_read_data(struct ao *ao, void **data, int samples, int64_t out_time_us)
{
    int out_samples;
    bool out_eof, out_underrun;
    ao_read_buffer(ao, data, samples, us_to_samples(ao, out_time_us),
                   &out_samples, &out_eof, &out_underrun);

    if (out_underrun)
        MP_WARN(ao, "Audio underflow by %d samples.\n", samples - out_samples);

    return out_samples;
}

// Same as ao_read_data(), but convert data according to *fmt.
// fmt->src_fmt and fmt->channels must be the same as the AO parameters.
int ao_read_data_converted(struct ao *ao, struct ao_convert_fmt *fmt,
                           void **data, int samples, int64_t out_time_us)
{
    void *ndata[MP_NUM_CHANNELS] = {0};

    if (!ao_need_conversion(fmt))
        return ao_read_data(ao, data, samples, out_time_us);

    assert(ao->format == fmt->src_fmt);
    assert(ao->channels.num == fmt->channels);

    bool planar = af_fmt_is_planar(fmt->src_fmt);
    int planes = planar ? fmt->channels : 1;
    int plane_samples = samples * (planar ? 1: fmt->channels);
    int src_plane_size = plane_samples * af_fmt_to_bytes(fmt->src_fmt);
    int dst_plane_size = plane_samples * fmt->dst_bits / 8;

    int needed = src_plane_size * planes;
    if (needed > talloc_get_size(ao->convert_buffer) || !ao->convert_buffer) {
        talloc_free(ao->convert_buffer);
        ao->convert_buffer = talloc_size(NULL, needed);
    }

    for (int n = 0; n < planes; n++)
        ndata[n] = ao->convert_buffer + n * src_plane_size;

    int res = ao_read_data(ao, ndata, samples, out_time_us);

    ao_convert_inplace(fmt, ndata, samples);
    for (int n = 0; n < planes; n++)
        memcpy(data[n], ndata[n], dst_plane_size);

    return res;
}

// Uninitialize and destroy the AO. Remaining audio must be dropped.
void ao_uninit(struct ao *ao)
{
    if (ao) {
        ao->driver->uninit(ao);
        talloc_free(ao->input);
        talloc_free(ao->convert_buffer);
    }
    talloc_free(ao);
}
