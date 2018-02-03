/*
 * This file is part of mpv.
 *
 * mpv is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * mpv is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with mpv.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <stddef.h>
#include <stdbool.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <assert.h>

#include "config.h"
#include "mpv_talloc.h"

#include "common/msg.h"
#include "common/encode.h"
#include "options/options.h"
#include "common/common.h"
#include "osdep/timer.h"

#include "audio/format.h"
#include "audio/out/ao.h"
#include "demux/demux.h"
#include "filters/f_decoder_wrapper.h"

#include "core.h"
#include "command.h"

enum {
    AD_OK = 0,
    AD_EOF = -2,
    AD_WAIT = -4,
};

// Try to reuse the existing filters to change playback speed. If it works,
// return true; if filter recreation is needed, return false.
static void update_speed_filters(struct MPContext *mpctx)
{
    struct ao_chain *ao_c = mpctx->ao_chain;
    if (!ao_c)
        return;

    double speed = mpctx->opts->playback_speed;
    double resample = mpctx->speed_factor_a;

    if (!mpctx->opts->pitch_correction) {
        resample *= speed;
        speed = 1.0;
    }

    mp_output_chain_set_audio_speed(ao_c->filter, speed, resample);
}

static int recreate_audio_filters(struct MPContext *mpctx)
{
    struct ao_chain *ao_c = mpctx->ao_chain;
    assert(ao_c);

    if (!mp_output_chain_update_filters(ao_c->filter, mpctx->opts->af_settings))
        goto fail;

    update_speed_filters(mpctx);

    mp_notify(mpctx, MPV_EVENT_AUDIO_RECONFIG, NULL);

    return 0;

fail:
    MP_ERR(mpctx, "Audio filter initialized failed!\n");
    return -1;
}

int reinit_audio_filters(struct MPContext *mpctx)
{
    struct ao_chain *ao_c = mpctx->ao_chain;
    if (!ao_c)
        return 0;

    double delay = mp_output_get_measured_total_delay(ao_c->filter);

    if (recreate_audio_filters(mpctx) < 0)
        return -1;

    double ndelay = mp_output_get_measured_total_delay(ao_c->filter);

    // Only force refresh if the amount of dropped buffered data is going to
    // cause "issues" for the A/V sync logic.
    if (mpctx->audio_status == STATUS_PLAYING && delay - ndelay >= 0.2)
        issue_refresh_seek(mpctx, MPSEEK_EXACT);
    return 1;
}

static double db_gain(double db)
{
    return pow(10.0, db/20.0);
}

static float compute_replaygain(struct MPContext *mpctx)
{
    struct MPOpts *opts = mpctx->opts;

    float rgain = 1.0;

    struct replaygain_data *rg = NULL;
    struct track *track = mpctx->current_track[0][STREAM_AUDIO];
    if (track)
        rg = track->stream->codec->replaygain_data;
    if (opts->rgain_mode && rg) {
        MP_VERBOSE(mpctx, "Replaygain: Track=%f/%f Album=%f/%f\n",
                   rg->track_gain, rg->track_peak,
                   rg->album_gain, rg->album_peak);

        float gain, peak;
        if (opts->rgain_mode == 1) {
            gain = rg->track_gain;
            peak = rg->track_peak;
        } else {
            gain = rg->album_gain;
            peak = rg->album_peak;
        }

        gain += opts->rgain_preamp;
        rgain = db_gain(gain);

        MP_VERBOSE(mpctx, "Applying replay-gain: %f\n", rgain);

        if (!opts->rgain_clip) { // clipping prevention
            rgain = MPMIN(rgain, 1.0 / peak);
            MP_VERBOSE(mpctx, "...with clipping prevention: %f\n", rgain);
        }
    } else if (opts->rgain_fallback) {
        rgain = db_gain(opts->rgain_fallback);
        MP_VERBOSE(mpctx, "Applying fallback gain: %f\n", rgain);
    }

    return rgain;
}

// Called when opts->softvol_volume or opts->softvol_mute were changed.
void audio_update_volume(struct MPContext *mpctx)
{
    struct MPOpts *opts = mpctx->opts;
    struct ao_chain *ao_c = mpctx->ao_chain;
    if (!ao_c || !ao_c->ao)
        return;

    float gain = MPMAX(opts->softvol_volume / 100.0, 0);
    gain = pow(gain, 3);
    gain *= compute_replaygain(mpctx);
    if (opts->softvol_mute == 1)
        gain = 0.0;

    ao_set_gain(ao_c->ao, gain);
}

// Call this if opts->playback_speed or mpctx->speed_factor_* change.
void update_playback_speed(struct MPContext *mpctx)
{
    mpctx->audio_speed = mpctx->opts->playback_speed * mpctx->speed_factor_a;
    mpctx->video_speed = mpctx->opts->playback_speed * mpctx->speed_factor_v;

    update_speed_filters(mpctx);
}

static void ao_chain_reset_state(struct ao_chain *ao_c)
{
    ao_c->last_out_pts = MP_NOPTS_VALUE;
    TA_FREEP(&ao_c->output_frame);
    ao_c->out_eof = false;

    //TODO:
    //reset ao buffer, but possibly not ao?
}

void reset_audio_state(struct MPContext *mpctx)
{
    if (mpctx->ao_chain)
        ao_chain_reset_state(mpctx->ao_chain);
    mpctx->audio_status = mpctx->ao_chain ? STATUS_SYNCING : STATUS_EOF;
    mpctx->audio_drop_throttle = 0;
}

void uninit_audio_out(struct MPContext *mpctx)
{
    if (mpctx->ao) {
        // TODO: this will actually eat audio from the last file's end
        ao_uninit(mpctx->ao);

        mp_notify(mpctx, MPV_EVENT_AUDIO_RECONFIG, NULL);
    }
    mpctx->ao = NULL;
    TA_FREEP(&mpctx->ao_filter_fmt);
}

static void ao_chain_uninit(struct ao_chain *ao_c)
{
    struct track *track = ao_c->track;
    if (track) {
        assert(track->ao_c == ao_c);
        track->ao_c = NULL;
        if (ao_c->dec_src)
            assert(track->dec->f->pins[0] == ao_c->dec_src);
        talloc_free(track->dec->f);
        track->dec = NULL;
    }

    if (ao_c->filter_src)
        mp_pin_disconnect(ao_c->filter_src);

    talloc_free(ao_c->filter->f);
    talloc_free(ao_c->output_frame);
    talloc_free(ao_c);
}

void uninit_audio_chain(struct MPContext *mpctx)
{
    if (mpctx->ao_chain) {
        ao_chain_uninit(mpctx->ao_chain);
        mpctx->ao_chain = NULL;

        mpctx->audio_status = STATUS_EOF;

        mp_notify(mpctx, MPV_EVENT_AUDIO_RECONFIG, NULL);
    }
}

static char *audio_config_to_str_buf(char *buf, size_t buf_sz, int rate,
                                     int format, struct mp_chmap channels)
{
    char ch[128];
    mp_chmap_to_str_buf(ch, sizeof(ch), &channels);
    char *hr_ch = mp_chmap_to_str_hr(&channels);
    if (strcmp(hr_ch, ch) != 0)
        mp_snprintf_cat(ch, sizeof(ch), " (%s)", hr_ch);
    snprintf(buf, buf_sz, "%dHz %s %dch %s", rate,
             ch, channels.num, af_fmt_to_str(format));
    return buf;
}

static void reinit_audio_filters_and_output(struct MPContext *mpctx)
{
    struct MPOpts *opts = mpctx->opts;
    struct ao_chain *ao_c = mpctx->ao_chain;
    assert(ao_c);
    struct track *track = ao_c->track;

    if (!ao_c->filter->ao_needs_update)
        return;

    TA_FREEP(&ao_c->output_frame); // stale?

    // The "ideal" filter output format
    struct mp_aframe *out_fmt = mp_aframe_new_ref(ao_c->filter->output_aformat);
    if (!out_fmt)
        abort();

    if (!mp_aframe_config_is_valid(out_fmt)) {
        talloc_free(out_fmt);
        goto init_error;
    }

    if (af_fmt_is_pcm(mp_aframe_get_format(out_fmt))) {
        if (opts->force_srate)
            mp_aframe_set_rate(out_fmt, opts->force_srate);
        if (opts->audio_output_format)
            mp_aframe_set_format(out_fmt, opts->audio_output_format);
        if (opts->audio_output_channels.num_chmaps == 1)
            mp_aframe_set_chmap(out_fmt, &opts->audio_output_channels.chmaps[0]);
    }

    // Weak gapless audio: if the filter output format is the same as the
    // previous one, keep the AO and don't reinit anything.
    // Strong gapless: always keep the AO
    if ((mpctx->ao_filter_fmt && mpctx->ao && opts->gapless_audio < 0 &&
         mp_aframe_config_equals(mpctx->ao_filter_fmt, out_fmt)) ||
        (mpctx->ao && opts->gapless_audio > 0))
    {
        mp_output_chain_set_ao(ao_c->filter, mpctx->ao);
        talloc_free(out_fmt);
        return;
    }

    // Potentially wait until current audio has been finished.
    if (mpctx->ao) {
        bool encountered, reached, recover;
        ao_get_eof_status(ao_c->ao, &encountered, &reached, &recover);
        if (reached) {
            // The AO will wake up the main thread once the EOF state changes,
            // so just exit for now.
            talloc_free(out_fmt);
            return;
        }
    }

    uninit_audio_out(mpctx);

    int out_rate = mp_aframe_get_rate(out_fmt);
    int out_format = mp_aframe_get_format(out_fmt);
    struct mp_chmap out_channels = {0};
    mp_aframe_get_chmap(out_fmt, &out_channels);

    int ao_flags = 0;
    bool spdif_fallback = af_fmt_is_spdif(out_format) &&
                          ao_c->spdif_passthrough;

    if (opts->ao_null_fallback && !spdif_fallback)
        ao_flags |= AO_INIT_NULL_FALLBACK;

    if (opts->audio_stream_silence)
        ao_flags |= AO_INIT_STREAM_SILENCE;

    if (opts->audio_exclusive)
        ao_flags |= AO_INIT_EXCLUSIVE;

    if (af_fmt_is_pcm(out_format)) {
        if (!opts->audio_output_channels.set ||
            opts->audio_output_channels.auto_safe)
            ao_flags |= AO_INIT_SAFE_MULTICHANNEL_ONLY;

        mp_chmap_sel_list(&out_channels,
                          opts->audio_output_channels.chmaps,
                          opts->audio_output_channels.num_chmaps);
    }

    mpctx->ao_filter_fmt = out_fmt;

    mpctx->ao = ao_init_best(mpctx->global, ao_flags, mp_wakeup_core_cb,
                             mpctx, mpctx->encode_lavc_ctx, out_rate,
                             out_format, out_channels);
    ao_c->ao = mpctx->ao;

    int ao_rate = 0;
    int ao_format = 0;
    struct mp_chmap ao_channels = {0};
    if (mpctx->ao)
        ao_get_format(mpctx->ao, &ao_rate, &ao_format, &ao_channels);

    // Verify passthrough format was not changed.
    if (mpctx->ao && af_fmt_is_spdif(out_format)) {
        if (out_rate != ao_rate || out_format != ao_format ||
            !mp_chmap_equals(&out_channels, &ao_channels))
        {
            MP_ERR(mpctx, "Passthrough format unsupported.\n");
            ao_uninit(mpctx->ao);
            mpctx->ao = NULL;
            ao_c->ao = NULL;
        }
    }

    if (!mpctx->ao) {
        // If spdif was used, try to fallback to PCM.
        if (spdif_fallback && ao_c->track && ao_c->track->dec) {
            MP_VERBOSE(mpctx, "Falling back to PCM output.\n");
            ao_c->spdif_passthrough = false;
            ao_c->spdif_failed = true;
            ao_c->track->dec->try_spdif = false;
            if (!mp_decoder_wrapper_reinit(ao_c->track->dec))
                goto init_error;
            reset_audio_state(mpctx);
            mp_output_chain_reset_harder(ao_c->filter);
            mp_wakeup_core(mpctx); // reinit with new format next time
            return;
        }

        MP_ERR(mpctx, "Could not open/initialize audio device -> no sound.\n");
        mpctx->error_playing = MPV_ERROR_AO_INIT_FAILED;
        goto init_error;
    }

    char tmp[80];
    MP_INFO(mpctx, "AO: [%s] %s\n", ao_get_name(mpctx->ao),
            audio_config_to_str_buf(tmp, sizeof(tmp), ao_rate, ao_format,
                                    ao_channels));
    MP_VERBOSE(mpctx, "AO: Description: %s\n", ao_get_description(mpctx->ao));
    update_window_title(mpctx, true);

    ao_c->ao_resume_time =
        opts->audio_wait_open > 0 ? mp_time_sec() + opts->audio_wait_open : 0;

    mp_output_chain_set_ao(ao_c->filter, mpctx->ao);

    audio_update_volume(mpctx);

    mp_notify(mpctx, MPV_EVENT_AUDIO_RECONFIG, NULL);
    mp_wakeup_core(mpctx);
    return;

init_error:
    uninit_audio_chain(mpctx);
    uninit_audio_out(mpctx);
    error_on_track(mpctx, track);
}

int init_audio_decoder(struct MPContext *mpctx, struct track *track)
{
    assert(!track->dec);
    if (!track->stream)
        goto init_error;

    track->dec = mp_decoder_wrapper_create(mpctx->filter_root, track->stream);
    if (!track->dec)
        goto init_error;

    if (track->ao_c)
        track->dec->try_spdif = true;

    if (!mp_decoder_wrapper_reinit(track->dec))
        goto init_error;

    return 1;

init_error:
    if (track->sink)
        mp_pin_disconnect(track->sink);
    track->sink = NULL;
    error_on_track(mpctx, track);
    return 0;
}

void reinit_audio_chain(struct MPContext *mpctx)
{
    struct track *track = NULL;
    track = mpctx->current_track[0][STREAM_AUDIO];
    if (!track || !track->stream) {
        uninit_audio_out(mpctx);
        error_on_track(mpctx, track);
        return;
    }
    reinit_audio_chain_src(mpctx, track);
}

// (track=NULL creates a blank chain, used for lavfi-complex)
void reinit_audio_chain_src(struct MPContext *mpctx, struct track *track)
{
    assert(!mpctx->ao_chain);

    mp_notify(mpctx, MPV_EVENT_AUDIO_RECONFIG, NULL);

    struct ao_chain *ao_c = talloc_zero(NULL, struct ao_chain);
    mpctx->ao_chain = ao_c;
    ao_c->log = mpctx->log;
    ao_c->filter =
        mp_output_chain_create(mpctx->filter_root, MP_OUTPUT_CHAIN_AUDIO);
    ao_c->spdif_passthrough = true;
    ao_c->last_out_pts = MP_NOPTS_VALUE;
    ao_c->ao = mpctx->ao;

    if (track) {
        ao_c->track = track;
        track->ao_c = ao_c;
        if (!init_audio_decoder(mpctx, track))
            goto init_error;
        ao_c->dec_src = track->dec->f->pins[0];
        mp_pin_connect(ao_c->filter->f->pins[0], ao_c->dec_src);
    }

    reset_audio_state(mpctx);

    if (recreate_audio_filters(mpctx) < 0)
        goto init_error;

    audio_update_volume(mpctx);

    mp_wakeup_core(mpctx);
    return;

init_error:
    uninit_audio_chain(mpctx);
    uninit_audio_out(mpctx);
    error_on_track(mpctx, track);
}

// Return pts value corresponding to currently playing audio.
double playing_audio_pts(struct MPContext *mpctx)
{
    struct ao_chain *ao_c = mpctx->ao_chain;
    if (!ao_c || !ao_c->ao)
        return MP_NOPTS_VALUE;

    return ao_get_pts(ao_c->ao);
}

void reload_audio_output(struct MPContext *mpctx)
{
    if (!mpctx->ao)
        return;

    ao_reset(mpctx->ao);
    uninit_audio_out(mpctx);
    reinit_audio_filters(mpctx); // mostly to issue refresh seek

    struct ao_chain *ao_c = mpctx->ao_chain;

    if (ao_c) {
        reset_audio_state(mpctx);
        mp_output_chain_reset_harder(ao_c->filter);
    }

    // Whether we can use spdif might have changed. If we failed to use spdif
    // in the previous initialization, try it with spdif again (we'll fallback
    // to PCM again if necessary).
    if (ao_c && ao_c->track) {
        struct mp_decoder_wrapper *dec = ao_c->track->dec;
        if (dec && ao_c->spdif_failed) {
            ao_c->spdif_passthrough = true;
            ao_c->spdif_failed = false;
            dec->try_spdif = true;
            if (!mp_decoder_wrapper_reinit(dec)) {
                MP_ERR(mpctx, "Error reinitializing audio.\n");
                error_on_track(mpctx, ao_c->track);
            }
        }
    }

    mp_wakeup_core(mpctx);
}

void fill_audio_out_buffers(struct MPContext *mpctx)
{
    struct MPOpts *opts = mpctx->opts;

    if (mpctx->ao && ao_query_and_reset_events(mpctx->ao, AO_EVENT_RELOAD))
        reload_audio_output(mpctx);

    struct ao_chain *ao_c = mpctx->ao_chain;
    if (!ao_c)
        return;

    if (ao_c->filter->failed_output_conversion) {
        error_on_track(mpctx, ao_c->track);
        return;
    }

    // (if AO is set due to gapless from previous file, then we can try to
    // filter normally until the filter tells us to change the AO)
    if (!mpctx->ao) {
        // Probe the initial audio format.
        mp_pin_out_request_data(ao_c->filter->f->pins[1]);
        reinit_audio_filters_and_output(mpctx);
        return; // try again next iteration
    }

    if (ao_c->filter->ao_needs_update) {
        reinit_audio_filters_and_output(mpctx);
        return; // retry on next iteration
    }

    if (ao_c->ao_resume_time > mp_time_sec()) {
        double remaining = ao_c->ao_resume_time - mp_time_sec();
        mp_set_timeout(mpctx, remaining);
        return;
    }

    if (mpctx->vo_chain && ao_c->track && ao_c->track->dec &&
        ao_c->track->dec->pts_reset)
    {
        MP_VERBOSE(mpctx, "Reset playback due to audio timestamp reset.\n");
        reset_playback_state(mpctx);
        mp_wakeup_core(mpctx);
        return;
    }

    if (mpctx->audio_status == STATUS_SYNCING) {
        double sync_pts = MP_NOPTS_VALUE;

        bool sync_to_video = mpctx->vo_chain && !mpctx->vo_chain->is_coverart &&
                             mpctx->video_status != STATUS_EOF;
        if (!opts->initial_audio_sync) {
            /* disable */
        } else if (sync_to_video) {
            if (mpctx->video_status < STATUS_READY)
                return; // wait until we know a video PTS
            if (mpctx->video_pts != MP_NOPTS_VALUE)
                sync_pts = mpctx->video_pts - opts->audio_delay;
        } else if (mpctx->hrseek_active) {
            sync_pts = mpctx->hrseek_pts;
        } else {
            // If audio is enabled mid-stream during playback, sync accordingly.
            sync_pts = mpctx->playback_pts;
        }

        ao_set_start_pts(ao_c->ao, sync_pts);
    }

    ao_set_end_pts(ao_c->ao, get_play_end_pts(mpctx));

    if (mpctx->audio_status < STATUS_PLAYING) {
        if (!ao_is_ready(ao_c->ao))
            return;
        if (mpctx->audio_status != STATUS_READY) {
            mpctx->audio_status = STATUS_READY;
            mp_wakeup_core(mpctx);
        }
        return;
    }

    mpctx->shown_aframes = true;

#if 0
    int skip_duplicate = 0; // >0: skip, <0: duplicate
    double drop_limit =
        (opts->sync_max_audio_change + opts->sync_max_video_change) / 100;
    if (mpctx->display_sync_active && opts->video_sync == VS_DISP_ADROP &&
        fabs(mpctx->last_av_difference) >= opts->sync_audio_drop_size &&
        mpctx->audio_drop_throttle < drop_limit &&
        mpctx->audio_status == STATUS_PLAYING)
    {
        int samples = ceil(opts->sync_audio_drop_size * play_samplerate);
        samples = (samples + align / 2) / align * align;

        skip_duplicate = mpctx->last_av_difference >= 0 ? -samples : samples;

        playsize = MPMAX(playsize, samples);

        mpctx->audio_drop_throttle += 1 - drop_limit - samples / play_samplerate;
    }

    mpctx->last_av_difference += skip_duplicate / play_samplerate;
    MP_VERBOSE(mpctx, "audio skip_duplicate=%d\n", skip_duplicate);

    mpctx->audio_drop_throttle =
        MPMAX(0, mpctx->audio_drop_throttle - played / play_samplerate);

#if HAVE_ENCODING
    encode_lavc_set_audio_pts(mpctx->encode_lavc_ctx, playing_audio_pts(mpctx));
#endif

#endif

    bool was_eof = mpctx->audio_status == STATUS_EOF;

    bool encountered, reached, recover;
    ao_get_eof_status(ao_c->ao, &encountered, &reached, &recover);

    if (encountered)
        mpctx->audio_status = STATUS_DRAINING;

    // If EOF was reached before, but now something can be decoded, try to
    // restart audio properly. This helps with video files where audio starts
    // later. Retrying is needed to get the correct sync PTS.
    if (recover && mpctx->audio_status >= STATUS_DRAINING) {
        mpctx->audio_status = STATUS_SYNCING;
        mp_wakeup_core(mpctx);
        return; // retry on next iteration
    }

    if (reached || (encountered && opts->gapless_audio)) {
        mpctx->audio_status = STATUS_EOF;
        if (!was_eof) {
            MP_VERBOSE(mpctx, "audio EOF reached\n");
            mp_wakeup_core(mpctx);
        }
    }
}

// Drop data queued for output, or which the AO is currently outputting.
void clear_audio_output_buffers(struct MPContext *mpctx)
{
    if (mpctx->ao)
        ao_reset(mpctx->ao);
}
