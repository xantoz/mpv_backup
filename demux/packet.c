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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <libavcodec/avcodec.h>
#include <libavutil/intreadwrite.h>

#include "config.h"

#include "common/av_common.h"
#include "common/common.h"
#include "demux.h"

#include "packet.h"

// Free any references dp itself holds. This includes refcounted data and
// metadata. This does not care about references that are _not_ refcounted
// (like demux_packet.codec).
// Normally, a user should use talloc_free(dp). This function is only for
// annoyingly specific obscure use cases.
void demux_packet_unref_contents(struct demux_packet *dp)
{
    av_packet_unref(dp->avpacket);
    dp->avpacket = NULL;
    dp->buffer = NULL;
    dp->len = 0;
    mp_packet_tags_unref(dp->metadata);
    dp->metadata = NULL;
}

// Whether calling demux_packet_unref_contents() is required to avoid leaks.
bool demux_packet_needs_unref_contents(struct demux_packet *dp)
{
    return dp->avpacket || dp->metadata;
}

static void packet_destroy(void *ptr)
{
    struct demux_packet *dp = ptr;
    demux_packet_unref_contents(dp);
}

// This actually preserves only data and side data, not PTS/DTS/pos/etc.
// It also allows avpkt->data==NULL with avpkt->size!=0 - the libavcodec API
// does not allow it, but we do it to simplify new_demux_packet().
// The returned packet uses talloc, and can be freed with tallo_free().
struct demux_packet *new_demux_packet_from_avpacket(struct AVPacket *avpkt)
{
    if (avpkt->size > 1000000000)
        return NULL;
    struct demux_packet *dp = talloc(NULL, struct demux_packet);
    talloc_set_destructor(dp, packet_destroy);
    *dp = (struct demux_packet) {
        .pts = MP_NOPTS_VALUE,
        .dts = MP_NOPTS_VALUE,
        .duration = -1,
        .pos = -1,
        .start = MP_NOPTS_VALUE,
        .end = MP_NOPTS_VALUE,
        .stream = -1,
        .avpacket = talloc_zero(dp, AVPacket),
        .kf_seek_pts = MP_NOPTS_VALUE,
    };
    av_init_packet(dp->avpacket);
    int r = -1;
    if (avpkt->data) {
        // We hope that this function won't need/access AVPacket input padding,
        // because otherwise new_demux_packet_from() wouldn't work.
        r = av_packet_ref(dp->avpacket, avpkt);
    } else {
        r = av_new_packet(dp->avpacket, avpkt->size);
    }
    if (r < 0) {
        *dp->avpacket = (AVPacket){0};
        talloc_free(dp);
        return NULL;
    }
    dp->buffer = dp->avpacket->data;
    dp->len = dp->avpacket->size;
    return dp;
}

// Create a packet with a new reference to buf.
// (buf must include proper padding)
struct demux_packet *new_demux_packet_from_buf(struct AVBufferRef *buf)
{
    if (!buf)
        return NULL;
    AVPacket pkt = {
        .size = buf->size,
        .data = buf->data,
        .buf = buf,
    };
    return new_demux_packet_from_avpacket(&pkt);
}

// Input data doesn't need to be padded. Copies all data.
struct demux_packet *new_demux_packet_from(void *data, size_t len)
{
    if (len > INT_MAX)
        return NULL;
    AVPacket pkt = { .data = data, .size = len };
    return new_demux_packet_from_avpacket(&pkt);
}

struct demux_packet *new_demux_packet(size_t len)
{
    if (len > INT_MAX)
        return NULL;
    AVPacket pkt = { .data = NULL, .size = len };
    return new_demux_packet_from_avpacket(&pkt);
}

void demux_packet_shorten(struct demux_packet *dp, size_t len)
{
    assert(len <= dp->len);
    dp->len = len;
    memset(dp->buffer + dp->len, 0, AV_INPUT_BUFFER_PADDING_SIZE);
}

void free_demux_packet(struct demux_packet *dp)
{
    talloc_free(dp);
}

void demux_packet_copy_attribs(struct demux_packet *dst, struct demux_packet *src)
{
    dst->pts = src->pts;
    dst->dts = src->dts;
    dst->duration = src->duration;
    dst->pos = src->pos;
    dst->segmented = src->segmented;
    dst->start = src->start;
    dst->end = src->end;
    dst->codec = src->codec;
    dst->back_restart = src->back_restart;
    dst->back_preroll = src->back_preroll;
    dst->keyframe = src->keyframe;
    dst->stream = src->stream;
    mp_packet_tags_setref(&dst->metadata, src->metadata);
}

struct demux_packet *demux_copy_packet(struct demux_packet *dp)
{
    struct demux_packet *new = NULL;
    if (dp->avpacket) {
        new = new_demux_packet_from_avpacket(dp->avpacket);
    } else {
        // Some packets might be not created by new_demux_packet*().
        new = new_demux_packet_from(dp->buffer, dp->len);
    }
    if (!new)
        return NULL;
    demux_packet_copy_attribs(new, dp);
    return new;
}

#define ROUND_ALLOC(s) MP_ALIGN_UP(s, 64)

// Attempt to estimate the total memory consumption of the given packet.
// This is important if we store thousands of packets and not to exceed
// user-provided limits. Of course we can't know how much memory internal
// fragmentation of the libc memory allocator will waste.
// Note that this should return a "stable" value - e.g. if a new packet ref
// is created, this should return the same value with the new ref. (This
// implies the value is not exact and does not return the actual size of
// memory wasted due to internal fragmentation.)
size_t demux_packet_estimate_total_size(struct demux_packet *dp)
{
    size_t size = ROUND_ALLOC(sizeof(struct demux_packet));
    size += ROUND_ALLOC(dp->len);
    if (dp->avpacket) {
        size += ROUND_ALLOC(sizeof(AVPacket));
        size += ROUND_ALLOC(sizeof(AVBufferRef));
        size += 64; // upper bound estimate on sizeof(AVBuffer)
        size += ROUND_ALLOC(dp->avpacket->side_data_elems *
                            sizeof(dp->avpacket->side_data[0]));
        for (int n = 0; n < dp->avpacket->side_data_elems; n++)
            size += ROUND_ALLOC(dp->avpacket->side_data[n].size);
    }
    return size;
}

int demux_packet_set_padding(struct demux_packet *dp, int start, int end)
{
#if LIBAVCODEC_VERSION_MICRO >= 100
    if (!start && !end)
        return 0;
    if (!dp->avpacket)
        return -1;
    uint8_t *p = av_packet_new_side_data(dp->avpacket, AV_PKT_DATA_SKIP_SAMPLES, 10);
    if (!p)
        return -1;

    AV_WL32(p + 0, start);
    AV_WL32(p + 4, end);
#endif
    return 0;
}

int demux_packet_add_blockadditional(struct demux_packet *dp, uint64_t id,
                                     void *data, size_t size)
{
#if LIBAVCODEC_VERSION_MICRO >= 100
    if (!dp->avpacket)
        return -1;
    uint8_t *sd =  av_packet_new_side_data(dp->avpacket,
                                           AV_PKT_DATA_MATROSKA_BLOCKADDITIONAL,
                                           8 + size);
    if (!sd)
        return -1;
    AV_WB64(sd, id);
    if (size > 0)
        memcpy(sd + 8, data, size);
#endif
    return 0;
}

// Write the packet payload and some other stuff to a memory buffer at ptr. The
// function is allowed to write up to avail bytes to the buffer. If avail is too
// small, the function may write nothing or may write useless partial data. It
// always returns the amount of bytes that are needed; if the return value is
// larger than avail, the caller may need to retry. In particular, avail==0 can
// be used to determine the needed size (then ptr can be NULL too).
// ptr must always be aligned on DEMUX_PACKET_SER_ALIGN boundaries.
// The returned size is always aligned on DEMUX_PACKET_SER_ALIGN.
// Returns 0 on unknown errors.
//
// The goal of this is that demux_packet_deserialize_payload() will be able to
// restore a packet if the following members have been clobbered:
//  demux_packet.buffer
//  demux_packet.len
//  demux_packet.avpacket
// The internals of this are closely coupled to new_demux_packet_from_avpacket()
// and mp_set_av_packet(). Also, the written data is EXTREMELY specific to the
// internals of both mpv and ffmpeg (memory dumps of external data structures).
// It's strictly intended for cache purposes, and for anything that interfaces
// with outside processes.
size_t demux_packet_serialize_payload(struct demux_packet *dp, void *ptr,
                                      size_t avail)
{
    static_assert(DEMUX_PACKET_SER_ALIGN == sizeof(uint32_t), "");
    assert(dp->avpacket);

    if (avail)
        assert(!((uintptr_t)ptr & 3));

    assert(dp->len >= 0 && dp->len <= INT32_MAX);
    assert(dp->avpacket->flags >= 0 && dp->avpacket->flags <= INT32_MAX);
    assert(dp->avpacket->side_data_elems >= 0 &&
           dp->avpacket->side_data_elems <= INT32_MAX);

    static_assert(DEMUX_PACKET_SER_MINSIZE == 16, "");
    size_t size = 16 + MP_ALIGN_UP(dp->len, 4);
    if (size <= avail) {
        uint32_t *wptr = ptr;
        // wptr[0] is written later.
        wptr[1] = dp->len;
        wptr[2] = dp->avpacket->flags;
        wptr[3] = dp->avpacket->side_data_elems;

        memcpy(wptr + 4, dp->buffer, dp->len);
    }

    // The handling of FFmpeg side data requires an extra long comment to
    // explain why this code is fragile and insane.
    // FFmpeg packet side data is per-packet out of band data, that contains
    // further information for the decoder (extra metadata and such), which is
    // not part of the codec itself and thus isn't contained in the packet
    // payload. All types use a flat byte array. The format of this byte array
    // is non-standard and FFmpeg-specific, and depends on the side data type
    // field. The side data type is of course a FFmpeg ABI artifact.
    // In some cases, the format is described as fixed byte layout. In others,
    // it contains a struct, i.e. is bound to FFmpeg ABI. Some newer types make
    // the format explicitly internal (and _not_ part of the ABI), and you need
    // to use separate accessors to turn it into complex data structures.
    // As of now, FFmpeg fortunately adheres to the idea that side data can not
    // contain embedded pointers (due to API rules, but also because they forgot
    // adding a refcount field, and can't change this until hey break ABI).
    // We rely on this. We hope that FFmpeg won't silently change their
    // semantics, and add refcounting and embedded pointers. This way we can
    // for example dump the data in a disk cache, even though we can't use the
    // data from another process or if this process is restarted (unless we're
    // absolutely sure the FFmpeg internals didn't change). The data has to be
    // treated as a memory dump.
    for (int n = 0; n < dp->avpacket->side_data_elems; n++) {
        AVPacketSideData *sd = &dp->avpacket->side_data[n];

        assert(sd->size >= 0 && sd->size <= INT32_MAX);
        assert(sd->type >= 0 && sd->type <= INT32_MAX);

        size_t start = size;
        size += 8 + MP_ALIGN_UP(sd->size, 4);
        if (size <= avail) {
            uint32_t *wptr = (uint32_t *)ptr + start / 4;
            wptr[0] = sd->size;
            wptr[1] = sd->type;
            memcpy(wptr + 2, sd->data, sd->size);
        }
    }

    if (size <= avail)
        *(uint32_t *)ptr = size;

    // I guess this could happen in theory (but of course not in practice).
    if (size > UINT32_MAX)
        return 0;

    return size;
}

// Return the size a demux_packet_serialize_payload() call wrote. avail is the
// number of readable bytes. Returns 0 if avail is too low. If avail >=
// DEMUX_PACKET_SER_MINSIZE, and it returns 0, it's an internal error.
size_t demux_packet_deserialize_payload_get_size(void *ptr, size_t avail)
{
    return avail >= 4 ? *(uint32_t *)ptr : 0;
}

// Undo demux_packet_serialize_payload(). The avail parameter is for some half-
// hearted error checking only, and must be >= the value the serialize function
// returned when writing the data at ptr (otherwise it's an internal error).
// Call demux_packet_deserialize_payload_get_size() to know the size.
// dp is used for metadata not stored in the raw stream.
// Tries to return a properly refcounted packet. May return NULL on OOM.
struct demux_packet *demux_packet_deserialize_payload(struct demux_packet *dp,
                                                      void *ptr, size_t avail)
{
    size_t size = demux_packet_deserialize_payload_get_size(ptr, avail);
    assert(size && size <= avail); // caller is stupid or data was corrupted
    assert(!((uintptr_t)ptr & 3));

    uint32_t *rptr = ptr;
    uint32_t dp_len = rptr[1];
    uint32_t avflags = rptr[2];
    uint32_t num_sd = rptr[3];

    struct demux_packet *new = new_demux_packet_from(rptr + 4, dp_len);
    if (!new)
        return NULL;

    new->avpacket->flags = avflags;

    size_t offset = 16 + MP_ALIGN_UP(dp_len, 4);

    for (int n = 0; n < num_sd; n++) {
        rptr = (uint32_t *)((char *)ptr + offset);
        uint32_t sd_size = rptr[0];
        uint32_t sd_type = rptr[1];

        uint8_t *sd = av_packet_new_side_data(new->avpacket, sd_type, sd_size);
        if (!sd) {
            talloc_free(new);
            return NULL;
        }

        memcpy(sd, rptr + 2, sd_size);

        offset += 8 + MP_ALIGN_UP(sd_size, 4);
    }

    assert(offset == size);

    demux_packet_copy_attribs(new, dp);
    return new;
}
