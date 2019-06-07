#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "packet.h"
#include "stheader.h"

struct demux_cache;
struct demux_cache_packets;

struct demux_cache_fns {
    // Init backend. Returns success. If it fails, destroy and talloc_free() is
    // called.
    bool (*init)(struct demux_cache *c);

    // Destroy everything. Packet lists are separately destroyed before this.
    // talloc_free(c) is called after this.
    void (*destroy)(struct demux_cache *c);

    // Generic allocation function. If size==0 it behaves like free(); if
    // old==NULL, it behaves like malloc(). Called with relatively low sizes and
    // in relatively low numbers (currently used for seek index only).
    void *(*generic_realloc)(struct demux_cache *c, void *old, size_t size);

    // Allocate a packet list. Currently there is one for every stream and for
    // every seek range. At this point it's already part of c->packet_lists.
    // On failure, packets_destroy will be called.
    bool (*packets_init)(struct demux_cache *c, struct demux_cache_packets *list);

    // Destroy a previously initialized list. Before this is called, the caller
    // will remove all packets (packets_clear or packets_prune). After this is
    // called, the caller will remove it from demux_cache.packet_lists and call
    // talloc_free(list).
    void (*packets_destroy)(struct demux_cache_packets *list);

    // Remove all packets. This can free all packet memory at once. Note that
    // the caller may start calling packets_append again.
    // The callback can be NULL if the backend does not support this, but then
    // packets_prune must be supported.
    void (*packets_clear)(struct demux_cache_packets *list);

    // Add a packet to the end of the list. The callee gets ownership over pkt
    // (except on failure). Returns a possibly different pointer to a data
    // structure containing the same metadata as pkt. However, the callee is
    // allowed to set the following fields to arbitrary values:
    //  demux_packet.buffer
    //  demux_packet.len
    //  demux_packet.avpacket
    //  demux_packet.cache_pos
    // The cache must not change any other fields.
    // The caller must not use these fields (including dereferencing them).
    // The intention is that the packet payload data can be moved to
    // inaccessible storage, and the fields can be used to store information
    // about the location of the data.
    // Warning: the above means that the returned demux_packet breaks internal
    // assumptions of the demux_packet API. In fact, it should probably use a
    // different type.
    // Further, it should set *out_used_size to the size of the estimated
    // size of bytes the packet uses in the cache.
    // Returned packets can only be deallocated by calling packets_prune or by
    // destroying the packet list.
    // Returns NULL on failure.
    struct demux_packet *(*packets_append)(struct demux_cache_packets *list,
                                           struct demux_packet *pkt,
                                           uint64_t *out_used_size);

    // Recreate a "proper" packet from a packet previously returned by
    // packets_append. The packet must not have been deallocated yet. The
    // returned packet is a normal packet that can be used with the demux_packet
    // API, and which is deallocated with talloc_free().
    struct demux_packet *(*packets_read)(struct demux_cache_packets *list,
                                         struct demux_packet *pkt);

    // Deallocate a packet. This must be the first packet in the list. The pkt
    // parameter is only for verifying API usage (can trigger an internal error).
    // The callback can be NULL if the backend does not support pruning (then
    // packets_clear must be supported).
    void (*packets_prune)(struct demux_cache_packets *list,
                          struct demux_packet *pkt);

    // Whether range merging is supported. Range merging requires appending a
    // packet list to another. Currently, there's no concept for this, so it
    // can work only for backends which treat packets as individual allocations.
    bool can_merge_packet_lists;
};

struct demux_cache {
    const char *tempdir_root;
    bool check_falloc;
    const struct demux_cache_fns *fns;
    struct mp_log *log;
    void *priv;

    struct demux_cache_packets **packet_lists;
    int num_packet_lists;
};

struct demux_cache_packets {
    struct demux_cache *owner;
    void *priv;

    struct sh_stream *sh;
    // Unique ID set before packets_init() is called. The ID is unique across
    // all cache ranges within a demux_cache instance and is not reused. This
    // will still set the same ID on all packet lists for a range (so only
    // (sh->index, range_id) is unique for each demux_cache_packets within a
    // single demux_cache).
    uint64_t range_id;

    // Cumulative position at which the latest appended packet is located.
    uint64_t last_pos;
};

extern const struct demux_cache_fns demux_cache_fns_malloc;
extern const struct demux_cache_fns demux_cache_fns_mmap;
