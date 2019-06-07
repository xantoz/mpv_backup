#include "cache.h"
#include "common/common.h"

static void cm_destroy(struct demux_cache *c)
{
}

static void *cm_generic_realloc(struct demux_cache *c, void *old, size_t size)
{
    return ta_realloc_size(NULL, old, size);
}

static bool cm_packets_init(struct demux_cache *c,
                            struct demux_cache_packets *list)
{
    return true;
}

static void cm_packets_destroy(struct demux_cache_packets *list)
{
}

static struct demux_packet *cm_packets_append(struct demux_cache_packets *list,
                                              struct demux_packet *pkt,
                                              uint64_t *out_used_size)
{
    *out_used_size = demux_packet_estimate_total_size(pkt);
    return pkt;
}

static struct demux_packet *cm_packets_read(struct demux_cache_packets *list,
                                            struct demux_packet *pkt)
{
    return demux_copy_packet(pkt);
}

static void cm_packets_prune(struct demux_cache_packets *list,
                             struct demux_packet *pkt)
{
    talloc_free(pkt);
}

static bool cm_init(struct demux_cache *c)
{
    return true;
}

const struct demux_cache_fns demux_cache_fns_malloc = {
    .init               = cm_init,
    .destroy            = cm_destroy,
    .generic_realloc    = cm_generic_realloc,
    .packets_init       = cm_packets_init,
    .packets_destroy    = cm_packets_destroy,
    .packets_append     = cm_packets_append,
    .packets_read       = cm_packets_read,
    .packets_prune      = cm_packets_prune,
    .can_merge_packet_lists = true,
};
