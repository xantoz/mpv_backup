#include <fcntl.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "cache.h"
#include "common/common.h"
#include "options/path.h"

#define MP_PAGE_SIZE 4096

static_assert(!(MP_PAGE_SIZE & (MP_PAGE_SIZE - 1)), "must be power of 2");

#define RT_WRITE    0   // mmap region for writing packet data
#define RT_READ     1   // mmap region for reading packet data

// Existing mapping for a mmap_file.
struct mmap_range {
    uint64_t offset;
    size_t size;
    void *ptr;
    int type;
};

struct mmap_file {
    struct demux_cache *owner;
    char *filename;
    bool is_generic;
    int fd;
    uint64_t size;
    struct mmap_range *ranges;
    int num_ranges;
    int lookup_cache;
};

// demux_cache.priv points to this.
struct priv {
    char *path;                 // exclusive temp. directory for this cache
    struct mmap_file **files;
    int num_files;
    uint64_t generic_id;
    bool falloc_warned;
};

// demux_cache_packets.priv points to this.
struct packets_priv {
    struct mmap_file *data; // packet payloads
    uint64_t data_pos;      // end position for writing
    struct mmap_file *meta; // demux_packet
    uint64_t meta_pos;

    bool any_packets_need_unref;

    struct demux_packet *first;
    struct demux_packet *last;
};

// Call mmap() to map the region. offset must be page-aligned, size not. Like
// with mmap semantics, the region doesn't need to be within the file, but the
// file can be resized later so that it is.
// Returns the new range on success, NULL on failure.
static struct mmap_range *mmap_file_map(struct mmap_file *f, uint64_t offset,
                                        size_t size)
{
    assert(size > 0);
    assert(!(offset & (MP_PAGE_SIZE - 1))); // not aligned

    // mmap doesn't require this, but do it for better mmap_range information.
    size = MP_ALIGN_UP(size, MP_PAGE_SIZE);

    void *addr =
        mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, f->fd, offset);
    if (addr == MAP_FAILED) {
        MP_ERR(f->owner, "Failed to create cache mapping.\n");
        return NULL;
    }

    struct mmap_range r = {
        .offset = offset,
        .size = size,
        .ptr = addr,
    };
    MP_TARRAY_APPEND(f, f->ranges, f->num_ranges, r);
    return &f->ranges[f->num_ranges - 1];
}

static void mmap_file_unmap(struct mmap_file *f, int index)
{
    assert(index >= 0 && index < f->num_ranges);

    // (Failure should not happen.)
    if (munmap(f->ranges[index].ptr, f->ranges[index].size))
        MP_ERR(f->owner, "Cache munmap() failed.\n");

    MP_TARRAY_REMOVE_AT(f->ranges, f->num_ranges, index);
}

static void mmap_file_unmap_all(struct mmap_file *f)
{
    while (f->num_ranges)
        mmap_file_unmap(f, f->num_ranges - 1);
}

static bool mmap_file_resize(struct mmap_file *f, uint64_t new_size)
{
    struct demux_cache *c = f->owner;
    struct priv *p = c->priv;

    if (f->size == new_size)
        return true;

    if (ftruncate(f->fd, new_size)) {
        MP_ERR(c, "Resizing cache size failed.\n");
        return false;
    }

    if (new_size > f->size) {
        int err = posix_fallocate(f->fd, f->size, new_size - f->size);
        if (err && !p->falloc_warned) {
            // (Maybe failure should be tolerated only if
            MP_ERR(c, "Failed to allocate disk cache data (%s).\n",
                   mp_strerror(err));
            if (c->check_falloc) {
                ftruncate(f->fd, f->size);
                return false;
            } else if (!p->falloc_warned) {
                MP_WARN(c, "Ignoring this and not calling it again. Bad idea.\n");
                p->falloc_warned = true;
            }
        }
    }

    f->size = new_size;
    return true;
}

static bool mmap_file_grow(struct mmap_file *f, uint64_t used, uint64_t needed)
{
    assert(used <= f->size);

    if (needed <= f->size - used)
        return true;

#if 1
    uint64_t new_size = MPMAX(f->size, MP_PAGE_SIZE * 64);
    while (new_size < f->size + needed)
        new_size *= 2; // some growth factor
#else
    uint64_t new_size = used;
    new_size += needed + MP_PAGE_SIZE * 64;
    new_size = MP_ALIGN_UP(new_size, MP_PAGE_SIZE);
#endif
    return mmap_file_resize(f, new_size);
}

static void *mmap_range_get_offset_ptr(struct mmap_range *r, uint64_t offset,
                                       size_t size)
{
    if (r->offset <= offset && r->offset + r->size >= offset + size)
        return (char *)r->ptr + (offset - r->offset);
    return NULL;
}

static void *mmap_file_lookup(struct mmap_file *f, uint64_t offset, size_t size)
{
    if (f->lookup_cache >= 0 && f->lookup_cache < f->num_ranges) {
        void *ptr = mmap_range_get_offset_ptr(&f->ranges[f->lookup_cache],
                                              offset, size);
        if (ptr)
            return ptr;
    }

    for (int n = 0; n < f->num_ranges; n++) {
        void *ptr = mmap_range_get_offset_ptr(&f->ranges[n], offset, size);
        if (ptr) {
            f->lookup_cache = n;
            return ptr;
        }
    }

    return NULL;
}

static void mmap_file_destroy(struct demux_cache *c, struct mmap_file *f)
{
    struct priv *p = c->priv;

    if (!f)
        return;

    mmap_file_unmap_all(f);

    int index = -1;
    for (int n = 0; n < p->num_files; n++) {
        if (p->files[n] == f) {
            index = n;
            break;
        }
    }
    assert(index >= 0);

    if (f->fd >= 0) {
        close(f->fd);

        if (unlink(f->filename))
            MP_ERR(c, "Failed to delete cache temporary file.\n");
    }

    MP_TARRAY_REMOVE_AT(p->files, p->num_files, index);
    talloc_free(f);
}

static struct mmap_file *mmap_file_create(struct demux_cache *c, char *name)
{
    struct priv *p = c->priv;

    struct mmap_file *f = talloc_zero(c, struct mmap_file);
    MP_TARRAY_APPEND(p, p->files, p->num_files, f);

    f->owner = c;
    f->filename = mp_path_join(f, p->path, name);

    f->fd = open(f->filename, O_CREAT | O_TRUNC | O_RDWR, 0666);
    if (f->fd < 0) {
        MP_ERR(c, "Failed to create cache temporary file.\n");
        mmap_file_destroy(c, f);
        return NULL;
    }

    return f;
}

static void *mmap_generic_realloc(struct demux_cache *c, void *old, size_t size)
{
    struct priv *p = c->priv;

    if (!size && !old)
        return NULL;

    struct mmap_file *f = NULL;
    if (old) {
        for (int n = 0; n < p->num_files; n++) {
            struct mmap_file *cur = p->files[n];
            if (cur->is_generic && cur->num_ranges && cur->ranges[0].ptr == old) {
                f = cur;
                break;
            }
        }
    }
    if (f)
        assert(f->num_ranges == 1);

    // free()
    if (size == 0) {
        assert(f); // invalid pointer passed?
        mmap_file_destroy(c, f);
        return NULL;
    }

    // malloc()
    if (!old) {
        p->generic_id++;
        f = mmap_file_create(c,
            mp_tprintf(64, "generic%lld", (long long)p->generic_id));
        if (!f)
            return NULL;
        f->is_generic = true;
        if (!mmap_file_resize(f, size) ||
            !mmap_file_map(f, 0, size))
        {
            mmap_file_destroy(c, f);
            return NULL;
        }
        assert(f->num_ranges == 1);
        return f->ranges[0].ptr;
    }

    // realloc()
    assert(f); // invalid pointer passed?

    if (!mmap_file_resize(f, size))
        return NULL;

    if (!mmap_file_map(f, 0, size))
        return NULL;

    assert(f->num_ranges == 2);

    mmap_file_unmap(f, 0);

    return f->ranges[0].ptr;
}

static void mmap_packets_destroy(struct demux_cache_packets *list)
{
    struct packets_priv *ppriv = list->priv;

    // Caller must have called packets_clear.
    assert(ppriv->data_pos == 0);
    assert(ppriv->meta_pos == 0);

    mmap_file_destroy(list->owner, ppriv->data);
    mmap_file_destroy(list->owner, ppriv->meta);
}

static void mmap_packets_clear(struct demux_cache_packets *list)
{
    struct packets_priv *ppriv = list->priv;

    //(Generally avoid having to do this; it's needed only in some obscure
    // corner cases.)
    if (ppriv->any_packets_need_unref) {
        for (uint64_t pos = 0;
             pos < ppriv->meta_pos;
             pos += sizeof(struct demux_packet))
        {
            struct demux_packet *ptr =
                mmap_file_lookup(ppriv->meta, pos, sizeof(struct demux_packet));
            assert(ptr);
            // Make a copy so this doesn't unnecessarily create dirty pages.
            struct demux_packet dp = *ptr;
            demux_packet_unref_contents(&dp);
        }
    }

    mmap_file_unmap_all(ppriv->data);
    mmap_file_unmap_all(ppriv->meta);

    mmap_file_resize(ppriv->data, 0);
    mmap_file_resize(ppriv->meta, 0);

    ppriv->data_pos = 0;
    ppriv->meta_pos = 0;
}

static bool mmap_packets_init(struct demux_cache *c,
                              struct demux_cache_packets *list)
{
    list->priv = talloc_zero(c, struct packets_priv);
    struct packets_priv *ppriv = list->priv;

    char *suffix = mp_tprintf(50, "%lld-%d", (long long)list->range_id,
                              list->sh->index);

    ppriv->data = mmap_file_create(c, mp_tprintf(64, "data-%s", suffix));
    ppriv->meta = mmap_file_create(c, mp_tprintf(64, "meta-%s", suffix));

    return ppriv->data && ppriv->meta;
}

// Ensure there's a mapping to access the file data at the given offset, with
// the possibility to access up to size bytes. Return the pointer to the mapped
// offset. Neither offset nor size need to be aligned. May reconstruct mappings
// freely and invalidate old pointers.
// type is matched against mmap_range.type, and is used to allow multiple access
// windows. For packet payload data, it's either RT_READ or RT_WRITE.
static void *remap_window(struct mmap_file *f, int type, uint64_t offset,
                          size_t size)
{
    assert(offset + size <= f->size);

    int typed_index = -1;

    for (int n = 0; n < f->num_ranges; n++) {
        struct mmap_range *r = &f->ranges[n];

        void *ptr = mmap_range_get_offset_ptr(r, offset, size);
        if (ptr)
            return ptr;

        if (r->type == type)
            typed_index = n;
    }

    if (typed_index >= 0)
        mmap_file_unmap(f, typed_index);

    struct mmap_range *r = NULL;

    // On 64 bit, actually try to keep everything mapped. Depending on the OS,
    // this is probably more efficient. (NB: it might be even better to attempt
    // to resize an existing mapping first.)
    if (sizeof(size_t) >= 8)
        r = mmap_file_map(f, 0, f->size);

    if (!r) {
        uint64_t a_offset = MP_ALIGN_DOWN(offset, MP_PAGE_SIZE);
        size_t a_size = MP_ALIGN_UP(size + (offset - a_offset), MP_PAGE_SIZE);

        // Try to mmap ahead by some. Though on 32 bit it's way too easy to
        // accidentally exhaust the scarce address space.
        size_t premap_limit = sizeof(size_t) >= 8 ? (1ULL << 27) : (1 << 25);
        a_size = MPMAX(a_size, premap_limit);

        r = mmap_file_map(f, a_offset, a_size);
    }

    if (!r)
        return NULL;

    r->type = type;

    return mmap_range_get_offset_ptr(r, offset, size);
}

static struct demux_packet *mmap_packets_append(struct demux_cache_packets *list,
                                                struct demux_packet *pkt,
                                                uint64_t *out_used_size)
{
    struct packets_priv *ppriv = list->priv;

    size_t needed_data = demux_packet_serialize_payload(pkt, NULL, 0);
    if (!needed_data) {
        MP_ERR(list->owner, "wat\n"); // I bet a beer that this never happens
        return NULL;
    }

    if (!mmap_file_grow(ppriv->data, ppriv->data_pos, needed_data))
        return NULL;

    void *data = remap_window(ppriv->data, RT_WRITE, ppriv->data_pos, needed_data);
    if (!data)
        return NULL;

    size_t res = demux_packet_serialize_payload(pkt, data, needed_data);
    assert(res == needed_data);

    size_t needed_meta = sizeof(struct demux_packet);
    size_t old_meta_map_size = ppriv->meta->size;
    if (!mmap_file_grow(ppriv->meta, ppriv->meta_pos, needed_meta))
        return NULL;
    if (ppriv->meta->size > old_meta_map_size) {
        assert(MP_IS_ALIGNED(ppriv->meta->size, MP_PAGE_SIZE));
        // Need to create new mappings. Unlike packet payload data, we must
        // never unmap packet metadata, because demux_packet pointers must
        // remain valid for the lifetime of the packet within the cache. This is
        // why we only add packets. (NB: on some OSes, we could attempt to grow
        // existing mappings.)
        if (!mmap_file_map(ppriv->meta, old_meta_map_size, ppriv->meta->size))
            return NULL;
    }

    void *meta_ptr = mmap_file_lookup(ppriv->meta, ppriv->meta_pos, needed_meta);
    assert(meta_ptr);

    struct demux_packet *new = meta_ptr;
    memset(new, 0, sizeof(*new));
    demux_packet_copy_attribs(new, pkt);

    new->cache_pos = ppriv->data_pos;

    ppriv->data_pos += needed_data;
    ppriv->meta_pos += needed_meta;
    ppriv->any_packets_need_unref |= demux_packet_needs_unref_contents(new);
    talloc_free(pkt);

    *out_used_size = needed_data + needed_meta;
    return new;
}

static struct demux_packet *mmap_packets_read(struct demux_cache_packets *list,
                                              struct demux_packet *pkt)
{
    struct packets_priv *ppriv = list->priv;

    uint64_t pkt_pos = pkt->cache_pos;
    assert(pkt_pos < ppriv->data_pos);

    void *data = remap_window(ppriv->data, RT_READ, pkt_pos,
                              DEMUX_PACKET_SER_MINSIZE);
    if (!data)
        return NULL;

    size_t pkt_size = demux_packet_deserialize_payload_get_size(data,
                                                    DEMUX_PACKET_SER_MINSIZE);
    assert(pkt_size); // corrupted data?

    data = remap_window(ppriv->data, RT_READ, pkt_pos, pkt_size);
    if (!data)
        return NULL;

    return demux_packet_deserialize_payload(pkt, data, pkt_size);
}

static void mmap_destroy(struct demux_cache *c)
{
    struct priv *p = c->priv;

    if (p->path) {
        if (rmdir(p->path))
            MP_ERR(c, "Failed to delete cache temporary directory.\n");
    }
}

static bool mmap_init(struct demux_cache *c)
{
    c->priv = talloc_zero(c, struct priv);
    struct priv *p = c->priv;

    // I don't think such a system even exists. While modern CPUs support
    // various page sizes, most things are pretty much hardcoded to a minimum
    // page size of 4K for numerous reasons.
    long page_size = sysconf(_SC_PAGE_SIZE);
    while (page_size > 0 && page_size < MP_PAGE_SIZE)
        page_size *= 2;
    if (page_size != MP_PAGE_SIZE)
        return false;

    if (!(c->tempdir_root && c->tempdir_root[0])) {
        MP_ERR(c, "No cache data directory supplied.\n");
        return false;
    }

    char *dir = mp_path_join(c, c->tempdir_root, "mpv-cache-XXXXXX");
    if (!mkdtemp(dir)) {
        MP_ERR(c, "Failed to create temporary directory in '%s'.\n",
               c->tempdir_root);
        return false;
    }
    p->path = dir;

    return true;
}

const struct demux_cache_fns demux_cache_fns_mmap = {
    .init               = mmap_init,
    .destroy            = mmap_destroy,

    .generic_realloc    = mmap_generic_realloc,

    .packets_init       = mmap_packets_init,
    .packets_destroy    = mmap_packets_destroy,
    .packets_clear      = mmap_packets_clear,
    .packets_append     = mmap_packets_append,
    .packets_read       = mmap_packets_read,
};
