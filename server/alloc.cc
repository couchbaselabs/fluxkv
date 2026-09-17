// Route every allocation in the process through jemalloc.
//
// The Couchbase build of jemalloc is compiled with a `je_` symbol prefix, so
// linking it does nothing by itself: `malloc`, `operator new` and everything
// that calls them still resolve to glibc. Under the write benchmark glibc's
// allocator was a third of server CPU. Requests are allocated on an IO thread
// and freed on a writer thread, which is the pattern glibc handles worst: the
// free has to take the owning arena's lock (`__lll_lock_wait_private`) and the
// arenas grow by mprotect under the process-wide mmap lock.
//
// Defining these symbols in the executable puts them first in the dynamic
// symbol lookup order, so glibc, folly, libevent and every other shared
// library in the process pick them up too. All of the C allocation entry
// points are covered; mixing families (jemalloc malloc, glibc free) would
// corrupt both heaps.

#include <jemalloc/jemalloc.h>

#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <new>

extern "C" {

void* malloc(size_t size) {
    return je_malloc(size);
}

void free(void* ptr) {
    je_free(ptr);
}

void* calloc(size_t num, size_t size) {
    return je_calloc(num, size);
}

void* realloc(void* ptr, size_t size) {
    return je_realloc(ptr, size);
}

void* aligned_alloc(size_t alignment, size_t size) {
    return je_aligned_alloc(alignment, size);
}

int posix_memalign(void** memptr, size_t alignment, size_t size) {
    return je_posix_memalign(memptr, alignment, size);
}

void* memalign(size_t alignment, size_t size) {
    return je_memalign(alignment, size);
}

void* valloc(size_t size) {
    return je_valloc(size);
}

size_t malloc_usable_size(void* ptr) {
    return je_malloc_usable_size(ptr);
}

} // extern "C"

namespace {
inline void* newOrThrow(size_t size) {
    void* p = je_malloc(size);
    if (p == nullptr) {
        throw std::bad_alloc();
    }
    return p;
}

inline void* newAlignedOrThrow(size_t size, std::align_val_t al) {
    void* p = je_aligned_alloc(static_cast<size_t>(al), size);
    if (p == nullptr) {
        throw std::bad_alloc();
    }
    return p;
}
} // namespace

// Numbered as on https://en.cppreference.com/w/cpp/memory/new/operator_new
// and operator_delete. Sized deletes use sdallocx so jemalloc can skip the
// size-class lookup.

[[nodiscard]] void* operator new(std::size_t count) {
    return newOrThrow(count);
}
[[nodiscard]] void* operator new[](std::size_t count) {
    return newOrThrow(count);
}
[[nodiscard]] void* operator new(std::size_t count, std::align_val_t al) {
    return newAlignedOrThrow(count, al);
}
[[nodiscard]] void* operator new[](std::size_t count, std::align_val_t al) {
    return newAlignedOrThrow(count, al);
}
[[nodiscard]] void* operator new(std::size_t count,
                                 const std::nothrow_t&) noexcept {
    return je_malloc(count);
}
[[nodiscard]] void* operator new[](std::size_t count,
                                   const std::nothrow_t&) noexcept {
    return je_malloc(count);
}
[[nodiscard]] void* operator new(std::size_t count,
                                 std::align_val_t al,
                                 const std::nothrow_t&) noexcept {
    return je_aligned_alloc(static_cast<size_t>(al), count);
}
[[nodiscard]] void* operator new[](std::size_t count,
                                   std::align_val_t al,
                                   const std::nothrow_t&) noexcept {
    return je_aligned_alloc(static_cast<size_t>(al), count);
}

void operator delete(void* ptr) noexcept {
    je_free(ptr);
}
void operator delete[](void* ptr) noexcept {
    je_free(ptr);
}
void operator delete(void* ptr, std::align_val_t) noexcept {
    je_free(ptr);
}
void operator delete[](void* ptr, std::align_val_t) noexcept {
    je_free(ptr);
}
void operator delete(void* ptr, std::size_t size) noexcept {
    if (ptr) {
        je_sdallocx(ptr, size, 0);
    }
}
void operator delete[](void* ptr, std::size_t size) noexcept {
    if (ptr) {
        je_sdallocx(ptr, size, 0);
    }
}
void operator delete(void* ptr, std::size_t size, std::align_val_t al) noexcept {
    if (ptr) {
        je_sdallocx(ptr, size, MALLOCX_ALIGN(static_cast<size_t>(al)));
    }
}
void operator delete[](void* ptr,
                       std::size_t size,
                       std::align_val_t al) noexcept {
    if (ptr) {
        je_sdallocx(ptr, size, MALLOCX_ALIGN(static_cast<size_t>(al)));
    }
}
void operator delete(void* ptr, const std::nothrow_t&) noexcept {
    je_free(ptr);
}
void operator delete[](void* ptr, const std::nothrow_t&) noexcept {
    je_free(ptr);
}
void operator delete(void* ptr,
                     std::align_val_t,
                     const std::nothrow_t&) noexcept {
    je_free(ptr);
}
void operator delete[](void* ptr,
                       std::align_val_t,
                       const std::nothrow_t&) noexcept {
    je_free(ptr);
}
