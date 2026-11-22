#pragma once
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <type_traits>
#include <utility>

// Only payloads with no ownership or internal self-pointers belong here. Unlike
// vector's allocate/copy/free growth, realloc can grow or remap a large block.
template<class T> class TrivialBuffer {
    static_assert(std::is_trivially_copyable_v<T>);
    static_assert(alignof(T)<=alignof(std::max_align_t));
    T* data_=nullptr;
    size_t size_=0, capacity_=0;
public:
    using value_type=T;
    TrivialBuffer()=default;
    explicit TrivialBuffer(size_t n) { reserve(n); size_=n; }
    TrivialBuffer(const TrivialBuffer&)=delete;
    TrivialBuffer& operator=(const TrivialBuffer&)=delete;
    TrivialBuffer(TrivialBuffer&& other) noexcept { swap(other); }
    TrivialBuffer& operator=(TrivialBuffer&& other) noexcept {
        if (this!=&other) { TrivialBuffer old; old.swap(*this); swap(other); }
        return *this;
    }
    ~TrivialBuffer() { std::free(data_); }
    size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return !size_; }
    T* data() noexcept { return data_; }
    const T* data() const noexcept { return data_; }
    T* begin() noexcept { return data_; }
    const T* begin() const noexcept { return data_; }
    T* end() noexcept { return size_ ? data_+size_ : data_; }
    const T* end() const noexcept { return size_ ? data_+size_ : data_; }
    T& operator[](size_t i) noexcept { return data_[i]; }
    const T& operator[](size_t i) const noexcept { return data_[i]; }
    void reserve(size_t n) {
        if (n<=capacity_) return;
        constexpr size_t maximum=std::numeric_limits<size_t>::max()/sizeof(T);
        if (n>maximum) throw std::bad_alloc();
        size_t grown=capacity_>maximum/2 ? maximum : std::max(size_t(256),capacity_*2);
        size_t capacity=std::max(n,std::min(grown,maximum));
        void* next=std::realloc(data_,capacity*sizeof(T));
        if (!next) throw std::bad_alloc();
        data_=static_cast<T*>(next); capacity_=capacity;
    }
    void append(const T* source,size_t count) {
        if (count>std::numeric_limits<size_t>::max()-size_) throw std::bad_alloc();
        reserve(size_+count);
        if (count) std::memcpy(data_+size_,source,count*sizeof(T));
        size_+=count;
    }
    void swap(TrivialBuffer& other) noexcept {
        std::swap(data_,other.data_); std::swap(size_,other.size_); std::swap(capacity_,other.capacity_);
    }
};
