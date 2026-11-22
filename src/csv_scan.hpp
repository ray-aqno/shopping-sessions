#pragma once
#include "core.hpp"
#include <cstring>

namespace shop {
template<size_t N,class F> void scan_csv_scalar(SV content,F& consume) {
    size_t pos=0;
    while (pos<content.size()) {
        size_t end=content.find('\n',pos);
        if (end==SV::npos) end=content.size();
        std::array<SV,N> fields;
        if (csv_scalar(content.substr(pos,end-pos),fields)) consume(fields);
        pos=end+1;
    }
}

#if defined(__x86_64__)
// One pass over 64-byte blocks: each block's commas, newlines and quotes become
// bitmasks, and the separators are visited in order. Every byte is loaded once;
// a row's quote count is a difference of running popcounts. The last partial
// block is copied into a zeroed buffer, so nothing past the content is read.
template<size_t N,class F> __attribute__((target("avx2,popcnt")))
void scan_csv_avx2(SV content,F& consume) {
    if (content.empty()) return;
    const char* const base=content.data();
    const size_t size=content.size();
    const __m256i commas=_mm256_set1_epi8(','),quotes=_mm256_set1_epi8('"'),newlines=_mm256_set1_epi8('\n');
    const char* start=base;
    std::array<const char*,N> ends{};
    size_t count=0; bool overflow=false;
    uint64_t quote_total=0, quote_start=0;
    auto finish=[&](const char* row_end,uint64_t quote_end) {
        if (!overflow && count==N-1) {
            ends[N-1]=row_end;
            std::array<SV,N> fields;
            size_t expected_quotes=0; bool valid=true;
            const char* field=start;
            for (size_t i=0;i<N;++i) {
                size_t length=size_t(ends[i]-field);
                const char* value=field;
                if (length && *value=='"') {
                    if (length<2 || ends[i][-1]!='"') { valid=false; break; }
                    ++value; length-=2; expected_quotes+=2;
                }
                fields[i]=SV(value,length);
                if (i+1<N) field=ends[i]+1;
            }
            if (valid && quote_end-quote_start==expected_quotes) consume(fields);
        }
        start=row_end+1; count=0; overflow=false;
    };
    for (size_t offset=0;offset<size;offset+=64) {
        const char* block=base+offset;
        __m256i lo,hi;
        if (size-offset>=64) {
            lo=_mm256_loadu_si256(reinterpret_cast<const __m256i*>(block));
            hi=_mm256_loadu_si256(reinterpret_cast<const __m256i*>(block+32));
        } else {
            alignas(32) char tail[64]={};
            std::memcpy(tail,block,size-offset);
            lo=_mm256_load_si256(reinterpret_cast<const __m256i*>(tail));
            hi=_mm256_load_si256(reinterpret_cast<const __m256i*>(tail+32));
        }
#define SHOP_MASK(c) (uint64_t(uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(lo,c)))) | (uint64_t(uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(hi,c))))<<32))
        const uint64_t nl=SHOP_MASK(newlines), q=SHOP_MASK(quotes);
        uint64_t separators=nl|SHOP_MASK(commas);
#undef SHOP_MASK
        while (separators) {
            const unsigned bit=unsigned(__builtin_ctzll(separators));
            if ((nl>>bit)&1) {
                const uint64_t before=quote_total+uint64_t(__builtin_popcountll(q&((uint64_t(1)<<bit)-1)));
                finish(block+bit,before);
                quote_start=before;   // A newline is not a quote.
            } else if (count<N-1) ends[count++]=block+bit;
            else overflow=true;
            separators&=separators-1;
        }
        quote_total+=uint64_t(__builtin_popcountll(q));
    }
    if (start<base+size) finish(base+size,quote_total);
}
#endif

template<size_t N,class F> void scan_csv(SV content,F&& consume) {
#if defined(__x86_64__)
    if (vector_csv_supported()) { scan_csv_avx2<N>(content,consume); return; }
#endif
    scan_csv_scalar<N>(content,consume);
}
}
