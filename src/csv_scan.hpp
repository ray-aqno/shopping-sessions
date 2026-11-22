#pragma once
#include "core.hpp"

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
template<size_t N,class F> __attribute__((target("avx2,popcnt")))
void scan_csv_avx2(SV content,F& consume) {
    if (content.empty()) return;
    const char* position=content.data();
    const char* limit=position+content.size();
    const __m256i commas=_mm256_set1_epi8(','),quotes=_mm256_set1_epi8('"'),newlines=_mm256_set1_epi8('\n');
    while (position<limit) {
        const char* start=position;
        const char* cursor=position;
        const char* row_end=nullptr;
        std::array<const char*,N> ends{};
        size_t count=0,quote_count=0;
        bool valid=true;
        while (limit-cursor>=32) {
            __m256i bytes=_mm256_loadu_si256(reinterpret_cast<const __m256i*>(cursor));
            uint32_t nl=uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes,newlines)));
            uint32_t mask=nl ? (uint32_t(1)<<unsigned(__builtin_ctz(nl)))-1 : UINT32_MAX;
            uint32_t separators=uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes,commas)))&mask;
            quote_count+=unsigned(__builtin_popcount(unsigned(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes,quotes)))&mask));
            while (separators) {
                if (count<N-1) ends[count++]=cursor+unsigned(__builtin_ctz(separators));
                else valid=false;
                separators&=separators-1;
            }
            if (nl) {
                row_end=cursor+unsigned(__builtin_ctz(nl)); position=row_end+1; break;
            }
            cursor+=32;
        }
        if (!row_end) {
            while (cursor<limit && *cursor!='\n') {
                if (*cursor==',') {
                    if (count<N-1) ends[count++]=cursor;
                    else valid=false;
                }
                quote_count+=*cursor=='"'; ++cursor;
            }
            row_end=cursor; position=cursor<limit ? cursor+1 : limit;
        }
        if (!valid || count!=N-1) continue;
        ends[N-1]=row_end;
        std::array<SV,N> fields;
        size_t expected_quotes=0;
        for (size_t i=0;i<N;++i) {
            size_t length=size_t(ends[i]-start);
            const char* value=start;
            if (length && *value=='"') {
                if (length<2 || ends[i][-1]!='"') { valid=false; break; }
                ++value; length-=2; expected_quotes+=2;
            }
            fields[i]=SV(value,length);
            if (i+1<N) start=ends[i]+1;
        }
        if (valid && quote_count==expected_quotes) consume(fields);
    }
}
#endif

template<size_t N,class F> void scan_csv(SV content,F&& consume) {
#if defined(__x86_64__)
    if (vector_csv_supported()) { scan_csv_avx2<N>(content,consume); return; }
#endif
    scan_csv_scalar<N>(content,consume);
}
}
