#pragma once
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>
#include <vector>
#if defined(__x86_64__)
#include <immintrin.h>
#endif

namespace shop {
using SV = std::string_view;
constexpr int64_t DAY = 86400000, WEEK = DAY * 7, GAP = 1800000;

// Injective representation of up to ten characters from a 63-character alphabet.
// The initial 1 is a length sentinel, preserving leading zeros and case.
inline uint64_t packed_id(SV s) {
    if (s.empty() || s.size()>10) return 0;
    static constexpr auto codes=[] {
        std::array<uint8_t,256> table{};
        for (unsigned c='0'; c<='9'; ++c) table[c]=uint8_t(1+c-'0');
        for (unsigned c='A'; c<='Z'; ++c) table[c]=uint8_t(11+c-'A');
        for (unsigned c='a'; c<='z'; ++c) table[c]=uint8_t(37+c-'a');
        table['_']=63; return table;
    }();
    uint64_t key=1; unsigned invalid=0;
    for (unsigned char c : s) { unsigned code=codes[c]; invalid|=code==0; key=(key<<6)|code; }
    return invalid ? 0 : key;
}
inline SV unpacked_id(uint64_t key,std::array<char,10>& buffer) {
    size_t pos=buffer.size();
    while (key>1) {
        unsigned code=key&63;
        buffer[--pos]=char(code<=10 ? '0'+code-1 : code<=36 ? 'A'+code-11 : code<=62 ? 'a'+code-37 : '_');
        key>>=6;
    }
    return {buffer.data()+pos,buffer.size()-pos};
}
inline size_t integer_hash(uint64_t key) {
    key^=key>>30; key*=0xbf58476d1ce4e5b9ULL;
    key^=key>>27; key*=0x94d049bb133111ebULL;
    return key^(key>>31);
}

template<class Sequence> void radix_sort64(Sequence& rows) {
    using T=typename Sequence::value_type;
    if (rows.size()<2) return;
    if (rows.size()<256 || rows.size()>UINT32_MAX) {
        std::stable_sort(rows.begin(),rows.end(),[](const T& a,const T& b) { return a.key<b.key; });
        return;
    }
    Sequence scratch(rows.size());
    std::array<uint32_t,65536> positions;
    uint64_t any=0,all=UINT64_MAX;
    for (const auto& row:rows) { any|=row.key; all&=row.key; }
    for (unsigned shift=0;shift<64;shift+=16) {
        if (!((any^all)&(0xffffULL<<shift))) continue;
        positions.fill(0);
        for (const auto& row:rows) ++positions[(row.key>>shift)&65535];
        uint32_t offset=0;
        for (auto& position:positions) { uint32_t count=position; position=offset; offset+=count; }
        for (const auto& row:rows) scratch[positions[(row.key>>shift)&65535]++]=row;
        rows.swap(scratch);
    }
}

// string_view's comparisons are bytewise (char_traits uses memcmp).
struct Hash {
    size_t operator()(SV s) const noexcept {
        // An unaligned, full-string hash; collisions always get an exact comparison.
        uint64_t h = 0xa0761d6478bd642fULL ^ s.size();
        auto mix = [](uint64_t a, uint64_t b) {
            __uint128_t p = __uint128_t(a) * b;
            return uint64_t(p) ^ uint64_t(p >> 64);
        };
        while (s.size() >= 8) {
            uint64_t v; std::memcpy(&v, s.data(), 8);
            h = mix(h ^ v, 0xe7037ed1a0b428dbULL);
            s.remove_prefix(8);
        }
        uint64_t tail = 0;
        if (s.size()>=4) {
            uint32_t first,last;
            std::memcpy(&first,s.data(),4); std::memcpy(&last,s.data()+s.size()-4,4);
            tail=first | (uint64_t(last)<<32);
        } else if (s.size()>=2) {
            uint16_t first,last;
            std::memcpy(&first,s.data(),2); std::memcpy(&last,s.data()+s.size()-2,2);
            tail=first | (uint64_t(last)<<16);
        } else if (!s.empty()) tail=static_cast<unsigned char>(s[0]);
        return mix(h ^ tail, 0x8ebc6af09c88c6e3ULL);
    }
};

template<size_t N> bool csv_scalar(SV line, std::array<SV, N>& fields) {
    size_t offset = 0;
    for (size_t i = 0; i < N; ++i) {
        size_t end = line.find(',', offset);
        if (end == SV::npos) {
            if (i != N - 1) return false;
            end = line.size();
        } else if (i == N - 1) return false;
        SV f = line.substr(offset, end - offset);
        if (!f.empty() && f.front() == '"') {
            if (f.size() < 2 || f.back() != '"') return false;
            f.remove_prefix(1); f.remove_suffix(1);
        }
        if (f.find('"') != SV::npos) return false;
        fields[i] = f;
        offset = end + 1;
    }
    return true;
}

#if defined(__x86_64__)
template<size_t N> __attribute__((target("avx2,popcnt")))
bool csv_avx2(SV line, std::array<SV,N>& fields) {
    std::array<size_t,N> ends;
    size_t count=0, quotes=0, offset=0;
    const __m256i comma=_mm256_set1_epi8(','), quote=_mm256_set1_epi8('"');
    while (line.size()-offset>=32) {
        __m256i bytes=_mm256_loadu_si256(reinterpret_cast<const __m256i*>(line.data()+offset));
        uint32_t separators=uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes,comma)));
        quotes+=unsigned(__builtin_popcount(unsigned(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes,quote)))));
        while (separators) {
            if (count>=N-1) return false;
            ends[count++]=offset+unsigned(__builtin_ctz(separators));
            separators&=separators-1;
        }
        offset+=32;
    }
    if (offset<line.size() && line.size()>=32) {
        size_t base=line.size()-32;
        uint32_t mask=UINT32_MAX << unsigned(offset-base);
        __m256i bytes=_mm256_loadu_si256(reinterpret_cast<const __m256i*>(line.data()+base));
        uint32_t separators=uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes,comma))) & mask;
        quotes+=unsigned(__builtin_popcount(unsigned(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes,quote))) & mask));
        while (separators) {
            if (count>=N-1) return false;
            ends[count++]=base+unsigned(__builtin_ctz(separators));
            separators&=separators-1;
        }
        offset=line.size();
    }
    for (; offset<line.size(); ++offset) {
        if (line[offset]==',') {
            if (count>=N-1) return false;
            ends[count++]=offset;
        }
        quotes+=line[offset]=='"';
    }
    if (count!=N-1) return false;
    ends[N-1]=line.size();
    size_t start=0, expected_quotes=0;
    for (size_t i=0; i<N; ++i) {
        SV field=line.substr(start,ends[i]-start);
        if (!field.empty() && field.front()=='"') {
            if (field.size()<2 || field.back()!='"') return false;
            field.remove_prefix(1); field.remove_suffix(1); expected_quotes+=2;
        }
        fields[i]=field; start=ends[i]+1;
    }
    // The boundary checks account for all legal quotes. Every extra quote,
    // inside a quoted field or in an unquoted one, makes the row malformed.
    return quotes==expected_quotes;
}
#endif

inline bool vector_csv_supported() {
#if defined(__x86_64__)
    static const bool supported=__builtin_cpu_supports("avx2") && __builtin_cpu_supports("popcnt");
    return supported;
#else
    return false;
#endif
}
template<size_t N> bool csv(SV line, std::array<SV,N>& fields) {
#if defined(__x86_64__)
    if (vector_csv_supported()) return csv_avx2(line,fields);
#endif
    return csv_scalar(line,fields);
}

inline bool digits(SV s, uint64_t limit, uint64_t& value) {
    if (s.empty()) return false;
    value = 0;
    for (unsigned char c : s) {
        if (c < '0' || c > '9') return false;
        unsigned d = c - '0';
        if (value > limit / 10 || (value == limit / 10 && d > limit % 10)) return false;
        value = value * 10 + d;
    }
    return true;
}

inline int64_t civil_days(int y, unsigned m, unsigned d) {
    y -= m <= 2;
    int era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = unsigned(y - era * 400);
    unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return int64_t(era) * 146097 + doe - 719468;
}

inline bool calendar_day(SV s,int64_t& out) {
    if (s.size()<10 || s[4]!='-' || s[7]!='-') return false;
    struct Entry { uint64_t head=0; int64_t milliseconds=0; uint16_t tail=0; };
    static thread_local std::array<Entry,64> cache;
    uint64_t head; uint16_t tail;
    std::memcpy(&head,s.data(),8); std::memcpy(&tail,s.data()+8,2);
    unsigned index=((tail&255)*11+(tail>>8)+unsigned((head>>40)&255)*7+unsigned((head>>48)&255))&63;
    auto& entry=cache[index];
    if (entry.head==head && entry.tail==tail) { out=entry.milliseconds; return true; }
    auto number=[&](size_t p,size_t n) {
        int value=0;
        for (size_t i=p;i<p+n;++i) {
            unsigned c=static_cast<unsigned char>(s[i]);
            if (c<'0' || c>'9') return -100000;
            value=value*10+int(c-'0');
        }
        return value;
    };
    int y=number(0,4),m=number(5,2),d=number(8,2);
    if (y<1 || m<1 || m>12 || d<1) return false;
    constexpr int md[]={31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap=y%4==0 && (y%100!=0 || y%400==0);
    if (d>md[m-1]+(m==2 && leap)) return false;
    out=civil_days(y,unsigned(m),unsigned(d))*DAY;
    entry={head,out,tail}; return true;
}

inline bool timestamp(SV s, int64_t& out) {
    if (s.size() != 20 && s.size() != 25) {
        uint64_t value;
        if (!digits(s, uint64_t(INT64_MAX), value)) return false;
        out = int64_t(value); return true;
    }
    // Numeric F3 can also have exactly 20 or 25 characters (leading zeros).
    if (s[4] != '-') {
        uint64_t value;
        if (!digits(s, uint64_t(INT64_MAX), value)) return false;
        out = int64_t(value); return true;
    }
    if (s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':') return false;
    auto number = [&](size_t p, size_t n) -> int {
        int x = 0;
        for (size_t i = p; i < p + n; ++i) {
            unsigned c = static_cast<unsigned char>(s[i]);
            if (c < '0' || c > '9') return -100000;
            x = x * 10 + int(c - '0');
        }
        return x;
    };
    int64_t day;
    if (!calendar_day(s,day)) return false;
    int hh = number(11, 2), mm = number(14, 2), ss = number(17, 2);
    if (hh < 0 || hh > 23 || mm < 0 || mm > 59 || ss < 0 || ss > 59) return false;
    int zone = 0;
    if (s.size() == 20) { if (s[19] != 'Z') return false; }
    else {
        if ((s[19] != '+' && s[19] != '-') || s[22] != ':') return false;
        int zh = number(20, 2), zm = number(23, 2);
        if (zh < 0 || zh > 23 || zm < 0 || zm > 59) return false;
        zone = (zh * 60 + zm) * (s[19] == '+' ? 1 : -1);
    }
    out = day + (hh * 3600LL + mm * 60 + ss - zone * 60) * 1000;
    return true;
}

inline int32_t day_of(int64_t t) {
    int64_t d = t / DAY;
    if (t % DAY < 0) --d;
    return int32_t(d);
}
}
