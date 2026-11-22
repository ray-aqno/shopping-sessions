#pragma once
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>

namespace shop {
using SV = std::string_view;
constexpr int64_t DAY = 86400000, WEEK = DAY * 7, GAP = 1800000;

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
        std::memcpy(&tail, s.data(), s.size());
        return mix(h ^ tail, 0x8ebc6af09c88c6e3ULL);
    }
};

template<size_t N> bool csv(SV line, std::array<SV, N>& fields) {
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
    int y = number(0, 4), m = number(5, 2), d = number(8, 2);
    int hh = number(11, 2), mm = number(14, 2), ss = number(17, 2);
    if (y < 1 || m < 1 || m > 12 || d < 1 || hh < 0 || hh > 23 || mm < 0 || mm > 59 || ss < 0 || ss > 59) return false;
    constexpr int md[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    if (d > md[m-1] + (m == 2 && leap)) return false;
    int zone = 0;
    if (s.size() == 20) { if (s[19] != 'Z') return false; }
    else {
        if ((s[19] != '+' && s[19] != '-') || s[22] != ':') return false;
        int zh = number(20, 2), zm = number(23, 2);
        if (zh < 0 || zh > 23 || zm < 0 || zm > 59) return false;
        zone = (zh * 60 + zm) * (s[19] == '+' ? 1 : -1);
    }
    out = civil_days(y, unsigned(m), unsigned(d)) * DAY + (hh * 3600LL + mm * 60 + ss - zone * 60) * 1000;
    return true;
}

inline int32_t day_of(int64_t t) {
    int64_t d = t / DAY;
    if (t % DAY < 0) --d;
    return int32_t(d);
}
}
