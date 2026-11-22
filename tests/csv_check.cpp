#include "core.hpp"
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>

using namespace shop;
template<size_t N> void check(SV line) {
    std::array<SV,N> expected, got;
    bool a=csv_scalar(line,expected), b=csv(line,got);
    if (a!=b || (a && expected!=got)) throw std::runtime_error("CSV parser disagreement");
}
int main() {
    std::mt19937_64 random(827415);
    struct Row { uint64_t key,payload; bool operator==(const Row&) const=default; };
    for (unsigned trial=0;trial<24;++trial) {
        std::vector<Row> actual;
        for (size_t i=0,n=trial<3 ? trial : 10000+trial;i<n;++i)
            actual.push_back({trial%3==0 ? random()%100 : trial%3==1 ? random()<<32 : random(),i});
        auto expected=actual;
        std::stable_sort(expected.begin(),expected.end(),[](const Row& a,const Row& b) { return a.key<b.key; });
        radix_sort64(actual);
        if (expected!=actual) throw std::runtime_error("radix sort disagreement");
    }
    for (SV bad : {SV(""),SV(" u1"),SV("u1 "),SV("é"),SV("12345678901")})
        if (packed_id(bad)) throw std::runtime_error("unsupported ID entered packed path");
    std::unordered_map<uint64_t,std::string> keys;
    constexpr SV allowed="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_";
    for (unsigned sample=0;sample<100000;++sample) {
        std::string text;
        for (size_t n=1+random()%10;n;--n) text+=allowed[random()%allowed.size()];
        uint64_t key=packed_id(text);
        if (!key) throw std::runtime_error("valid ID rejected");
        std::array<char,10> buffer;
        if (unpacked_id(key,buffer)!=text) throw std::runtime_error("ID round trip failed");
        auto [it,inserted]=keys.emplace(key,text);
        if (!inserted && it->second!=text) throw std::runtime_error("packed ID collision");
    }
    constexpr SV alphabet="abcXYZ012 ,\"\r\t\xc3\xa9";
    for (unsigned sample=0; sample<100000; ++sample) {
        size_t prefix=random()%32;
        std::string text(prefix,' ');
        size_t size=random()%400;
        for (size_t i=0; i<size; ++i) text+=alphabet[random()%alphabet.size()];
        SV line(text.data()+prefix,size);
        check<2>(line); check<4>(line); check<7>(line);
        text.resize(prefix);
        for (unsigned f=0; f<7; ++f) {
            if (f) text+=',';
            bool quoted=random()%2;
            if (quoted) text+='"';
            for (size_t n=random()%80; n; --n) text+=char('a'+random()%26);
            if (quoted) text+='"';
        }
        check<7>(SV(text).substr(prefix));
        if (sample%2) { text.insert(prefix+random()%(text.size()-prefix+1),1,'"'); check<7>(SV(text).substr(prefix)); }
    }
    std::cout<<"450,000 parser comparisons passed; vector path: "<<vector_csv_supported()<<'\n';
}
