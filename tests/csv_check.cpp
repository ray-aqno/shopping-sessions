#include "core.hpp"
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>

using namespace shop;
template<size_t N> void check(SV line) {
    std::array<SV,N> expected, got;
    bool a=csv_scalar(line,expected), b=csv(line,got);
    if (a!=b || (a && expected!=got)) throw std::runtime_error("CSV parser disagreement");
}
int main() {
    std::mt19937_64 random(827415);
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
