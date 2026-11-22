#include "csv_scan.hpp"
#include "input_chunks.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

using namespace shop;
template<size_t N> void check(SV input) {
    std::vector<std::array<SV,N>> expected,got;
    auto reference=[&](const auto& fields) { expected.push_back(fields); };
    scan_csv_scalar<N>(input,reference);
    scan_csv<N>(input,[&](const auto& fields) { got.push_back(fields); });
    if (expected!=got) throw std::runtime_error("fused CSV disagreement");
}
int main() {
    std::mt19937_64 random(718341);
    for (const std::string& text:{std::string{},std::string("header"),std::string("header\n"),std::string("h\na,b\n\nc,d\r\nx,y"),std::string("\n\na,b\n")}) {
        for (size_t width=1;width<70;++width) {
            std::string joined;
            for (const auto& chunk:chunks({SV(text)},width)) joined+=chunk.view();
            size_t newline=text.find('\n');
            SV expected=newline==std::string::npos ? SV{} : SV(text).substr(newline+1);
            if (joined!=expected) throw std::runtime_error("chunk boundary disagreement");
        }
    }
    {
        std::string text="h\n"+std::string(5*1024*1024,'x')+"\nlast,row";
        std::string joined;
        for (const auto& chunk:chunks({SV(text)})) joined+=chunk.view();
        if (joined!=SV(text).substr(2)) throw std::runtime_error("long-row chunk disagreement");
    }
    constexpr SV alphabet="AbC012 ,\"\r\t\n\xc3\xa9";
    check<7>(SV{});
    for (size_t trial=0;trial<10000;++trial) {
        std::string input(random()%32,' ');
        size_t prefix=input.size();
        for (size_t n=random()%900;n;--n) input+=alphabet[random()%alphabet.size()];
        check<2>(SV(input).substr(prefix)); check<4>(SV(input).substr(prefix)); check<7>(SV(input).substr(prefix));
        input.resize(prefix);
        for (unsigned row=0;row<10;++row) {
            for (unsigned field=0;field<7;++field) {
                if (field) input+=',';
                bool quoted=random()%2;
                if (quoted) input+='"';
                for (size_t n=random()%45;n;--n) input+=char('a'+random()%26);
                if (quoted) input+='"';
            }
            if (row!=9 || random()%2) input+='\n';
        }
        check<7>(SV(input).substr(prefix));
        input.insert(prefix+random()%(input.size()-prefix+1),1,'"');
        check<7>(SV(input).substr(prefix));
    }
    size_t page=size_t(sysconf(_SC_PAGESIZE));
    char* mapping=static_cast<char*>(mmap(nullptr,2*page,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0));
    if (mapping==MAP_FAILED || mprotect(mapping+page,page,PROT_NONE)) throw std::runtime_error("guard-page setup");
    for (size_t n=0;n<=page;n+=17) {
        char* begin=mapping+page-n;
        for (size_t i=0;i<n;++i) begin[i]=alphabet[random()%alphabet.size()];
        check<2>({begin,n}); check<4>({begin,n}); check<7>({begin,n});
    }
    munmap(mapping,2*page);
    std::cout<<"Fused CSV parity and guard-page tests passed\n";
}
