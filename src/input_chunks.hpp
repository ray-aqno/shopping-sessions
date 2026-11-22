#pragma once
#include "core.hpp"

namespace shop {
struct InputChunk {
    SV file;
    size_t start,end;
    SV view() const {
        auto after_line=[&](size_t boundary) {
            if (boundary>=file.size()) return file.size();
            size_t newline=file.find('\n',boundary ? boundary-1 : 0);
            return newline==SV::npos ? file.size() : newline+1;
        };
        size_t first=after_line(start),last=after_line(end);
        return file.substr(first,last-first);
    }
};
// Planning uses only sizes. The worker that reads a chunk also resolves its
// record boundaries, avoiding serial page faults across every mapped input.
inline std::vector<InputChunk> chunks(const std::vector<SV>& files,size_t bytes=4*1024*1024) {
    std::vector<InputChunk> result;
    for (SV file:files) for (size_t start=0;start<file.size();) {
        size_t end=start+std::min(bytes,file.size()-start);
        result.push_back({file,start,end}); start=end;
    }
    return result;
}
}
