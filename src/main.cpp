#include "core.hpp"
#include "csv_scan.hpp"
#include "input_chunks.hpp"
#include "trivial_buffer.hpp"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <malloc.h>
#include <unistd.h>
#include <chrono>
#include <atomic>
#include <bit>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <numeric>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using namespace shop;
constexpr unsigned PARTS = 256;
constexpr uint32_t NONE = UINT32_MAX;
template<class T> using Dict = std::unordered_map<SV, T, Hash>;

struct Mapping {
    char* data = nullptr; size_t size = 0;
    explicit Mapping(const fs::path& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("open: " + path.string());
        struct stat st{};
        if (fstat(fd, &st)) { close(fd); throw std::runtime_error("fstat"); }
        size = size_t(st.st_size);
        if (size) {
            void* p = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
            if (p == MAP_FAILED) { close(fd); throw std::runtime_error("mmap"); }
            data = static_cast<char*>(p);
            madvise(data, size, MADV_SEQUENTIAL);
        }
        close(fd);
    }
    Mapping(const Mapping&) = delete;
    ~Mapping() { if (data) munmap(data, size); }
};
struct Inputs {
    std::vector<std::unique_ptr<Mapping>> maps;
    SV map(const fs::path& path) {
        maps.push_back(std::make_unique<Mapping>(path));
        auto& m = *maps.back(); return {m.data, m.size};
    }
    std::vector<SV> csvs(const fs::path& dir) {
        std::vector<fs::path> paths;
        if (fs::exists(dir)) for (const auto& e : fs::directory_iterator(dir))
            if (e.is_regular_file() && e.path().extension() == ".csv") paths.push_back(e.path());
        std::sort(paths.begin(), paths.end());
        std::vector<SV> result;
        for (auto& p : paths) result.push_back(map(p));
        return result;
    }
};
template<class F> void lines(SV content, F fn, bool header = true) {
    size_t pos = 0;
    if (header) {
        size_t e = content.find('\n');
        if (e == SV::npos) return;
        pos = e + 1;
    }
    while (pos < content.size()) {
        size_t e = content.find('\n', pos);
        if (e == SV::npos) e = content.size();
        fn(content.substr(pos, e-pos)); pos = e+1;
    }
}

// An ID whose directory slot has been prefetched. A per-thread cache in front of
// the directory measured slower: it hit about a quarter of lookups and its own
// misses competed for cache with the prefetched probes.
struct ResolveRequest {
    SV name;
    uint64_t packed=0;
    size_t hash=0;
};
// Every user ID seen, in one fixed-capacity, insert-only, lock-free table per key
// kind. Linked IDs are preloaded with their person; a standalone ID is claimed on
// first sight by CAS on the key, and the winner then publishes its person. A
// reader that finds the key waits for that publication. Past the load limit an
// insert is refused and `overflowed` set: the caller grows the table and reruns
// the whole read, so each ID still maps to exactly one person.
// Fixed-size table storage: 2 MiB aligned so it may use huge pages, and filled in
// parallel so the page faults of first touch are spread across workers.
template<class T> class Table {
    T* data_=nullptr; size_t size_=0;
public:
    Table()=default;
    Table(const Table&)=delete;
    Table& operator=(const Table&)=delete;
    Table(Table&& other) noexcept { swap(other); }
    Table& operator=(Table&& other) noexcept { Table old; old.swap(*this); swap(other); return *this; }
    ~Table() { std::free(data_); }
    void swap(Table& other) noexcept { std::swap(data_,other.data_); std::swap(size_,other.size_); }
    void reset(size_t count) {
        static_assert(std::is_trivially_copyable_v<T>);
        constexpr size_t PAGE=2*1024*1024;
        Table().swap(*this);
        size_t bytes=(count*sizeof(T)+PAGE-1)/PAGE*PAGE;
        data_=static_cast<T*>(std::aligned_alloc(PAGE,bytes));
        if (!data_) throw std::bad_alloc();
        madvise(data_,bytes,MADV_HUGEPAGE);
        size_=count;
        #pragma omp parallel for schedule(static)
        for (size_t i=0;i<count;++i) data_[i]=T{};
    }
    size_t size() const noexcept { return size_; }
    T& operator[](size_t i) noexcept { return data_[i]; }
    const T& operator[](size_t i) const noexcept { return data_[i]; }
    const T* begin() const noexcept { return data_; }
    const T* end() const noexcept { return data_+size_; }
};
struct Directory {
    struct Packed { uint64_t key=0; uint32_t value=NONE, unused=0; };
    struct alignas(32) Named { uint64_t hash=0; const char* data=nullptr; uint32_t size=0, value=NONE; };
    // No load counter: one shared counter serialises every claim across cores.
    // Instead an insert that probes this far without finding its key or a free
    // slot is refused. A stored key always lies within this distance of its
    // home, so lookups stay exact, and at sane loads such a run never occurs.
    static constexpr size_t MAX_PROBE=512;
    static constexpr unsigned PACKED_FULL=1, NAMED_FULL=2;
    Table<Packed> packed;
    Table<Named> named;
    std::atomic<unsigned> overflowed{0};
    static uint64_t named_hash(size_t hash) { return hash ? hash : 1; }
    static uint32_t published(uint32_t& value) {
        std::atomic_ref<uint32_t> ref(value);
        uint32_t v;
        while ((v=ref.load(std::memory_order_acquire))==NONE) _mm_pause();
        return v;
    }
    uint32_t refuse(unsigned table) { overflowed.fetch_or(table,std::memory_order_relaxed); return 0; }
    // `make` runs only in the thread that claims the key and returns its person.
    template<class Make> uint32_t find_or_add(uint64_t key,size_t hash,Make&& make) {
        const size_t mask=packed.size()-1;
        for (size_t probe=0,i=hash&mask;probe<MAX_PROBE;++probe,i=(i+1)&mask) {
            std::atomic_ref<uint64_t> slot(packed[i].key);
            uint64_t current=slot.load(std::memory_order_acquire);
            if (!current && slot.compare_exchange_strong(current,key,std::memory_order_acq_rel)) {
                uint32_t id=make();
                std::atomic_ref<uint32_t>(packed[i].value).store(id,std::memory_order_release);
                return id;
            }
            if (current==key) return published(packed[i].value);
        }
        return refuse(PACKED_FULL);
    }
    template<class Make> uint32_t find_or_add(SV key,size_t raw_hash,Make&& make) {
        const uint64_t hash=named_hash(raw_hash);
        const size_t mask=named.size()-1;
        for (size_t probe=0,i=raw_hash&mask;probe<MAX_PROBE;++probe,i=(i+1)&mask) {
            auto& entry=named[i];
            std::atomic_ref<uint64_t> slot(entry.hash);
            uint64_t current=slot.load(std::memory_order_acquire);
            if (!current && slot.compare_exchange_strong(current,hash,std::memory_order_acq_rel)) {
                entry.data=key.data(); entry.size=uint32_t(key.size());
                uint32_t id=make();
                std::atomic_ref<uint32_t>(entry.value).store(id,std::memory_order_release);
                return id;
            }
            if (current==hash) {
                uint32_t id=published(entry.value);
                if (entry.size==key.size() && std::memcmp(entry.data,key.data(),key.size())==0) return id;
            }
        }
        return refuse(NAMED_FULL);
    }
    // Lookups for a phase with no concurrent inserts: absent keys give NONE.
    uint32_t find(uint64_t key,size_t hash) const {
        const size_t mask=packed.size()-1;
        for (size_t probe=0,i=hash&mask;probe<MAX_PROBE;++probe,i=(i+1)&mask) {
            if (packed[i].key==key) return packed[i].value;
            if (!packed[i].key) break;
        }
        return NONE;
    }
    uint32_t find(SV key,size_t raw_hash) const {
        const uint64_t hash=named_hash(raw_hash);
        const size_t mask=named.size()-1;
        for (size_t probe=0,i=raw_hash&mask;probe<MAX_PROBE;++probe,i=(i+1)&mask) {
            const auto& entry=named[i];
            if (!entry.hash) break;
            if (entry.hash==hash && entry.size==key.size() && std::memcmp(entry.data,key.data(),key.size())==0) return entry.value;
        }
        return NONE;
    }
    const Packed* packed_slot(size_t hash) const { return &packed[hash&(packed.size()-1)]; }
    const Named* named_slot(size_t hash) const { return &named[hash&(named.size()-1)]; }
    void allocate(size_t packed_slots,size_t named_slots) {
        packed.reset(std::bit_ceil(packed_slots));
        named.reset(std::bit_ceil(named_slots));
        overflowed=0;
    }
    // Serial and rare: rebuild the full table(s) 8x larger, keeping every published
    // person. A refused reinsert would lose one, so retry until all fit.
    void grow() {
        auto old_packed=std::move(packed); auto old_named=std::move(named);
        size_t packed_slots=old_packed.size(), named_slots=old_named.size();
        for (unsigned full=overflowed;full;full=overflowed) {
            if (full&PACKED_FULL) packed_slots*=8;
            if (full&NAMED_FULL) named_slots*=8;
            allocate(packed_slots,named_slots);
            for (const auto& e:old_packed) if (e.key) find_or_add(e.key,integer_hash(e.key),[&] { return e.value; });
            for (const auto& e:old_named) if (e.hash) { SV key(e.data,e.size); find_or_add(key,Hash{}(key),[&] { return e.value; }); }
        }
    }
};
struct People {
    std::vector<uint32_t> parent;
    std::vector<SV> names;
    Directory directory;
    // Standalone names, one list per worker thread so appends need no lock. Their
    // views point into the mapped inputs, which outlive every reader.
    struct alignas(64) Loose { std::vector<SV> names; };
    std::vector<Loose> loose;
    ResolveRequest prepare(SV s) const {
        uint64_t packed=packed_id(s);
        size_t hash=packed ? integer_hash(packed) : Hash{}(s);
        __builtin_prefetch(packed ? static_cast<const void*>(directory.packed_slot(hash)) : static_cast<const void*>(directory.named_slot(hash)),0,3);
        return {s,packed,hash};
    }
    uint32_t resolve(const ResolveRequest& request,unsigned thread) {
        auto make=[&] { return add_standalone(request.name,thread); };
        return request.packed ? directory.find_or_add(request.packed,request.hash,make) : directory.find_or_add(request.name,request.hash,make);
    }
    // The person of an ID already seen, or NONE; never inserts.
    uint32_t lookup(const ResolveRequest& request) const {
        return request.packed ? directory.find(request.packed,request.hash) : directory.find(request.name,request.hash);
    }
    uint32_t add_standalone(SV s,unsigned thread) {
        auto& list=loose[thread].names;
        uint64_t index=uint64_t(list.size())*loose.size()+thread;
        if (index>=0x80000000ULL) throw std::runtime_error("too many singleton IDs");
        list.push_back(s);
        return 0x80000000U | uint32_t(index);
    }
    SV name(uint32_t id) const {
        if (!(id & 0x80000000U)) return names[id];
        uint32_t index=id & 0x7fffffffU;
        return loose[index % loose.size()].names[index / loose.size()];
    }
};

struct Dimensions {
    Dict<uint32_t> product, campaign;
    std::vector<SV> categories = {"unknown"}, campaigns = {"organic"}, channels = {"organic"};
    void load(Inputs& inputs, const fs::path& dir) {
        Dict<uint32_t> cats; cats.emplace("unknown", 0);
        lines(inputs.map(dir / "products.csv"), [&](SV line) {
            std::array<SV,2> f; if (!csv(line,f)) throw std::runtime_error("bad products.csv");
            auto [it, inserted] = cats.emplace(f[1], uint32_t(categories.size()));
            if (inserted) categories.push_back(f[1]);
            product.emplace(f[0], it->second);
        });
        lines(inputs.map(dir / "campaigns.csv"), [&](SV line) {
            std::array<SV,2> f; if (!csv(line,f)) throw std::runtime_error("bad campaigns.csv");
            campaign.emplace(f[0], uint32_t(campaigns.size()));
            campaigns.push_back(f[0]); channels.push_back(f[1]);
        });
    }
};
// Event times are offsets from window_start, so the window must span at most 2^32 ms.
struct CompactEvent { uint64_t key; uint32_t price,category; };
inline uint32_t event_person(const CompactEvent& e) { return uint32_t(e.key>>32); }
inline int64_t event_time(const CompactEvent& e,int64_t base) { return base+uint32_t(e.key); }
// One event row awaiting dedup, 32 bytes. `id` is a packed ID, or for any other ID
// 1<<63 | length<<48 | address (length LONG_ID when it does not fit 15 bits; the
// ID then ends at its field's closing quote or comma). `fingerprint` is the high
// half of the ID hash; the low byte chose the partition. `ingest` is in seconds
// from INGEST_BASE: ingest_ts is guaranteed F1, so whole seconds. person NONE
// marks an invalid copy or one that lost dedup.
struct Raw {
    static constexpr uint64_t STRING=uint64_t(1)<<63, LONG_ID=0x7fff;
    uint64_t id;
    uint32_t ingest, fingerprint, person, ts, price, category;
    static uint64_t encode(SV s,uint64_t packed) {
        if (packed) return packed;
        uint64_t length=std::min<uint64_t>(s.size(),LONG_ID);
        return STRING | length<<48 | uint64_t(reinterpret_cast<uintptr_t>(s.data()));
    }
    SV key() const {
        const char* data=reinterpret_cast<const char*>(uintptr_t(id & ((uint64_t(1)<<48)-1)));
        size_t length=(id>>48) & LONG_ID;
        if (length==LONG_ID) { length=0; while (data[length]!=',' && data[length]!='"' && data[length]!='\n') ++length; }
        return {data,length};
    }
    bool same_id(const Raw& other) const {
        if (!(id & STRING) || !(other.id & STRING)) return id==other.id;
        return key()==other.key();
    }
};
static_assert(sizeof(Raw)==32);
inline size_t event_id_hash(SV s,uint64_t packed) { return packed ? integer_hash(packed) : Hash{}(s); }
// key is person<<32 | milliseconds from window_start - 7 days.
struct Click {
    uint64_t key; SV id; uint32_t campaign;
    uint32_t person() const { return uint32_t(key>>32); }
};
inline int64_t click_time(const Click& c,int64_t base) { return base+uint32_t(c.key); }
// A bucket is the list of per-thread segments handed to it: producers move whole
// buffers in once per pass, and a consumer gathers them into a reused buffer.
template<class T> struct Bucket {
    std::mutex mutex;
    std::vector<TrivialBuffer<T>> parts;
    void gather(TrivialBuffer<T>& out) {
        size_t size=0; for (const auto& part:parts) size+=part.size();
        out.clear(); out.reserve(size);
        for (const auto& part:parts) out.append(part.data(),part.size());
        parts.clear();
    }
};
template<class T> using Buckets = std::array<Bucket<T>, PARTS>;
template<class T> using Local = std::array<TrivialBuffer<T>, PARTS>;
// Sizes each partition buffer once for its expected share of `rows`. Growth by
// mremap takes the address-space lock exclusively and stalls every other thread's
// page faults; unused reservation is only address space.
template<class T> void presize(Local<T>& local,size_t rows) {
    size_t share=rows/(size_t(omp_get_num_threads())*PARTS);
    for (auto& buffer:local) buffer.reserve(share+share/2+64);
}
inline size_t input_bytes(const std::vector<SV>& files) { size_t bytes=0; for (SV file:files) bytes+=file.size(); return bytes; }
template<class T> void flush(Local<T>& local, Buckets<T>& buckets) {
    for (unsigned i=0; i<PARTS; ++i) if (!local[i].empty()) {
        auto& dst = buckets[i]; std::lock_guard lock(dst.mutex);
        dst.parts.push_back(std::move(local[i]));
    }
}
// Each link side is claimed in the directory while parsing. Thread t's i-th new ID
// gets the provisional value i*T+t, so claims need no shared counter; a prefix sum
// over per-thread counts then gives the dense ID offset[t]+i. Edges keep these
// integers, so joining never looks an ID up again.
void build_links(const std::vector<SV>& files,People& people) {
    const unsigned T=unsigned(omp_get_max_threads());
    people.loose.assign(T,People::Loose{});
    auto& directory=people.directory;
    size_t bytes=0; for (SV file:files) bytes+=file.size();
    // About one new ID per 20 bytes of links; standalone IDs come later.
    size_t estimate=bytes/20+1, packed_slots=3*estimate+65536, named_slots=3*estimate/8+65536;
    struct Edge { uint32_t a,b; };
    std::vector<std::vector<SV>> names(T);
    std::vector<std::vector<Edge>> edges(T);
    auto work=chunks(files,1024*1024);
    for (;;) {
        directory.allocate(packed_slots,named_slots);
        // Pre-sized: about one new ID and one line per 20 bytes of links.
        for (auto& list:names) { list.clear(); list.reserve(estimate/T*3/2+64); }
        for (auto& list:edges) { list.clear(); list.reserve(estimate/T*3/2+64); }
        #pragma omp parallel
        {
            const unsigned t=unsigned(omp_get_thread_num());
            auto& local_names=names[t]; auto& local_edges=edges[t];
            auto add=[&](SV s) {
                auto make=[&] {
                    uint64_t value=uint64_t(local_names.size())*T+t;
                    if (value>=0x80000000ULL) throw std::runtime_error("too many linked IDs");
                    local_names.push_back(s); return uint32_t(value);
                };
                uint64_t packed=packed_id(s);
                return packed ? directory.find_or_add(packed,integer_hash(packed),make) : directory.find_or_add(s,Hash{}(s),make);
            };
            #pragma omp for schedule(dynamic)
            for (size_t i=0;i<work.size();++i) {
                if (directory.overflowed.load(std::memory_order_relaxed)) continue;
                scan_csv<2>(work[i].view(),[&](const std::array<SV,2>& f) {
                    if (f[0].empty() || f[1].empty()) return;
                    uint32_t a=add(f[0]),b=add(f[1]);
                    local_edges.push_back({a,b});
                });
            }
        }
        if (!directory.overflowed) break;
        if (directory.overflowed & Directory::PACKED_FULL) packed_slots=directory.packed.size()*8;
        if (directory.overflowed & Directory::NAMED_FULL) named_slots=directory.named.size()*8;
    }
    std::vector<uint32_t> offset(T+1,0);
    for (unsigned t=0;t<T;++t) offset[t+1]=offset[t]+uint32_t(names[t].size());
    const uint32_t size=offset[T];
    auto dense=[&](uint32_t value) { return offset[value%T]+value/T; };
    people.names.resize(size); people.parent.resize(size);
    #pragma omp parallel for schedule(static,1)
    for (unsigned t=0;t<T;++t) {
        std::copy(names[t].begin(),names[t].end(),people.names.begin()+offset[t]);
        std::iota(people.parent.begin()+offset[t],people.parent.begin()+offset[t+1],offset[t]);
        std::vector<SV>().swap(names[t]);
    }
    auto root=[&](uint32_t p) {
        for (;;) {
            std::atomic_ref<uint32_t> ref(people.parent[p]);
            uint32_t q=ref.load(std::memory_order_relaxed);
            if (q==p) return p;
            uint32_t r=std::atomic_ref<uint32_t>(people.parent[q]).load(std::memory_order_relaxed);
            ref.compare_exchange_weak(q,r,std::memory_order_relaxed); p=q;
        }
    };
    // Hook a larger root below a smaller root with CAS. Parent IDs strictly
    // decrease, so concurrent unions cannot introduce cycles.
    #pragma omp parallel for schedule(dynamic)
    for (unsigned t=0;t<T;++t) {
        for (const auto& edge:edges[t]) {
            uint32_t a=dense(edge.a),b=dense(edge.b);
            for (;;) {
                uint32_t x=root(a),y=root(b);
                if (x==y) break;
                if (x<y) std::swap(x,y);
                uint32_t expected=x;
                if (std::atomic_ref<uint32_t>(people.parent[x]).compare_exchange_weak(expected,y,std::memory_order_relaxed)) break;
            }
        }
        std::vector<Edge>().swap(edges[t]);
    }
    std::vector<uint32_t> roots(size),representative(size);
    std::iota(representative.begin(),representative.end(),0U);
    #pragma omp parallel for
    for (uint32_t i=0;i<size;++i) roots[i]=root(i);
    people.parent=std::move(roots);
    #pragma omp parallel for
    for (uint32_t i=0;i<size;++i) {
        std::atomic_ref<uint32_t> ref(representative[people.parent[i]]);
        uint32_t current=ref.load(std::memory_order_relaxed);
        while (people.names[i]<people.names[current] && !ref.compare_exchange_weak(current,i,std::memory_order_relaxed)) {}
    }
    #pragma omp parallel for
    for (uint32_t i=0;i<size;++i) if (people.parent[i]==i) people.names[i]=people.names[representative[i]];
    // Seal: every directory value becomes its person's root.
    #pragma omp parallel for schedule(static)
    for (size_t i=0;i<directory.packed.size();++i) if (directory.packed[i].key) directory.packed[i].value=people.parent[dense(directory.packed[i].value)];
    #pragma omp parallel for schedule(static)
    for (size_t i=0;i<directory.named.size();++i) if (directory.named[i].hash) directory.named[i].value=people.parent[dense(directory.named[i].value)];
}
unsigned person_bucket(uint32_t p) {
    return integer_hash(p)&(PARTS-1);
}
void read_events(const std::vector<SV>& files, Buckets<Raw>& raw, People& people, const Dimensions& dim, int64_t begin, int64_t end) {
    auto work = chunks(files);
    const size_t rows_estimate=input_bytes(files)/80;
    // Raw::ingest is an unsigned second count centred on the window start.
    const int64_t INGEST_BASE=begin/1000-(int64_t(1)<<31);
    #pragma omp parallel
    {
        Local<Raw> local;
        presize(local,rows_estimate);
        const unsigned thread=unsigned(omp_get_thread_num());
        struct Pending { Raw row; ResolveRequest request; unsigned partition; };
        // Lookups are prefetched as rows parse and resolved 32 at a time.
        std::array<Pending,32> pending;
        size_t pending_count=0;
        auto settle=[&](Pending& item) {
            if (!item.request.name.empty()) item.row.person=people.resolve(item.request,thread);
            local[item.partition].push_back(item.row);
        };
        auto retire=[&] {
            for (size_t i=0;i<pending_count;++i) settle(pending[i]);
            pending_count=0;
        };
        auto enqueue=[&](const Pending& item) {
            if (pending_count==pending.size()) retire();
            pending[pending_count++]=item;
        };
        #pragma omp for schedule(dynamic) nowait
        for (size_t j=0; j<work.size(); ++j) {
            if (people.directory.overflowed.load(std::memory_order_relaxed)) continue;
            scan_csv<7>(work[j].view(), [&](const std::array<SV,7>& f) {
                if (f[0].empty()) return;
                int64_t ingest;
                if (!timestamp(f[3], ingest)) throw std::runtime_error("invalid guaranteed ingest_ts");
                int64_t ingest_seconds=ingest/1000-INGEST_BASE;
                if (ingest%1000 || ingest_seconds<0 || ingest_seconds>int64_t(UINT32_MAX)) throw std::runtime_error("ingest_ts outside supported range");
                uint64_t packed=packed_id(f[0]);
                size_t id_hash=event_id_hash(f[0],packed);
                Raw row{Raw::encode(f[0],packed),uint32_t(ingest_seconds),uint32_t(id_hash>>32),NONE,0,0,0};
                ResolveRequest request;
                // A semantically invalid copy remains a dedup candidate.
                // Its NONE person is a tombstone, never a discarded early row.
                int64_t ts; uint64_t price=0;
                bool valid=!f[1].empty() && !f[5].empty() && timestamp(f[2],ts) && ts>=begin && ts<end;
                valid=valid && (f[4]=="view" || f[4]=="cart" || f[4]=="purchase");
                if (valid && f[4]=="purchase") valid=digits(f[6],10000000,price) && price;
                if (valid && ingest<=ts+DAY) {
                    row.ts=uint32_t(ts-begin); row.price=uint32_t(price);
                    if (price) { auto p=dim.product.find(f[5]); if (p!=dim.product.end()) row.category=p->second; }
                    request=people.prepare(f[1]);
                }
                enqueue({row,request,unsigned(id_hash & (PARTS-1))});
            });
            retire();
        }
        flush(local,raw);
    }
}
// Per partition: an open-addressing table on the stored fingerprint keeps, for each
// ID, the copy with the smallest ingest; every losing copy is marked NONE in place.
// Survivors (the kept, valid copies) then stream out in row order.
void dedup_validate(Buckets<Raw>& raw, Buckets<CompactEvent>& events) {
    size_t rows_estimate=0;
    for (const auto& bucket:raw) for (const auto& part:bucket.parts) rows_estimate+=part.size();
    #pragma omp parallel
    {
        Local<CompactEvent> local;
        presize(local,rows_estimate);
        TrivialBuffer<Raw> rows;
        struct Slot { uint32_t fingerprint=0,index=NONE; };
        std::vector<Slot> table;
        #pragma omp for schedule(dynamic) nowait
        for (unsigned j=0; j<PARTS; ++j) {
            raw[j].gather(rows);
            if (rows.size() >= UINT32_MAX) throw std::runtime_error("dedup partition exceeds index range");
            size_t capacity = 16; while (capacity < rows.size()*10/7+1) capacity *= 2;
            const size_t mask=capacity-1;
            table.assign(capacity,Slot{});
            constexpr uint32_t AHEAD=16;
            for (uint32_t i=0;i<rows.size();++i) {
                if (i+AHEAD<rows.size()) __builtin_prefetch(&table[rows[i+AHEAD].fingerprint & mask],1,3);
                auto& row=rows[i];
                size_t slot=row.fingerprint & mask;
                while (table[slot].index!=NONE && (table[slot].fingerprint!=row.fingerprint || !rows[table[slot].index].same_id(row))) slot=(slot+1)&mask;
                auto& entry=table[slot];
                if (entry.index==NONE) { entry={row.fingerprint,i}; continue; }
                auto& kept=rows[entry.index];
                if (row.ingest<kept.ingest) { kept.person=NONE; entry.index=i; }
                else row.person=NONE;
            }
            for (const auto& row:rows) if (row.person!=NONE)
                local[person_bucket(row.person)].push_back({(uint64_t(row.person)<<32)|row.ts,row.price,row.category});
        }
        flush(local, events);
    }
}
// Clicks only look IDs up. An ID absent after the event pass belongs to a person
// with no surviving events: its clicks can credit or assist nothing, so they drop.
void read_clicks(const std::vector<SV>& files, Buckets<Click>& clicks, const People& people, const Dimensions& dim, int64_t begin, int64_t end) {
    auto work=chunks(files);
    const int64_t base=begin-WEEK;
    const size_t rows_estimate=input_bytes(files)/50;
    #pragma omp parallel
    {
        Local<Click> local;
        presize(local,rows_estimate);
        struct Pending { Click click; ResolveRequest request; };
        std::array<Pending,32> pending;
        size_t pending_count=0;
        auto settle=[&](Pending& item) {
            uint32_t p=people.lookup(item.request);
            if (p==NONE) return;
            item.click.key|=uint64_t(p)<<32;
            local[person_bucket(p)].push_back(item.click);
        };
        auto retire=[&] {
            for (size_t i=0;i<pending_count;++i) settle(pending[i]);
            pending_count=0;
        };
        auto enqueue=[&](const Pending& item) {
            if (pending_count==pending.size()) retire();
            pending[pending_count++]=item;
        };
        #pragma omp for schedule(dynamic) nowait
        for (size_t j=0; j<work.size(); ++j) {
            scan_csv<4>(work[j].view(), [&](const std::array<SV,4>& f) {
                int64_t ts;
                if (f[0].empty() || f[1].empty() || !timestamp(f[3],ts) || ts < base || ts>=end) return;
                auto c=dim.campaign.find(f[2]); if (c==dim.campaign.end()) return;
                enqueue({Click{uint64_t(ts-base),f[0],c->second},people.prepare(f[1])});
            });
            retire();
        }
        flush(local,clicks);
    }
}

struct UserRow { uint32_t person; int32_t day; int64_t sessions=0, events=0, revenue=0; uint32_t category=NONE; };
struct CampaignRow { int64_t sessions=0, purchasing=0, revenue=0, assists=0;
    void merge(const CampaignRow& b) { sessions+=b.sessions; purchasing+=b.purchasing; revenue+=b.revenue; assists+=b.assists; }
};
using CampaignTotals = std::unordered_map<uint64_t,CampaignRow>;
uint64_t campaign_key(int32_t day, uint32_t campaign) { return (uint64_t(uint32_t(day))<<32) | campaign; }
struct CampaignAccumulator {
    std::vector<CampaignRow> dense;
    CampaignTotals sparse;
    int32_t first_day=0;
    size_t campaign_count=0,day_count=0;
    void configure(int32_t first,int32_t last,size_t campaigns,size_t workers) {
        first_day=first; campaign_count=campaigns; day_count=size_t(int64_t(last)-first+1);
        constexpr size_t BUDGET=64*1024*1024;
        size_t maximum=BUDGET/(sizeof(CampaignRow)*workers);
        if (day_count && campaign_count<=maximum/day_count) dense.resize(day_count*campaign_count);
    }
    CampaignRow& operator[](uint64_t key) {
        if (dense.empty()) return sparse[key];
        size_t day=size_t(int64_t(int32_t(key>>32))-first_day);
        return dense[day*campaign_count+uint32_t(key)];
    }
    template<class F> void each(F&& consume) const {
        if (dense.empty()) { for (const auto& [key,row]:sparse) consume(key,row); return; }
        for (size_t d=0;d<day_count;++d) for (uint32_t c=0;c<campaign_count;++c) {
            const auto& row=dense[d*campaign_count+c];
            if (row.sessions || row.assists) consume(campaign_key(int32_t(int64_t(first_day)+int64_t(d)),c),row);
        }
    }
};
struct Results { std::vector<UserRow> users; CampaignAccumulator campaigns; };

void aggregate(Buckets<CompactEvent>& events, Buckets<Click>& clicks, const Dimensions& dim, std::vector<Results>& results,int64_t base) {
    #pragma omp parallel
    {
        auto& result=results[omp_get_thread_num()];
        // About one user-day row per nine surviving events.
        { size_t rows=0; for (const auto& bucket:events) for (const auto& part:bucket.parts) rows+=part.size();
          result.users.reserve(rows/9/size_t(omp_get_num_threads())*3/2+64); }
        std::vector<uint32_t> counts(dim.campaigns.size(),0), active, positions(dim.campaigns.size());
        std::vector<int64_t> categories(dim.categories.size(),0);
        TrivialBuffer<CompactEvent> ev, scratch; TrivialBuffer<Click> cl, click_scratch;
        const int64_t click_base=base-WEEK;
        #pragma omp for schedule(dynamic)
        for (unsigned bucket=0; bucket<PARTS; ++bucket) {
            events[bucket].gather(ev); clicks[bucket].gather(cl);
            radix_sort64(ev,scratch);
            // By person and time; at one timestamp the last added click must be the
            // smallest ID, so runs of equal keys go to descending ID order.
            radix_sort64(cl,click_scratch);
            for (size_t a=0;a<cl.size();) {
                size_t b=a+1; while (b<cl.size() && cl[b].key==cl[a].key) ++b;
                if (b-a>1) std::sort(cl.begin()+a,cl.begin()+b,[](const Click& x,const Click& y) { return x.id>y.id; });
                a=b;
            }
            size_t ep=0, cp=0;
            while (ep<ev.size()) {
                uint32_t person=event_person(ev[ep]);
                size_t ee=ep+1; while (ee<ev.size() && event_person(ev[ee])==person) ++ee;
                while (cp<cl.size() && cl[cp].person()<person) ++cp;
                size_t ce=cp; while (ce<cl.size() && cl[ce].person()==person) ++ce;
                size_t left=cp, right=cp;
                UserRow user{person,day_of(event_time(ev[ep],base))};
                auto emit_user = [&] {
                    int64_t best=0; uint32_t top=NONE;
                    for (uint32_t cat=0; cat<categories.size(); ++cat) {
                        if (categories[cat]>best || (categories[cat] && categories[cat]==best && dim.categories[cat]<dim.categories[top])) { best=categories[cat]; top=cat; }
                        categories[cat]=0;
                    }
                    user.category=top; result.users.push_back(user);
                };
                while (ep<ee) {
                    int64_t start=event_time(ev[ep],base);
                    int32_t day=day_of(start);
                    if (day!=user.day) { emit_user(); user=UserRow{person,day}; }
                    size_t se=ep+1;
                    while (se<ee && event_time(ev[se],base)-event_time(ev[se-1],base)<=GAP) ++se;
                    int64_t revenue=0;
                    for (size_t e=ep; e<se; ++e) if (ev[e].price) { revenue+=ev[e].price; categories[ev[e].category]+=ev[e].price; }
                    ++user.sessions; user.events+=int64_t(se-ep); user.revenue+=revenue;
                    // Click duplicates are identical. Keeping their multiplicity changes
                    // neither the last-click choice nor the active campaign set.
                    while (right<ce && click_time(cl[right],click_base)<=start) {
                        uint32_t c=cl[right++].campaign;
                        if (counts[c]++==0) { positions[c]=uint32_t(active.size()); active.push_back(c); }
                    }
                    while (left<right && click_time(cl[left],click_base)<start-WEEK) {
                        uint32_t c=cl[left++].campaign;
                        if (--counts[c]==0) {
                            uint32_t pos=positions[c], last=active.back();
                            active[pos]=last; positions[last]=pos; active.pop_back();
                        }
                    }
                    uint32_t credited=left<right ? cl[right-1].campaign : 0;
                    auto& credit=result.campaigns[campaign_key(day,credited)];
                    ++credit.sessions; credit.purchasing+=(revenue>0); credit.revenue+=revenue;
                    for (uint32_t c : active) ++result.campaigns[campaign_key(day,c)].assists;
                    ep=se;
                }
                emit_user();
                for (uint32_t c : active) counts[c]=0;
                active.clear(); cp=ce;
            }
        }
    }
}

void check(const arrow::Status& status) { if (!status.ok()) throw std::runtime_error(status.ToString()); }
template<class T> T unwrap(arrow::Result<T> result) { if (!result.ok()) throw std::runtime_error(result.status().ToString()); return std::move(result).ValueOrDie(); }
template<class B> std::shared_ptr<arrow::Array> finish(B& builder) { return unwrap(builder.Finish()); }
void parquet_write(const fs::path& file, const std::shared_ptr<arrow::Table>& table) {
    auto out=unwrap(arrow::io::FileOutputStream::Open(file.string()));
    parquet::WriterProperties::Builder props;
    // Uncompressed, without statistics. Dictionaries stay for the low-cardinality
    // columns, which they shrink, but not for user_id, whose names are nearly all
    // distinct. Any encoding satisfies the output contract.
    props.compression(parquet::Compression::UNCOMPRESSED);
    props.disable_dictionary("user_id");
    props.disable_statistics();
    check(parquet::arrow::WriteTable(*table,arrow::default_memory_pool(),out,65536,props.build()));
    check(out->Close());
}
void write_users(const fs::path& file,const std::vector<UserRow>& rows,const People& people,const Dimensions& dim) {
    arrow::StringBuilder user, category;
    arrow::Date32Builder day;
    arrow::Int64Builder sessions, events, revenue;
    for (auto* b : std::vector<arrow::ArrayBuilder*>{&user,&category,&day,&sessions,&events,&revenue}) check(b->Reserve(int64_t(rows.size())));
    for (const auto& row : rows) {
        check(user.Append(people.name(row.person))); check(day.Append(row.day));
        check(sessions.Append(row.sessions)); check(events.Append(row.events)); check(revenue.Append(row.revenue));
        if (row.category==NONE) check(category.AppendNull()); else check(category.Append(dim.categories[row.category]));
    }
    auto schema=arrow::schema({arrow::field("user_id",arrow::utf8(),false),arrow::field("day",arrow::date32(),false),arrow::field("sessions",arrow::int64(),false),arrow::field("events",arrow::int64(),false),arrow::field("revenue_cents",arrow::int64(),false),arrow::field("top_category",arrow::utf8(),true)});
    parquet_write(file,arrow::Table::Make(schema,{finish(user),finish(day),finish(sessions),finish(events),finish(revenue),finish(category)}));
}
void write_campaigns(const fs::path& file,const CampaignTotals& rows,const Dimensions& dim) {
    arrow::StringBuilder campaign, channel;
    arrow::Date32Builder day;
    arrow::Int64Builder sessions,purchasing,revenue,assists;
    for (const auto& [key,row] : rows) {
        uint32_t c=uint32_t(key);
        check(campaign.Append(dim.campaigns[c])); check(channel.Append(dim.channels[c])); check(day.Append(int32_t(key>>32)));
        check(sessions.Append(row.sessions)); check(purchasing.Append(row.purchasing)); check(revenue.Append(row.revenue)); check(assists.Append(row.assists));
    }
    auto schema=arrow::schema({arrow::field("campaign_id",arrow::utf8(),false),arrow::field("channel",arrow::utf8(),false),arrow::field("day",arrow::date32(),false),arrow::field("sessions",arrow::int64(),false),arrow::field("purchasing_sessions",arrow::int64(),false),arrow::field("revenue_cents",arrow::int64(),false),arrow::field("assisted_sessions",arrow::int64(),false)});
    parquet_write(file,arrow::Table::Make(schema,{finish(campaign),finish(channel),finish(day),finish(sessions),finish(purchasing),finish(revenue),finish(assists)}));
}
SV manifest_value(SV text,SV key) {
    size_t p=text.find(std::string("\"")+std::string(key)+"\"");
    if (p==SV::npos) throw std::runtime_error("missing manifest key");
    p=text.find(':',p); p=text.find('"',p);
    if (p==SV::npos) throw std::runtime_error("bad manifest");
    size_t end=text.find('"',p+1);
    if (end==SV::npos) throw std::runtime_error("bad manifest");
    return text.substr(p+1,end-p-1);
}
std::string env(const char* key,const char* fallback) { const char* s=std::getenv(key); return s ? s : fallback; }
int main() {
    try {
        // Keep large realloc-backed payload buffers eligible for Linux mremap.
        mallopt(M_MMAP_THRESHOLD,128*1024);
        auto start=std::chrono::steady_clock::now();
        auto timing=[&](const char* phase) { std::cerr<<phase<<": "<<std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count()<<"s\n"; };
        unsigned ncpu=std::clamp(std::stoi(env("NCPU","8")),1,256);
        omp_set_dynamic(0); omp_set_num_threads(int(ncpu));
        fs::path data=env("DATA_DIR","/data"), output=env("OUTPUT_DIR","/output");
        Inputs inputs; Dimensions dim; People people;
        SV manifest=inputs.map(data/"manifest.json"); int64_t begin,end;
        if (!timestamp(manifest_value(manifest,"window_start"),begin) || !timestamp(manifest_value(manifest,"window_end"),end) || begin>=end) throw std::runtime_error("invalid manifest window");
        // Event and click times are 32-bit offsets from window_start - 7 days.
        if (end-begin+WEEK>int64_t(UINT32_MAX)+1) throw std::runtime_error("window plus 7 days longer than 2^32 ms is unsupported");
        dim.load(inputs,data);
        build_links(inputs.csvs(data/"links"),people);
        timing("dimensions + links");
        // A pass that overflowed the directory resolved some IDs to a placeholder;
        // drop its rows and repeat it with a larger directory.
        auto until_fits=[&]<class T>(Buckets<T>& buckets,auto&& read) {
            for (;;) {
                read();
                if (!people.directory.overflowed) return;
                for (auto& bucket:buckets) bucket.parts.clear();
                people.directory.grow();
            }
        };
        auto eventfiles=inputs.csvs(data/"events"), clickfiles=inputs.csvs(data/"clicks");
        auto raw=std::make_unique<Buckets<Raw>>(); auto clicks=std::make_unique<Buckets<Click>>();
        until_fits(*raw,[&] { read_events(eventfiles,*raw,people,dim,begin,end); }); timing("CSV + validation + event partition");
        std::vector<Results> results(ncpu);
        for (auto& result:results) result.campaigns.configure(day_of(begin),day_of(end-1),dim.campaigns.size(),ncpu);
        auto events=std::make_unique<Buckets<CompactEvent>>();
        dedup_validate(*raw,*events); raw.reset(); timing("dedup + compact events");
        read_clicks(clickfiles,*clicks,people,dim,begin,end); timing("clicks");
        aggregate(*events,*clicks,dim,results,begin); timing("sessions + attribution + assists");
        fs::create_directories(output/"user_daily"); fs::create_directories(output/"campaign_daily");
        // Clear only visible Parquet outputs from an earlier run.
        for (const char* folder : {"user_daily","campaign_daily"}) for (auto& e : fs::directory_iterator(output/folder))
            if (e.is_regular_file() && e.path().extension()==".parquet" && e.path().filename().string()[0]!='.' && e.path().filename().string()[0]!='_') fs::remove(e.path());
        // Task ncpu merges and writes campaign_daily alongside the user_daily parts.
        #pragma omp parallel for schedule(dynamic,1)
        for (unsigned i=0; i<=ncpu; ++i) {
            if (i==ncpu) {
                CampaignTotals campaigns;
                for (const auto& r:results) r.campaigns.each([&](uint64_t key,const CampaignRow& row) { campaigns[key].merge(row); });
                write_campaigns(output/"campaign_daily"/"part-0.parquet",campaigns,dim);
            } else if (!results[i].users.empty() || i==0)
                write_users(output/"user_daily"/("part-"+std::to_string(i)+".parquet"),results[i].users,people,dim);
        }
        timing("Parquet complete");
        // Every output file is closed. Skip teardown: unmapping the inputs and
        // freeing every buffer costs tens of milliseconds and changes nothing.
        std::fflush(nullptr);
        std::_Exit(0);
    } catch (const std::exception& e) { std::cerr<<"fatal: "<<e.what()<<'\n'; return 1; }
}
