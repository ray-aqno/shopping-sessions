#include "core.hpp"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <numeric>
#include <stdexcept>
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
std::vector<SV> chunks(const std::vector<SV>& files) {
    std::vector<SV> result;
    constexpr size_t CHUNK = 4 * 1024 * 1024;
    for (SV s : files) {
        size_t p = s.find('\n');
        if (p == SV::npos) continue;
        ++p;
        while (p < s.size()) {
            size_t e = std::min(s.size(), p + CHUNK);
            if (e < s.size()) { e = s.find('\n', e); if (e == SV::npos) e = s.size(); else ++e; }
            result.push_back(s.substr(p, e-p)); p = e;
        }
    }
    return result;
}

// Append-only, flat exact-string map. Only construction mutates it; readers are lock-free.
struct IdMap {
    struct Entry { SV key; uint32_t value = 0; };
    std::vector<Entry> slots = std::vector<Entry>(1024);
    size_t size = 0;
    uint32_t find(SV key, size_t hash) const {
        size_t i = hash & (slots.size()-1);
        while (slots[i].key.data()) {
            if (slots[i].key == key) return slots[i].value;
            i = (i+1) & (slots.size()-1);
        }
        return NONE;
    }
    uint32_t find(SV key) const { return find(key, Hash{}(key)); }
    void insert(SV key, uint32_t value) {
        if ((size+1)*10 > slots.size()*7) {
            auto old = std::move(slots); slots.resize(old.size()*2); size = 0;
            for (auto& e : old) if (e.key.data()) insert(e.key, e.value);
        }
        size_t i = Hash{}(key) & (slots.size()-1);
        while (slots[i].key.data()) i = (i+1) & (slots.size()-1);
        slots[i] = {key, value}; ++size;
    }
};
struct ResolveCache {
    struct Entry { SV key; uint32_t value=0; };
    std::vector<Entry> entries = std::vector<Entry>(32768);
};
struct People {
    IdMap linked;
    std::vector<uint32_t> parent, weight;
    std::vector<SV> names;
    struct Stripe { std::mutex mutex; Dict<uint32_t> ids; std::vector<SV> names; };
    std::array<Stripe, PARTS> standalone;
    uint32_t add_link_id(SV s) {
        uint32_t id = linked.find(s);
        if (id != NONE) return id;
        if (names.size() >= 0x80000000ULL) throw std::runtime_error("too many linked IDs");
        id = uint32_t(names.size()); names.push_back(s); parent.push_back(id); weight.push_back(1);
        linked.insert(s, id); return id;
    }
    uint32_t root(uint32_t a) {
        while (a != parent[a]) { parent[a] = parent[parent[a]]; a = parent[a]; }
        return a;
    }
    void join(SV a, SV b) {
        uint32_t x = root(add_link_id(a)), y = root(add_link_id(b));
        if (x == y) return;
        if (weight[x] < weight[y]) std::swap(x, y);
        parent[y] = x; weight[x] += weight[y]; names[x] = std::min(names[x], names[y]);
    }
    void seal() {
        for (auto& e : linked.slots) if (e.key.data()) e.value = root(e.value);
    }
    uint32_t resolve(SV s, ResolveCache& cache) {
        size_t hash=Hash{}(s);
        auto& cached=cache.entries[hash & (cache.entries.size()-1)];
        if (cached.key==s) return cached.value;
        uint32_t id = linked.find(s,hash);
        if (id != NONE) { cached={s,id}; return id; }
        unsigned shard = hash & (PARTS-1);
        auto& stripe = standalone[shard];
        std::lock_guard lock(stripe.mutex);
        auto it = stripe.ids.find(s);
        if (it != stripe.ids.end()) id = it->second;
        else {
            if (stripe.names.size() >= (0x80000000ULL / PARTS)) throw std::runtime_error("too many singleton IDs");
            id = 0x80000000U | (uint32_t(stripe.names.size()) * PARTS + shard);
            stripe.names.push_back(s); stripe.ids.emplace(s, id);
        }
        cached={s,id}; return id;
    }
    SV name(uint32_t id) const {
        if (!(id & 0x80000000U)) return names[id];
        return standalone[id & (PARTS-1)].names[(id & 0x7fffffffU) / PARTS];
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
struct Event { int64_t ts; uint32_t person, price, category; };
struct Raw {
    const char* id;
    int64_t ingest, ts;
    uint32_t id_size, person, price, category;
    SV key() const { return {id,id_size}; }
};
struct Click { int64_t ts; SV id; uint32_t person, campaign; };
template<class T> struct Bucket { std::mutex mutex; std::vector<T> rows; };
template<class T> using Buckets = std::array<Bucket<T>, PARTS>;
template<class T> void flush(std::array<std::vector<T>,PARTS>& local, Buckets<T>& buckets) {
    for (unsigned i=0; i<PARTS; ++i) if (!local[i].empty()) {
        auto& dst = buckets[i]; std::lock_guard lock(dst.mutex);
        dst.rows.insert(dst.rows.end(), local[i].begin(), local[i].end());
        local[i].clear();
    }
}
unsigned person_bucket(uint32_t p) {
    // Mix numeric roots, including the packed standalone IDs, before partitioning.
    p ^= p >> 16; p *= 0x7feb352dU; p ^= p >> 15; p *= 0x846ca68bU; return (p ^ (p >> 16)) & (PARTS-1);
}
void read_events(const std::vector<SV>& files, Buckets<Raw>& raw, People& people, const Dimensions& dim, int64_t begin, int64_t end) {
    auto work = chunks(files);
    #pragma omp parallel
    {
        std::array<std::vector<Raw>,PARTS> local;
        ResolveCache cache;
        #pragma omp for schedule(dynamic)
        for (size_t j=0; j<work.size(); ++j) {
            lines(work[j], [&](SV line) {
                std::array<SV,7> f;
                if (!csv(line,f) || f[0].empty()) return;
                int64_t ingest;
                if (!timestamp(f[3], ingest)) throw std::runtime_error("invalid guaranteed ingest_ts");
                if (f[0].size()>UINT32_MAX) throw std::runtime_error("event ID exceeds index range");
                Raw row{f[0].data(),ingest,0,uint32_t(f[0].size()),NONE,0,0};
                // A semantically invalid copy remains a dedup candidate.
                // Its NONE person is a tombstone, never a discarded early row.
                int64_t ts; uint64_t price=0;
                bool valid=!f[1].empty() && !f[5].empty() && timestamp(f[2],ts) && ts>=begin && ts<end;
                valid=valid && (f[4]=="view" || f[4]=="cart" || f[4]=="purchase");
                if (valid && f[4]=="purchase") valid=digits(f[6],10000000,price) && price;
                if (valid && ingest<=ts+DAY) {
                    row.ts=ts; row.price=uint32_t(price);
                    if (price) { auto p=dim.product.find(f[5]); if (p!=dim.product.end()) row.category=p->second; }
                    row.person=people.resolve(f[1],cache);
                }
                local[Hash{}(f[0]) & (PARTS-1)].push_back(row);
            }, false);
            flush(local,raw);
        }
    }
}
void dedup_validate(Buckets<Raw>& raw, Buckets<Event>& events) {
    #pragma omp parallel
    {
        std::array<std::vector<Event>,PARTS> local;
        #pragma omp for schedule(dynamic)
        for (unsigned j=0; j<PARTS; ++j) {
            auto& rows = raw[j].rows;
            if (rows.size() >= UINT32_MAX) throw std::runtime_error("dedup partition exceeds index range");
            size_t capacity = 16; while (capacity < rows.size()*10/7+1) capacity *= 2;
            std::vector<uint32_t> table(capacity, NONE);
            for (uint32_t i=0; i<rows.size(); ++i) {
                SV id = rows[i].key();
                size_t slot = (Hash{}(id) >> 8) & (capacity-1);
                while (table[slot] != NONE && rows[table[slot]].key() != id) slot = (slot+1) & (capacity-1);
                if (table[slot] == NONE || rows[i].ingest < rows[table[slot]].ingest) table[slot] = i;
            }
            for (uint32_t i : table) if (i!=NONE && rows[i].person!=NONE) {
                const auto& row=rows[i];
                local[person_bucket(row.person)].push_back({row.ts,row.person,row.price,row.category});
            }
            std::vector<Raw>().swap(rows);
            flush(local, events);
        }
    }
}
void read_clicks(const std::vector<SV>& files, Buckets<Click>& clicks, People& people, const Dimensions& dim, int64_t begin, int64_t end) {
    auto work=chunks(files);
    #pragma omp parallel
    {
        ResolveCache cache;
        std::array<std::vector<Click>,PARTS> local;
        #pragma omp for schedule(dynamic)
        for (size_t j=0; j<work.size(); ++j) {
            lines(work[j], [&](SV line) {
                std::array<SV,4> f; int64_t ts;
                if (!csv(line,f) || f[0].empty() || f[1].empty() || !timestamp(f[3],ts) || ts < begin-WEEK || ts>=end) return;
                auto c=dim.campaign.find(f[2]); if (c==dim.campaign.end()) return;
                uint32_t p=people.resolve(f[1],cache);
                local[person_bucket(p)].push_back({ts,f[0],p,c->second});
            },false);
            flush(local,clicks);
        }
    }
}

struct UserRow { uint32_t person; int32_t day; int64_t sessions=0, events=0, revenue=0; uint32_t category=NONE; };
struct CampaignRow { int64_t sessions=0, purchasing=0, revenue=0, assists=0;
    void merge(const CampaignRow& b) { sessions+=b.sessions; purchasing+=b.purchasing; revenue+=b.revenue; assists+=b.assists; }
};
using CampaignTotals = std::unordered_map<uint64_t,CampaignRow>;
uint64_t campaign_key(int32_t day, uint32_t campaign) { return (uint64_t(uint32_t(day))<<32) | campaign; }
struct Results { std::vector<UserRow> users; CampaignTotals campaigns; };

void aggregate(Buckets<Event>& events, Buckets<Click>& clicks, const Dimensions& dim, std::vector<Results>& results) {
    #pragma omp parallel
    {
        auto& result=results[omp_get_thread_num()];
        std::vector<uint32_t> counts(dim.campaigns.size(),0), active, positions(dim.campaigns.size());
        std::vector<int64_t> categories(dim.categories.size(),0);
        #pragma omp for schedule(dynamic)
        for (unsigned bucket=0; bucket<PARTS; ++bucket) {
            auto& ev=events[bucket].rows; auto& cl=clicks[bucket].rows;
            std::sort(ev.begin(),ev.end(),[](const Event& a,const Event& b) { return a.person!=b.person ? a.person<b.person : a.ts<b.ts; });
            // At a timestamp the last added click must be the smallest ID.
            std::sort(cl.begin(),cl.end(),[](const Click& a,const Click& b) {
                if (a.person!=b.person) return a.person<b.person;
                if (a.ts!=b.ts) return a.ts<b.ts;
                return a.id>b.id;
            });
            size_t ep=0, cp=0;
            while (ep<ev.size()) {
                uint32_t person=ev[ep].person;
                size_t ee=ep+1; while (ee<ev.size() && ev[ee].person==person) ++ee;
                while (cp<cl.size() && cl[cp].person<person) ++cp;
                size_t ce=cp; while (ce<cl.size() && cl[ce].person==person) ++ce;
                size_t left=cp, right=cp;
                UserRow user{person,day_of(ev[ep].ts)};
                auto emit_user = [&] {
                    int64_t best=0; uint32_t top=NONE;
                    for (uint32_t cat=0; cat<categories.size(); ++cat) {
                        if (categories[cat]>best || (categories[cat] && categories[cat]==best && dim.categories[cat]<dim.categories[top])) { best=categories[cat]; top=cat; }
                        categories[cat]=0;
                    }
                    user.category=top; result.users.push_back(user);
                };
                while (ep<ee) {
                    int64_t start=ev[ep].ts;
                    int32_t day=day_of(start);
                    if (day!=user.day) { emit_user(); user=UserRow{person,day}; }
                    size_t se=ep+1;
                    while (se<ee && ev[se].ts-ev[se-1].ts<=GAP) ++se;
                    int64_t revenue=0;
                    for (size_t e=ep; e<se; ++e) if (ev[e].price) { revenue+=ev[e].price; categories[ev[e].category]+=ev[e].price; }
                    ++user.sessions; user.events+=int64_t(se-ep); user.revenue+=revenue;
                    // Click duplicates are identical. Keeping their multiplicity changes
                    // neither the last-click choice nor the active campaign set.
                    while (right<ce && cl[right].ts<=start) {
                        uint32_t c=cl[right++].campaign;
                        if (counts[c]++==0) { positions[c]=uint32_t(active.size()); active.push_back(c); }
                    }
                    while (left<right && cl[left].ts<start-WEEK) {
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
            std::vector<Event>().swap(ev); std::vector<Click>().swap(cl);
        }
    }
}

void check(const arrow::Status& status) { if (!status.ok()) throw std::runtime_error(status.ToString()); }
template<class T> T unwrap(arrow::Result<T> result) { if (!result.ok()) throw std::runtime_error(result.status().ToString()); return std::move(result).ValueOrDie(); }
template<class B> std::shared_ptr<arrow::Array> finish(B& builder) { return unwrap(builder.Finish()); }
void parquet_write(const fs::path& file, const std::shared_ptr<arrow::Table>& table) {
    auto out=unwrap(arrow::io::FileOutputStream::Open(file.string()));
    parquet::WriterProperties::Builder props;
    props.compression(parquet::Compression::SNAPPY);
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
        auto start=std::chrono::steady_clock::now();
        auto timing=[&](const char* phase) { std::cerr<<phase<<": "<<std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count()<<"s\n"; };
        unsigned ncpu=std::clamp(std::stoi(env("NCPU","8")),1,256);
        omp_set_dynamic(0); omp_set_num_threads(int(ncpu));
        fs::path data=env("DATA_DIR","/data"), output=env("OUTPUT_DIR","/output");
        Inputs inputs; Dimensions dim; People people;
        SV manifest=inputs.map(data/"manifest.json"); int64_t begin,end;
        if (!timestamp(manifest_value(manifest,"window_start"),begin) || !timestamp(manifest_value(manifest,"window_end"),end) || begin>=end) throw std::runtime_error("invalid manifest window");
        dim.load(inputs,data);
        for (SV file : inputs.csvs(data/"links")) lines(file,[&](SV line) { std::array<SV,2> f; if (csv(line,f) && !f[0].empty() && !f[1].empty()) people.join(f[0],f[1]); });
        people.seal(); timing("dimensions + links");
        auto eventfiles=inputs.csvs(data/"events"), clickfiles=inputs.csvs(data/"clicks");
        auto raw=std::make_unique<Buckets<Raw>>(); auto events=std::make_unique<Buckets<Event>>(); auto clicks=std::make_unique<Buckets<Click>>();
        read_events(eventfiles,*raw,people,dim,begin,end); timing("CSV + validation + event partition");
        dedup_validate(*raw,*events); raw.reset(); timing("dedup + compact events");
        read_clicks(clickfiles,*clicks,people,dim,begin,end); timing("clicks");
        std::vector<Results> results(ncpu);
        aggregate(*events,*clicks,dim,results); timing("sessions + attribution + assists");
        fs::create_directories(output/"user_daily"); fs::create_directories(output/"campaign_daily");
        // Clear only visible Parquet outputs from an earlier run.
        for (const char* folder : {"user_daily","campaign_daily"}) for (auto& e : fs::directory_iterator(output/folder))
            if (e.is_regular_file() && e.path().extension()==".parquet" && e.path().filename().string()[0]!='.' && e.path().filename().string()[0]!='_') fs::remove(e.path());
        #pragma omp parallel for schedule(dynamic)
        for (unsigned i=0; i<ncpu; ++i) if (!results[i].users.empty() || i==0)
            write_users(output/"user_daily"/("part-"+std::to_string(i)+".parquet"),results[i].users,people,dim);
        CampaignTotals campaigns;
        for (auto& r : results) for (auto& [key,row] : r.campaigns) campaigns[key].merge(row);
        write_campaigns(output/"campaign_daily"/"part-0.parquet",campaigns,dim);
        timing("Parquet complete");
        return 0;
    } catch (const std::exception& e) { std::cerr<<"fatal: "<<e.what()<<'\n'; return 1; }
}
