#include "core.hpp"
#include "csv_scan.hpp"
#include "input_chunks.hpp"
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <filesystem>
#include <iostream>

using namespace shop;
namespace fs=std::filesystem;
struct Map { void* p=MAP_FAILED; size_t n=0; int fd=-1; explicit Map(const fs::path& f){ fd=open(f.c_str(),O_RDONLY); if(fd<0) throw std::runtime_error("open"); struct stat st{}; fstat(fd,&st); n=st.st_size; if(n) p=mmap(nullptr,n,PROT_READ,MAP_PRIVATE,fd,0); if(p==MAP_FAILED) throw std::runtime_error("mmap"); }
 ~Map(){ if(p!=MAP_FAILED && n) munmap(p,n); if(fd>=0) close(fd); }};
struct File { std::unique_ptr<Map> map; SV bytes; };
int main(int argc,char** argv){
 try {
  if(argc<3){ std::cerr<<"usage: throughput DATA_DIR sum|fnv|scan|parse [threads]\n"; return 2; }
  fs::path dir=argv[1]; std::string mode=argv[2]; int nt=argc>3?std::stoi(argv[3]):8; omp_set_dynamic(0); omp_set_num_threads(nt);
  std::vector<fs::path> paths; for(auto const& e:fs::directory_iterator(dir/"events")) if(e.is_regular_file()&&e.path().extension()==".csv") paths.push_back(e.path()); std::sort(paths.begin(),paths.end());
  std::vector<File> files; uint64_t total_bytes=0; for(auto const& p:paths){ File f; f.map=std::make_unique<Map>(p); f.bytes={static_cast<const char*>(f.map->p),f.map->n}; total_bytes+=f.bytes.size(); files.push_back(std::move(f)); }
  std::vector<InputChunk> work; for(auto const& f:files){ auto c=chunks(std::vector<SV>{f.bytes}); work.insert(work.end(),c.begin(),c.end()); }
  uint64_t rows=0,ts_ok=0,id_ok=0,checksum=0,processed_bytes=0;
  auto t0=std::chrono::steady_clock::now();
  if(mode=="sum" || mode=="fnv") {
   #pragma omp parallel for reduction(+:checksum,processed_bytes) schedule(static)
   for(size_t j=0;j<files.size();++j){ auto s=files[j].bytes; uint64_t h=0; for(unsigned char c:s) h=mode=="sum" ? h+c : (h^c)*1099511628211ULL; checksum+=h; processed_bytes+=s.size(); }
  } else if(mode=="scan" || mode=="parse") {
   #pragma omp parallel for reduction(+:rows,ts_ok,id_ok,processed_bytes) schedule(static)
   for(size_t j=0;j<work.size();++j){
     auto input=work[j].view(); processed_bytes+=input.size();
     scan_csv<7>(input,[&](const std::array<SV,7>& f){ ++rows; if(mode=="parse"){ int64_t ts,ing; if(timestamp(f[2],ts)&&timestamp(f[3],ing)) ++ts_ok; if(!f[0].empty()&&!f[1].empty()&&packed_id(f[0])&&packed_id(f[1])) ++id_ok; }});
   }
  } else throw std::runtime_error("unknown mode");
  auto sec=std::chrono::duration<double>(std::chrono::steady_clock::now()-t0).count();
  std::cout<<"mode="<<mode<<" files="<<files.size()<<" mapped_bytes="<<total_bytes<<" processed_bytes="<<processed_bytes<<" seconds="<<sec<<" MiB_s="<<(double(processed_bytes)/1048576/sec)<<" rows="<<rows<<" timestamps="<<ts_ok<<" packed_ids="<<id_ok<<" checksum="<<checksum<<"\n";
 } catch(std::exception const& e){ std::cerr<<e.what()<<"\n"; return 1; }
}
