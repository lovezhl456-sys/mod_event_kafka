// Real core opens tool-produced databases; test fixture operations only.
#include "kafka_outbox.hpp"
#include <iostream>
#include <stdexcept>
using namespace event_kafka;
#define CHECK(x) do { if (!(x)) throw std::runtime_error(#x + std::string(": ") + e); } while (0)
int main(int argc, char** argv) {
 try {
  if (argc != 3) return 2;
  std::string mode=argv[1], e;
  Outbox b(argv[2],mode=="migrated"?10:1,100000);
  CHECK(b.open(e));
  if (mode=="migrated") {
   CHECK(b.requeue_in_flight(e));
   CHECK(b.fetch_due(wall_now_ms()+1000,10).empty()); // imported dead head remains barrier
   CHECK(b.retry_dead("b",e));
   const char* ids[]={"b","c","a"};
   for(int n=0;n<3;++n) {
    auto due=b.fetch_due(wall_now_ms()+1000,10);
    CHECK(due.size()==1); CHECK(due[0].event_id==ids[n]); CHECK(due[0].call_seq==n);
    CHECK(b.mark_in_flight(ids[n],e)); CHECK(b.mark_acked(ids[n],e));
   }
   OutboxRecord r; r.event_id="new";r.topic="t";r.msg_key="k";r.call_uuid="call";r.payload="new";
   int64_t seq=-1;CHECK(b.insert_pending(r,e,&seq));CHECK(seq==3);
  } else {
   OutboxRecord r; r.event_id="closed-0";r.topic="t";r.msg_key="k";r.call_uuid="closed";r.payload="p";
   if(mode=="seed") {
    CHECK(b.insert_pending(r,e)); CHECK(b.mark_in_flight(r.event_id,e));CHECK(b.mark_acked(r.event_id,e));
   } else if(mode=="retired") {
    CHECK(!b.insert_pending(r,e));CHECK(e.find("retired")!=std::string::npos);
    r.event_id="different-0";r.call_uuid="different";
    CHECK(b.insert_pending(r,e)); // released capacity can be used by a different ID
   } else return 2;
  }
  std::cout<<mode<<" PASS\n";
 }catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}
}
