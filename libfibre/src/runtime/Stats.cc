/******************************************************************************
    Copyright (C) Martin Karsten 2015-2023

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
******************************************************************************/
#include "runtime/Basics.h"
#include "runtime/Stats.h"

#include <cstring>
#include <map>

namespace FredStats {

#if TESTING_ENABLE_STATISTICS

static EventScopeStats*  totalEventScopeStats  = nullptr;
static TimerStats*       totalTimerStats       = nullptr;
static PollerStats*      totalPollerStats      = nullptr;
static IOUringStats*     totalIOUringStats     = nullptr;
static ClusterStats*     totalClusterStats     = nullptr;
static IdleManagerStats* totalIdleManagerStats = nullptr;
static ProcessorStats*   totalProcessorStats   = nullptr;
static ReadyQueueStats*  totalReadyQueueStats  = nullptr;

static char statsListMemory[sizeof(IntrusiveQueue<Base>)];
static IntrusiveQueue<Base>* statsList = (IntrusiveQueue<Base>*)statsListMemory;

void StatsClear(int) {
  for (Base* o = statsList->front(); o != statsList->edge(); o = statsList->next(*o)) o->reset();
}

void StatsReset() {
  new (statsList) IntrusiveQueue<FredStats::Base>;
}

struct PrintStatsNode {
  cptr_t object;
  size_t sort;
  const char* name;
  bool operator<(const PrintStatsNode& x) const {
    if (object != x.object) return object < x.object;
    if (sort != x.sort) return sort < x.sort;
    return name[0] < x.name[0];
  }
};

typedef std::multimap<PrintStatsNode,const Base*> PrintStatsMap;

void PrintRecursive(const Base* o, size_t depth, ostream& os, PrintStatsMap& statsMap) {
  cptr_t next = nullptr;
  if (o) {
    next = o->object;
    for (size_t i = 0; i < depth; i += 1) os << ' ';
    o->print(os);
    os << std::endl;
    delete o;
    depth += 1;
  }
  for (;;) {
    auto iter = statsMap.lower_bound( {next, 0, ""} );
    if (iter == statsMap.end() || iter->first.object != next) break;
    o = iter->second;
    statsMap.erase(iter);
    PrintRecursive(o, depth, os, statsMap);
  }
}

void StatsPrint(ostream& os, bool totals) {
  char* env = getenv("FibrePrintStats");
  if (env) {
    PrintStatsMap statsMap;

    while (!statsList->empty()) {
      const Base* o = statsList->pop();
      statsMap.insert( {{o->parent, o->sort, o->name}, o} );
    }

    totalEventScopeStats  = new EventScopeStats (nullptr, nullptr, "EventScope ");
    totalPollerStats      = new PollerStats     (nullptr, nullptr, "Poller     ");
    totalIOUringStats     = new IOUringStats    (nullptr, nullptr, "IOUring    ");
    totalTimerStats       = new TimerStats      (nullptr, nullptr, "Timer      ");
    totalClusterStats     = new ClusterStats    (nullptr, nullptr, "Cluster    ");
    totalIdleManagerStats = new IdleManagerStats(nullptr, nullptr, "IdleManager");
    totalProcessorStats   = new ProcessorStats  (nullptr, nullptr, "Processor  ");
    totalReadyQueueStats  = new ReadyQueueStats (nullptr, nullptr, "ReadyQueue ");

    os << "LIBFIBRE STATS BEGIN ==========================================" << std::endl;
    PrintRecursive(nullptr, 0, os, statsMap);
    if (totals) {
      os << "TOTALS:" << std::endl;
      while (!statsList->empty()) {
        const Base* o = statsList->pop();
        o->print(os);
        os << std::endl;
        delete o;
      }
    }
    os << "LIBFIBRE STATS END ============================================" << std::endl;
  }
}

Base::Base(cptr_t const o, cptr_t const p, const char* const n, const size_t s) 
: object(o), parent(p), name(n), sort(s) { statsList->push(*this); }

Base::~Base() {}

void Base::reset() {}

void Base::print(ostream& os) const {
  os << name << ' ' << FmtHex(object);
}

void EventScopeStats::print(ostream& os) const {
  if (totalEventScopeStats && this != totalEventScopeStats) totalEventScopeStats->aggregate(*this);
  Base::print(os);
  os << " srvconn: " << srvconn << " cliconn: " << cliconn << " resets: " << resets << " calls: " << calls << " fails: " << fails;
}

void PollerStats::print(ostream& os) const {
  if (totalPollerStats && this != totalPollerStats) totalPollerStats->aggregate(*this);
  Base::print(os);
  os << " regs: " << regs << " eventsB:" << eventsB << " eventsNB:" << eventsNB;
}

void IOUringStats::print(ostream& os) const {
  if (totalIOUringStats && this != totalIOUringStats) totalIOUringStats->aggregate(*this);
  Base::print(os);
  os << " attempts:" << attempts << " submits:" << submits << " eventsB:" << eventsB << " eventsNB:" << eventsNB;
}

void TimerStats::print(ostream& os) const {
  if (totalTimerStats && this != totalTimerStats) totalTimerStats->aggregate(*this);
  Base::print(os);
  os << " events:" << events;
}

void ClusterStats::print(ostream& os) const {
  if (totalClusterStats && this != totalClusterStats) totalClusterStats->aggregate(*this);
  Base::print(os);
  os << " pause: " << pause;
}

void IdleManagerStats::print(ostream& os) const {
  if (totalIdleManagerStats && this != totalIdleManagerStats) totalIdleManagerStats->aggregate(*this);
  Base::print(os);
  os << " ready:" << ready << " blocked:" << blocked;
}

void ProcessorStats::print(ostream& os) const {
  if (totalProcessorStats && this != totalProcessorStats) totalProcessorStats->aggregate(*this);
  Base::print(os);
  os << " C: "  << create;
  os << " S: "  << start;
  os << " D: "  << deq;
  if (handover)     os << " H: "  << handover;
  if (borrow)       os << " B: "  << borrow;
  if (steal)        os << " S: "  << steal;
  os << " I: " << idle;
  os << " W: " << wake;
}

void ReadyQueueStats::print(ostream& os) const {
  if (totalReadyQueueStats && this != totalReadyQueueStats) totalReadyQueueStats->aggregate(*this);
  Base::print(os);
  os << queue;
}

#else

void StatsClear(int) {}
void StatsReset() {}

#endif /* TESTING_ENABLE_STATISTICS */

} // namespace FredStats
