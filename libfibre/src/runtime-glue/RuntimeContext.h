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
#ifndef _RuntimeContext_h_
#define _RuntimeContext_h_ 1

#include "runtime/Basics.h"

class BaseProcessor;
class BaseThreadPoller;
class Cluster;
class EventScope;
class Fred;

// routine definitions are in Cluster.cc
namespace Context {

  // 'noinline' needed for TLS:
  // http://stackoverflow.com/questions/25673787/making-thread-local-variables-fully-volatile
  // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=66631

  // CurrFred() and CurrProcessor() needed for generic runtime code
  Fred*          CurrFred()       __no_inline;
  BaseProcessor& CurrProcessor()  __no_inline;
  // CurrCluster(), CurrEventScope() only used in libfibre code
  Cluster&       CurrCluster()    __no_inline;
  EventScope&    CurrEventScope() __no_inline;

  // setCurrFred() to update current fred
  void setCurrFred(Fred& f, _friend<Fred>);

  // install context
  void install(BaseProcessor* bp, Cluster* cl, EventScope* es, _friend<Cluster>);
  void installFake(EventScope* es, _friend<BaseThreadPoller>);
}

#endif /* _RuntimeContext_h_ */
