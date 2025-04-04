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
#ifndef _RuntimeFred_h_
#define _RuntimeFred_h_ 1

#include "libfibre/Fibre.h"

inline void RuntimeStartFred(funcvoid3_t func, ptr_t arg1, ptr_t arg2, ptr_t arg3) {
  Context::CurrProcessor().stats->start.count();
  ptr_t p;
  try {
    funcptr3_t f3 = (funcptr3_t)(ptr_t)(func);
    p = (Fibre::ExitException*)f3(arg1, arg2, arg3);
  } catch (Fibre::ExitException* e) {
    p = e;
  }
  reinterpret_cast<Fibre*>(arg3)->finalize(p);
}

inline void RuntimePreFredSwitch(Fred& currFred, Fred& nextFred, _friend<Fred> fs) {
  Fibre& currFibre = reinterpret_cast<Fibre&>(currFred);
  Fibre& nextFibre = reinterpret_cast<Fibre&>(nextFred);
  currFibre.deactivate(nextFibre, fs);
  Context::setCurrFred(nextFibre, fs);
}

inline void RuntimePostFredSwitch(Fred& newFred, _friend<Fred> fs) {
  Fibre& newFibre = reinterpret_cast<Fibre&>(newFred);
  newFibre.activate(fs);
}

inline void RuntimeFredDestroy(Fred& prevFred, _friend<Fred> fs) {
  Fibre& prevFibre = reinterpret_cast<Fibre&>(prevFred);
  prevFibre.destroy(fs);
}

#endif /* _RuntimeFred_h_ */
