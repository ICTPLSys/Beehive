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
#ifndef _Debug_h_
#define _Debug_h_ 1

#include "runtime/Bitmap.h"

class DBG {
  static size_t levels;
public:
  enum Level : size_t;
  static void init(const char* options[], char* dstring, bool print);
  static inline bool test(size_t c) { return bit_tst(levels, c); }
  template<typename... Args> static inline void out1(Level c, const Args&... a);
  template<typename... Args> static inline void outs(Level c, const Args&... a);
  template<typename... Args> static inline void outl(Level c, const Args&... a);
                             static inline void outl(Level c);
};

#include "runtime-glue/RuntimeDebug.h"

#endif /* _Debug_h_ */
