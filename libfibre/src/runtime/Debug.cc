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
#include "runtime/Debug.h"

#include <cstring>

size_t DBG::levels = 0;

void DBG::init(const char* options[], char* dstring, bool print) {
  levels = 0;
  bit_set(levels, Basic);
  char* wordstart = dstring;
  char* end = wordstart + strlen(dstring);
  for (;;) {
    char* wordend = strchr(wordstart, ',');
    if (wordend == nullptr) wordend = end;
    *wordend = 0;
    size_t level = -1;
    for (size_t i = 0; i < MaxLevel; ++i) {
      if (!strncmp(wordstart, options[i], wordend - wordstart)) {
        if (level == size_t(-1)) level = i;
        else {
          if (print) DBG::outl(Basic, "multiple matches for debug option: ", wordstart);
          goto nextoption;
        }
      }
    }
    if (level != size_t(-1)) {
      bit_set(levels, level);
      if (print) DBG::outl(Basic, "matched debug option: ", wordstart, '=', options[level]);
    } else {
      if (print) DBG::outl(Basic, "unmatched debug option: ", wordstart);
    }
nextoption:
  if (wordend == end) break;
    *wordend = ',';
    wordstart = wordend + 1;
  }
}
