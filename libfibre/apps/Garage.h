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
#ifndef _Garage_h_
#define _Garage_h_ 1

// assume shim_cond/shim_mutex API is already defined

class Garage {
  struct Link {
    Link* next;
    void* arg;
    shim_cond_t cond;
    Link() { shim_cond_init(&cond); }
    ~Link() { shim_cond_destroy(&cond); }
  };
  shim_mutex_t lock;
  Link* stack;
  
public:
  Garage() : stack(nullptr) { shim_mutex_init(&lock); }
  ~Garage() { shim_mutex_destroy(&lock); }
  void* park() {
    Link link;
    shim_mutex_lock(&lock);
    link.next = stack;
    stack = &link;
    shim_cond_wait(&link.cond, &lock);
    return link.arg;
  }
  bool run(void* arg) {
    shim_mutex_lock(&lock);
    if (!stack) {
      shim_mutex_unlock(&lock);
      return false;
    }
    Link* link = stack;
    stack = link->next;
    shim_mutex_unlock(&lock);      // can unlock early...
    link->arg = arg;
    shim_cond_signal(&link->cond); // ...since cond is private
    return true;
  }
};

#endif /* _Garage_h_ */
