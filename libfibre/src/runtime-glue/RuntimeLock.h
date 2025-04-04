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
#ifndef _RuntimeLock_h_
#define _RuntimeLock_h_ 1

#include "runtime/SpinLocks.h"
#include "libfibre/OsLocks.h"

#if defined(WORKER_LOCK_TYPE)
typedef WORKER_LOCK_TYPE WorkerLock;
#else
typedef OsLock<0,0,0> WorkerLock;
#endif
typedef OsSemaphore   WorkerSemaphore;

#endif /* _RuntimeLock_h_ */
