/*
    170 163 134217427170 163 134217427;
MIT License

Copyright (c) 2021 Parallel Applications Modelling Group - GMAP
    GMAP website: https://gmap.pucrs.br

    Pontifical Catholic University of Rio Grande do Sul (PUCRS)
    Av. Ipiranga, 6681, Porto Alegre - Brazil, 90619-900

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

------------------------------------------------------------------------------

The original NPB 3.4.1 version was written in Fortran and belongs to:
    http://www.nas.nasa.gov/Software/NPB/

Authors of the Fortran code:
    E. Barszcz
    P. Frederickson
    A. Woo
    M. Yarrow

------------------------------------------------------------------------------

The serial C++ version is a translation of the original NPB 3.4.1
Serial C++ version: https://github.com/GMAP/NPB-CPP/tree/master/NPB-SER

Authors of the C++ code:
    Dalvan Griebler <dalvangriebler@gmail.com>
    Gabriell Araujo <hexenoften@gmail.com>
    Júnior Löff <loffjh@gmail.com>
*/
#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <thread>

#include "cache/cache.hpp"
#include "common/npb-CPP.hpp"
#include "data_structure/far_vector.hpp"
#include "npbparams.hpp"
#include "rdma/server.hpp"
#include "utils/control.hpp"
#include "utils/parallel.hpp"
#include "utils/perf.hpp"
#include "utils/profile.hpp"

using namespace FarLib;
using namespace FarLib::rdma;
constexpr size_t UthreadCount = 16;
constexpr size_t perthreadsize = 1;
#ifndef FORCE_INLINE
#define FORCE_INLINE inline __attribute__((always_inline))
#endif
// #define STANDALONE
static constexpr uint64_t NM =
    (2UL +
     (1UL
      << LM)); /* actual dimension including ghost cells for communications */
static constexpr uint64_t NV =
    (ONE * (2UL + (1 << NDIM1)) * (2UL + (1UL << NDIM2)) *
     (2UL + (1UL << NDIM3))); /* size of rhs array */
static constexpr uint64_t NR =
    (((NV + NM * NM + 5UL * NM + 7UL * LM + 6UL) / 7UL) *
     8UL); /* size of residual array \
            */
static constexpr uint64_t MAXLEVEL =
    (LT_DEFAULT + 1UL); /* maximum number of levels */
static constexpr uint64_t M =
    (NM + 1UL); /* set at m=1024, can handle cases up to 1024^3 case */
static constexpr uint64_t MM = (10UL);
static constexpr uint64_t A = 1220703125;  // 5 ** 13
static constexpr uint64_t X = (314159265.0);
static constexpr uint64_t T_INIT = 0;
static constexpr uint64_t T_BENCH = 1;
static constexpr uint64_t T_MG3P = 2;
static constexpr uint64_t T_PSINV = 3;
static constexpr uint64_t T_RESID = 4;
static constexpr uint64_t T_RESID2 = 5;
static constexpr uint64_t T_RPRJ3 = 6;
static constexpr uint64_t T_INTERP = 7;
static constexpr uint64_t T_NORM2 = 8;
static constexpr uint64_t T_COMM3 = 9;
static constexpr uint64_t T_LAST = 10;

static constexpr double r23 =
    (0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 *
     0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5 * 0.5);
static constexpr double r46 = (r23 * r23);
static constexpr uint64_t t23(2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 *
                              2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 *
                              2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0 * 2.0);
static constexpr uint64_t t46 = t23 * t23;

/* global variables */
#if defined(DO_NOT_ALLOCATE_ARRAYS_WITH_DYNAMIC_MEMORY_AND_AS_SINGLE_DIMENSION)
static int nx[MAXLEVEL + 1];
static int ny[MAXLEVEL + 1];
static int nz[MAXLEVEL + 1];
static int m1[MAXLEVEL + 1];
static int m2[MAXLEVEL + 1];
static int m3[MAXLEVEL + 1];
static int ir[MAXLEVEL + 1];
static int debug_vec[8];
static double u[NR];
static double v[NV];
static double r[NR];
#else
static int* nx = (int*)malloc(sizeof(int) * (MAXLEVEL + 1));
static int* ny = (int*)malloc(sizeof(int) * (MAXLEVEL + 1));
static int* nz = (int*)malloc(sizeof(int) * (MAXLEVEL + 1));
static int* m1 = (int*)malloc(sizeof(int) * (MAXLEVEL + 1));
static int* m2 = (int*)malloc(sizeof(int) * (MAXLEVEL + 1));
static int* m3 = (int*)malloc(sizeof(int) * (MAXLEVEL + 1));
static int* ir = (int*)malloc(sizeof(int) * (MAXLEVEL + 1));
static int* debug_vec = (int*)malloc(sizeof(int) * (8));
// static double(*u) = (double*)malloc(sizeof(double) * (NR));
// static double(*v) = (double*)malloc(sizeof(double) * (NV));
// static double(*r) = (double*)malloc(sizeof(double) * (NR));
#endif
using DoubleVec = FarVector<double>;
using DoubleVecIter = DoubleVec::lite_iterator;
using DoubleVecConstIter = DoubleVec::const_lite_iterator;

#define _pfor uthread::parallel_for_with_scope<1>
#define _pit(I, N) UthreadCount, (N), [&](size_t I, DereferenceScope& scope)

#define _fork_join(ID, ACCESS_START, ACCESS_END, TYPE, ITER_START, ITER_END)   \
    {                                                                          \
        const TYPE block =                                                     \
            ((ACCESS_END) - (ACCESS_START) + UthreadCount - 1) / UthreadCount; \
        _pfor(_pit(ID, UthreadCount) {                                           \
        const TYPE ITER_START = (ACCESS_START) + ID * block;                   \
        const TYPE ITER_END = std::min(static_cast<TYPE>(ACCESS_END),        \
            static_cast<TYPE>(ITER_START + block));                          \
        if (ITER_START >= ITER_END) {                                        \
            return;                                                          \
        }
#define _fork_join_end \
    });                \
    }
#define INIT_PREFETCH_ITER(IT, START, OFFSET, VEC, SCOPE) \
    uint64_t IT##_start = START;                          \
    uint64_t IT##_end = (START) + (OFFSET);               \
    IT = VEC.get_lite_iter(IT##_start, SCOPE, IT##_start, IT##_end);

#define DEFINE_PREFETCH_ITER(IT, START, OFFSET, VEC, SCOPE) \
    uint64_t IT##_start = START;                            \
    uint64_t IT##_end = (START) + (OFFSET);                 \
    auto IT = VEC.get_lite_iter(IT##_start, SCOPE, IT##_start, IT##_end);

#define DEFINE_PREFETCH_CONST_ITER(IT, START, OFFSET, VEC, SCOPE) \
    uint64_t IT##_start = START;                                  \
    uint64_t IT##_end = (START) + (OFFSET);                       \
    auto IT = VEC.get_const_lite_iter(IT##_start, SCOPE, IT##_start, IT##_end);

#define INIT_PREFETCH_CONST_ITER(IT, START, OFFSET, VEC, SCOPE) \
    uint64_t IT##_start = START;                                \
    uint64_t IT##_end = (START) + (OFFSET);                     \
    IT = VEC.get_const_lite_iter(IT##_start, SCOPE, IT##_start, IT##_end);

DoubleVec u_vec;
DoubleVec v_vec;
DoubleVec r_vec;
static int is1, is2, is3, ie1, ie2, ie3, lt, lb;
static boolean timeron;

static FORCE_INLINE int calculate_offset(int i1, int i2, int i3, int n1, int n2,
                                         int /* n3 */) {
    return i3 * n2 * n1 + i2 * n1 + i1;
}
template <typename IT>
static FORCE_INLINE void forward_iter(IT& it, int i1, int i2, int i3, int n1,
                                      int n2, int n3, DereferenceScope& scope) {
    int offset = calculate_offset(i1, i2, i3, n1, n2, n3);
    switch (offset) {
    case 1:
        it.next(scope);
        break;
    case -1:
        it.prev(scope);
        break;
    case 0:
        break;
    default:
        it.nextn(offset, scope);
    }
}

template <typename IT>
static FORCE_INLINE void forward_iter(IT& it, int i1, int i2, int i3, int n1,
                                      int n2, int n3, DereferenceScope& scope,
                                      __DMH__) {
    int offset = i3 * n2 * n1 + i2 * n1 + i1;
    switch (offset) {
    case 1:
        it.next(scope, __on_miss__);
        break;
    case -1:
        it.prev(scope, __on_miss__);
        break;
    case 0:
        break;
    default:
        it.nextn(offset, scope, __on_miss__);
    }
}

template <typename IT>
FORCE_INLINE void forward_iters_by_same_step(DereferenceScope& scope, int i1,
                                             int i2, int i3, int n1, int n2,
                                             int n3, IT& it) {
    forward_iter(it, i1, i2, i3, n1, n2, n3, scope);
}

template <typename IT, typename... ITs>
FORCE_INLINE void forward_iters_by_same_step(DereferenceScope& scope, int i1,
                                             int i2, int i3, int n1, int n2,
                                             int n3, IT& it, ITs&... its) {
    forward_iters_by_same_step(scope, i1, i2, i3, n1, n2, n3, it);
    forward_iters_by_same_step(scope, i1, i2, i3, n1, n2, n3, its...);
}

/* function prototypes */
static void bubble(double ten[][MM], int j1[][MM], int j2[][MM], int j3[][MM],
                   int m, int ind);
static double power(double a, int n);
static void setup(int* n1, int* n2, int* n3, int k);
/*
 * ---------------------------------------------------------------------
 * comm3 organizes the communication on all borders
 * ---------------------------------------------------------------------
 */
static void comm3_caller(DoubleVec& u, int n1, int n2, int n3, int offset) {
    if (timeron) {
        timer_start(T_COMM3);
    }
    struct Scope : public DereferenceScope {
        DoubleVecIter it0, it2;
        DoubleVecConstIter it1, it3;
        void pin() const override {
            it0.pin();
            it1.pin();
            it2.pin();
            it3.pin();
        }
        void unpin() const override {
            it0.unpin();
            it1.unpin();
            it2.unpin();
            it3.unpin();
        }
        Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
    };

    _fork_join(id, 1, n3 - 1, int64_t, i3_start, i3_end) {
        Scope scp(&scope);
        auto& it0 = scp.it0;
        auto& it1 = scp.it1;
        auto& it2 = scp.it2;
        auto& it3 = scp.it3;
        INIT_PREFETCH_ITER(it0,
                           calculate_offset(offset, 1, i3_start, n1, n2, n3),
                           (i3_end - i3_start) * n2 * n1 - n1, u, scp);
        INIT_PREFETCH_CONST_ITER(
            it1, calculate_offset(offset + n1 - 2, 1, i3_start, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1 - n1, u, scp);
        INIT_PREFETCH_ITER(
            it2, calculate_offset(offset + n1 - 1, 1, i3_start, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1 - n1, u, scp);
        INIT_PREFETCH_CONST_ITER(
            it3, calculate_offset(offset + 1, 1, i3_start, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1 - n1, u, scp);

        for (int64_t i3 = i3_start; i3 < i3_end; i3++) {
            for (int64_t i2 = 1; i2 < n2 - 1; i2++) {
                *it0 = *it1;
                *it2 = *it3;
                forward_iters_by_same_step(scp, 0, 1, 0, n1, n2, n3, it0, it1,
                                           it2, it3);
            }
            // turn (X, n2 - 1, i3) to (X, 1, i3 + 1)
            forward_iters_by_same_step(scp, 0, -(n2 - 1 - 1), 1, n1, n2, n3,
                                       it0, it1, it2, it3);
        }
    }
    _fork_join_end {}

    _fork_join(id, 1, n3 - 1, int64_t, i3_start, i3_end) {
        Scope scp(&scope);
        auto& it0 = scp.it0;
        auto& it1 = scp.it1;
        auto& it2 = scp.it2;
        auto& it3 = scp.it3;
        for (int i3 = i3_start; i3 < i3_end; i3++) {
            INIT_PREFETCH_ITER(it0, calculate_offset(offset, 0, i3, n1, n2, n3),
                               n1, u, scp);
            INIT_PREFETCH_CONST_ITER(
                it1, calculate_offset(offset, n2 - 2, i3, n1, n2, n3), n1, u,
                scp);
            INIT_PREFETCH_ITER(it2,
                               calculate_offset(offset, n2 - 1, i3, n1, n2, n3),
                               n1, u, scp);
            INIT_PREFETCH_CONST_ITER(
                it3, calculate_offset(offset, 1, i3, n1, n2, n3), n1, u, scp);

            for (int i1 = 0; i1 < n1; i1++) {
                *it0 = *it1;
                *it2 = *it3;
                forward_iters_by_same_step(scp, 1, 0, 0, n1, n2, n3, it0, it1,
                                           it2, it3);
            }
            // turn (n1, X, i3) to (0, X, i3 + 1)
            forward_iters_by_same_step(scp, -n1, 0, 1, n1, n2, n3, it0, it1,
                                       it2, it3);
        }
    }
    _fork_join_end {}

    _fork_join(id, 0, n2, int64_t, i2_start, i2_end) {
        Scope scp(&scope);
        auto& it0 = scp.it0;
        auto& it1 = scp.it1;
        auto& it2 = scp.it2;
        auto& it3 = scp.it3;
        INIT_PREFETCH_ITER(it0,
                           calculate_offset(offset, i2_start, 0, n1, n2, n3),
                           (i2_end - i2_start) * n1, u, scp);
        INIT_PREFETCH_CONST_ITER(
            it1, calculate_offset(offset, i2_start, n3 - 2, n1, n2, n3),
            (i2_end - i2_start) * n1, u, scp);
        INIT_PREFETCH_ITER(
            it2, calculate_offset(offset, i2_start, n3 - 1, n1, n2, n3),
            (i2_end - i2_start) * n1, u, scp);
        INIT_PREFETCH_CONST_ITER(
            it3, calculate_offset(offset, i2_start, 1, n1, n2, n3),
            (i2_end - i2_start) * n1, u, scp);
        for (int i2 = i2_start; i2 < i2_end; i2++) {
            for (int i1 = 0; i1 < n1; i1++) {
                *it0 = *it1;
                *it2 = *it3;
                forward_iters_by_same_step(scp, 1, 0, 0, n1, n2, n3, it0, it1,
                                           it2, it3);
            }
            forward_iters_by_same_step(scp, -n1, 1, 0, n1, n2, n3, it0, it1,
                                       it2, it3);
        }
    }
    _fork_join_end {}
    if (timeron) {
        timer_stop(T_COMM3);
    }
}

void vranlc_farlib(int n, double* x_seed, double a, DoubleVec& y_vec,
                   size_t y_offset, DereferenceScope& scope) {
    int i;
    double x, t1, t2, t3, t4, a1, a2, x1, x2, z;

    /*
     * ---------------------------------------------------------------------
     * break A into two parts such that A = 2^23 * A1 + A2.
     * ---------------------------------------------------------------------
     */
    t1 = r23 * a;
    a1 = (int)t1;
    a2 = a - t23 * a1;
    x = *x_seed;

    /*
     * ---------------------------------------------------------------------
     * generate N results. this loop is not vectorizable.
     * ---------------------------------------------------------------------
     */
    DEFINE_PREFETCH_ITER(y_it, y_offset, n, y_vec, scope);

    for (i = 0; i < n; i++) {
        /*
         * ---------------------------------------------------------------------
         * break X into two parts such that X = 2^23 * X1 + X2, compute
         * Z = A1 * X2 + A2 * X1  (mod 2^23), and then
         * X = 2^23 * Z + A2 * X2  (mod 2^46).
         * ---------------------------------------------------------------------
         */
        t1 = r23 * x;
        x1 = (int)t1;
        x2 = x - t23 * x1;
        t1 = a1 * x2 + a2 * x1;
        t2 = (int)(r23 * t1);
        z = t1 - t23 * t2;
        t3 = t23 * z + a2 * x2;
        t4 = (int)(r46 * t3);
        x = t3 - t46 * t4;
        *y_it = (r46 * x);
        y_it.next(scope);
    }
    *x_seed = x;
}

static void alloc_farlib(DoubleVec& z, int length) { z.resize(length); }

static void zero3_caller(DoubleVec& z, int n1, int n2, int n3, int off) {
    _fork_join(id, 0, n3, int64_t, i3_start, i3_end) {
        DEFINE_PREFETCH_ITER(it, calculate_offset(off, 0, i3_start, n1, n2, n3),
                             (i3_end - i3_start) * n2 * n1, z, scope);
        for (int i3 = i3_start; i3 < i3_end; i3++) {
            for (int i2 = 0; i2 < n2; i2++) {
                for (int i1 = 0; i1 < n1; i1++) {
                    *it = 0.0;
                    it.next(scope);
                }
            }
        }
    }
    _fork_join_end
}

static void zran3_caller(DoubleVec& z, int n1, int n2, int n3, int nx, int ny,
                         int k) {
    /* this function dont need a offset temporarily,
     for all caller of it dont have a offset of z*/
    double ten_global[UthreadCount][2][MM] = {0}, best;
    int j1_global[UthreadCount][2][MM] = {0},
        j2_global[UthreadCount][2][MM] = {0},
        j3_global[UthreadCount][2][MM] = {0};
    int jg[2][MM][4] = {0};

    const double a1 = power(A, nx);
    const double a2 = power(A, nx * ny);

    zero3_caller(z, n1, n2, n3, 0);

    const int i = is1 - 2 + nx * (is2 - 2 + ny * (is3 - 2));

    const double ai = power(A, i);
    const int d1 = ie1 - is1 + 1;
    // const int e1 = ie1 - is1 + 2;
    const int e2 = ie2 - is2 + 2;
    const int e3 = ie3 - is3 + 2;
    std::cout << "vlanc start" << std::endl;
    double x0 = X;
    randlc(&x0, ai);

    _fork_join(id, 1, e3, int64_t, i3_start, i3_end) {
        double x1 = x0;
        for (int i3 = i3_start; i3 < i3_end; i3++) {
            for (int i2 = 1; i2 < e2; i2++) {
                double xx = x1;
                vranlc_farlib(d1, &xx, A, z, 0, scope);
                randlc(&x1, a1);
            }
        }
        randlc(&x0, a2);
    }
    _fork_join_end {}  // TODO is it parallelize right?
    struct Element {
        double val;
        int j1, j2, j3;
    } elements[2][MM * UthreadCount];
    for (uint64_t ind = 0; ind < 2; ind++) {
        for (uint64_t i = 0; i < MM * UthreadCount; i++) {
            elements[ind][i].val =
                ten_global[i % UthreadCount][ind][i / UthreadCount];
            elements[ind][i].j1 =
                j1_global[i % UthreadCount][ind][i / UthreadCount];
            elements[ind][i].j2 =
                j2_global[i % UthreadCount][ind][i / UthreadCount];
            elements[ind][i].j3 =
                j3_global[i % UthreadCount][ind][i / UthreadCount];
        }
    }
    qsort(elements[1], sizeof(Element), MM * UthreadCount,
          [](const void* a, const void* b) -> int {
              const Element* e1 = static_cast<const Element*>(a);
              const Element* e2 = static_cast<const Element*>(b);
              return e1->val - e2->val;
          });
    qsort(elements[0], sizeof(Element), MM * UthreadCount,
          [](const void* a, const void* b) -> int {
              const Element* e1 = static_cast<const Element*>(a);
              const Element* e2 = static_cast<const Element*>(b);
              return e2->val - e1->val;
          });
    double ten[2][MM];
    int j1[2][MM], j2[2][MM], j3[2][MM];
    for (uint64_t ind = 0; ind < 2; ind++) {
        for (uint64_t i = 0; i < MM; i++) {
            ten[ind][i] = elements[ind][i].val;
            j1[ind][i] = elements[ind][i].j1;
            j2[ind][i] = elements[ind][i].j2;
            j3[ind][i] = elements[ind][i].j3;
        }
    }

    std::cout << "vlanc end" << std::endl;
    /*
     * ---------------------------------------------------------------------
     * each processor looks for twenty candidates
     * ---------------------------------------------------------------------
     */

    {
        _fork_join(id, 1, n3 - 1, int64_t, i3_start, i3_end) {
            auto* ten = ten_global[id];
            auto* j1 = j1_global[id];
            auto* j2 = j2_global[id];
            auto* j3 = j3_global[id];
            DEFINE_PREFETCH_CONST_ITER(
                it, calculate_offset(1, 1, i3_start, n1, n2, n3),
                (i3_end - i3_start) * n2 * n1, z, scope);
            for (int i3 = i3_start; i3 < i3_end; i3++) {
                for (int i2 = 1; i2 < n2 - 1; i2++) {
                    for (int i1 = 1; i1 < n1 - 1; i1++) {
                        double data = *it;
                        if (data > ten[1][0]) {
                            ten[1][0] = data;
                            j1[1][0] = i1;
                            j2[1][0] = i2;
                            j3[1][0] = i3;
                            bubble(ten, j1, j2, j3, MM, 1);
                        }
                        if (data < ten[0][0]) {
                            ten[0][0] = data;
                            j1[0][0] = i1;
                            j2[0][0] = i2;
                            j3[0][0] = i3;
                            bubble(ten, j1, j2, j3, MM, 0);
                        }
                        it.next(scope);
                    }
                    // turn (n1 - 1, i2, X) to (1, i2 + 1, X)
                    forward_iter(it, -(n1 - 1 - 1), 1, 0, n1, n2, n3, scope);
                }
                // turn (1, n2 - 1, i3) to (1, 1, i3 + 1)
                forward_iter(it, 0, -(n2 - 1 - 1), 1, n1, n2, n3, scope);
            }
        }
        _fork_join_end
    }

    int i1 = MM - 1;
    int i0 = MM - 1;
    for (int i = MM - 1; i >= 0; i--) {
        best = 0.0;
        if (best < ten[1][i1]) {
            jg[1][i][0] = 0;
            jg[1][i][1] = is1 - 2 + j1[1][i1];
            jg[1][i][2] = is2 - 2 + j2[1][i1];
            jg[1][i][3] = is3 - 2 + j3[1][i1];
            i1 = i1 - 1;
        } else {
            jg[1][i][0] = 0;
            jg[1][i][1] = 0;
            jg[1][i][2] = 0;
            jg[1][i][3] = 0;
        }
        best = 1.0;
        if (best > ten[0][i0]) {
            jg[0][i][0] = 0;
            jg[0][i][1] = is1 - 2 + j1[0][i0];
            jg[0][i][2] = is2 - 2 + j2[0][i0];
            jg[0][i][3] = is3 - 2 + j3[0][i0];
            i0 = i0 - 1;
        } else {
            jg[0][i][0] = 0;
            jg[0][i][1] = 0;
            jg[0][i][2] = 0;
            jg[0][i][3] = 0;
        }
    }
    int m1 = 0;
    int m0 = 0;

    zero3_caller(z, n1, n2, n3, 0);
    _fork_join(id, m0, MM, int64_t, istart, iend) {
        for (int i = istart; i < iend; i++) {
            DEFINE_PREFETCH_ITER(it,
                                 calculate_offset(jg[0][i][1], jg[0][i][2],
                                                  jg[0][i][3], n1, n2, n3),
                                 1, z, scope);
            *it = -1.0;
        }
    }
    _fork_join_end {}
    _fork_join(id, m1, MM, int64_t, istart, iend) {
        for (int i = istart; i < iend; i++) {
            DEFINE_PREFETCH_ITER(it,
                                 calculate_offset(jg[1][i][1], jg[1][i][2],
                                                  jg[1][i][3], n1, n2, n3),
                                 1, z, scope);
            *it = 1.0;
        }
    }
    _fork_join_end {}
    comm3_caller(z, n1, n2, n3, 0);
}

static void norm2u3_caller(DoubleVec& r, int n1, int n2, int n3, double* rnm2,
                           double* rnmu, int nx, int ny, int nz, int offset) {
    double dn;
    if (timeron) {
        timer_start(T_NORM2);
    }
    dn = 1.0 * nx * ny * nz;
    std::array<double, UthreadCount> sums({0.0});
    std::array<double, UthreadCount> maxas({0.0});
    uthread::parallel_for_with_scope<1, true>(
        UthreadCount, n3 - 1 - 1, [&](size_t i3_, DereferenceScope& scope) {
            auto i3 = i3_ + 1;
            DEFINE_PREFETCH_CONST_ITER(it,
                                       calculate_offset(1, 1, i3, n1, n2, n3),
                                       (n1 - 1 - 1) * n1, r, scope);
            double sum = 0.0;
            double maxa = 0.0;
            for (int i2 = 1; i2 < n2 - 1; i2++) {
                for (int i1 = 1; i1 < n1 - 1; i1++) {
                    sum += *it * *it;
                    maxa = std::max(maxa, fabs(*it));
                    forward_iter(it, 1, 0, 0, n1, n2, n3, scope);
                }
                forward_iter(it, -(n1 - 1 - 1), 1, 0, n1, n2, n3, scope);
            }
            sums[i3_ % UthreadCount] += sum;
            maxas[i3_ % UthreadCount] =
                std::max(maxa, maxas[i3_ % UthreadCount]);
        });
    double s = std::accumulate(sums.begin(), sums.end(), 0.0);
    *rnmu = *std::max_element(maxas.begin(), maxas.end());
    *rnm2 = sqrt(s / dn);
    if (timeron) {
        timer_stop(T_NORM2);
    }
}

static void resid_caller(DoubleVec& u, DoubleVec& v, DoubleVec& r, int n1,
                         int n2, int n3, double a[4], int k, int u_offset,
                         int v_offset, int r_offset) {
    if (timeron) {
        timer_start(T_RESID);
    }

    _fork_join(id, 1, n3 - 1, int64_t, i3_start, i3_end) {
        struct Scope : public DereferenceScope {
            DoubleVecConstIter it0, it1, it2, it3, it4, it5, it6, it7, itu, itv;
            DoubleVecIter itr;
            void pin() const override {
                it0.pin();
                it1.pin();
                it2.pin();
                it3.pin();
                it4.pin();
                it5.pin();
                it6.pin();
                it7.pin();
                itu.pin();
                itv.pin();
                itr.pin();
            }
            void unpin() const override {
                it0.unpin();
                it1.unpin();
                it2.unpin();
                it3.unpin();
                it4.unpin();
                it5.unpin();
                it6.unpin();
                it7.unpin();
                itu.unpin();
                itv.unpin();
                itr.unpin();
            }

            Scope(DereferenceScope* scope) : DereferenceScope(scope) {}

        } scp(&scope);
        double u1[M], u2[M];
        auto& it0 = scp.it0;
        auto& it1 = scp.it1;
        auto& it2 = scp.it2;
        auto& it3 = scp.it3;
        auto& it4 = scp.it4;
        auto& it5 = scp.it5;
        auto& it6 = scp.it6;
        auto& it7 = scp.it7;
        auto& itu = scp.itu;
        auto& itv = scp.itv;
        auto& itr = scp.itr;
        INIT_PREFETCH_CONST_ITER(
            it0, calculate_offset(u_offset, 1 - 1, i3_start, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, u, scp);
        INIT_PREFETCH_CONST_ITER(
            it1, calculate_offset(u_offset, 1 + 1, i3_start, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, u, scp);
        INIT_PREFETCH_CONST_ITER(
            it2, calculate_offset(u_offset, 1, i3_start - 1, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, u, scp);
        INIT_PREFETCH_CONST_ITER(
            it3, calculate_offset(u_offset, 1, i3_start + 1, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, u, scp);
        INIT_PREFETCH_CONST_ITER(
            it4, calculate_offset(u_offset, 1 - 1, i3_start - 1, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, u, scp);
        INIT_PREFETCH_CONST_ITER(
            it5, calculate_offset(u_offset, 1 + 1, i3_start - 1, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, u, scp);
        INIT_PREFETCH_CONST_ITER(
            it6, calculate_offset(u_offset, 1 - 1, i3_start + 1, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, u, scp);
        INIT_PREFETCH_CONST_ITER(
            it7, calculate_offset(u_offset, 1 + 1, i3_start + 1, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, u, scp);
        INIT_PREFETCH_CONST_ITER(
            itu, calculate_offset(u_offset + 1, 1, i3_start, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, u, scp);
        INIT_PREFETCH_CONST_ITER(
            itv, calculate_offset(v_offset + 1, 1, i3_start, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, v, scp);
        INIT_PREFETCH_ITER(
            itr, calculate_offset(r_offset + 1, 1, i3_start, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, r, scp);

        for (int i3 = i3_start; i3 < i3_end; i3++) {
            for (int i2 = 1; i2 < n2 - 1; i2++) {
                for (int i1 = 0; i1 < n1; i1++) {
                    u1[i1] = *it0 + *it1 + *it2 + *it3;
                    u2[i1] = *it4 + *it5 + *it6 + *it7;
                    forward_iters_by_same_step(scp, 1, 0, 0, n1, n2, n3, it0,
                                               it1, it2, it3, it4, it5, it6,
                                               it7);
                }
                forward_iters_by_same_step(scp, -n1, 1, 0, n1, n2, n3, it0, it1,
                                           it2, it3, it4, it5, it6, it7);
                for (int i1 = 1; i1 < n1 - 1; i1++) {
                    *itr = *itv - a[0] * *itu -
                           a[2] * (u2[i1] + u1[i1 - 1] + u1[i1 + 1]) -
                           a[3] * (u2[i1 - 1] + u2[i1 + 1]);
                    forward_iters_by_same_step(scp, 1, 0, 0, n1, n2, n3, itu,
                                               itv, itr);
                }
                forward_iters_by_same_step(scp, -(n1 - 1 - 1), 1, 0, n1, n2, n3,
                                           itu, itv, itr);
            }
            forward_iters_by_same_step(scp, 0, -(n2 - 1 - 1), 1, n1, n2, n3,
                                       it0, it1, it2, it3, it4, it5, it6, it7,
                                       itu, itv, itr);
        }
    }
    _fork_join_end {}

    if (timeron) {
        timer_stop(T_RESID);
    }

    /*
     * --------------------------------------------------------------------
     * exchange boundary data
     * --------------------------------------------------------------------
     */
    comm3_caller(r, n1, n2, n3, r_offset);
}

static void psinv_caller(DoubleVec& r, DoubleVec& u, int n1, int n2, int n3,
                         double c[4], int k, size_t r_off, size_t u_off) {
    if (timeron) {
        timer_start(T_PSINV);
    }
    _fork_join(id, 1, n3 - 1, int64_t, i3_start, i3_end) {
        double r1[M], r2[M];
        struct Scope : public DereferenceScope {
            DoubleVecConstIter it0, it1, it2, it3, it4, it5, it6, it7, itr;
            DoubleVecIter itu;

            void pin() const override {
                it0.pin();
                it1.pin();
                it2.pin();
                it3.pin();
                it4.pin();
                it5.pin();
                it6.pin();
                it7.pin();
                itr.pin();
                itu.pin();
            }

            void unpin() const override {
                it0.unpin();
                it1.unpin();
                it2.unpin();
                it3.unpin();
                it4.unpin();
                it5.unpin();
                it6.unpin();
                it7.unpin();
                itr.unpin();
                itu.unpin();
            }

            Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
        } scp(&scope);
        auto& it0 = scp.it0;
        auto& it1 = scp.it1;
        auto& it2 = scp.it2;
        auto& it3 = scp.it3;
        auto& it4 = scp.it4;
        auto& it5 = scp.it5;
        auto& it6 = scp.it6;
        auto& it7 = scp.it7;
        auto& itr = scp.itr;
        auto& itu = scp.itu;
        INIT_PREFETCH_CONST_ITER(
            it0, calculate_offset(r_off, 1 - 1, i3_start, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it1, calculate_offset(r_off, 1 + 1, i3_start, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it2, calculate_offset(r_off, 1, i3_start - 1, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it3, calculate_offset(r_off, 1, i3_start + 1, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it4, calculate_offset(r_off, 1 - 1, i3_start - 1, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it5, calculate_offset(r_off, 1 + 1, i3_start - 1, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it6, calculate_offset(r_off, 1 - 1, i3_start + 1, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it7, calculate_offset(r_off, 1 + 1, i3_start + 1, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, r, scp);
        INIT_PREFETCH_CONST_ITER(
            itr, calculate_offset(r_off, 1, i3_start, n1, n2, n3),
            (i3_end - i3_start) * n2 * n1, r, scp);
        INIT_PREFETCH_ITER(itu,
                           calculate_offset(u_off, 1, i3_start, n1, n2, n3),
                           (i3_end - i3_start) * n2 * n1, r, scp);

        for (int i3 = i3_start; i3 < i3_end; i3++) {
            for (int i2 = 1; i2 < n2 - 1; i2++) {
                for (int i1 = 0; i1 < n1; i1++) {
                    r1[i1] = *it0 + *it1 + *it2 + *it3;
                    r2[i1] = *it4 + *it5 + *it6 + *it7;
                    forward_iters_by_same_step(scp, 1, 0, 0, n1, n2, n3, it0,
                                               it1, it2, it3, it4, it5, it6,
                                               it7);
                }
                struct Scope2 : public DereferenceScope {
                    DoubleVecConstIter itun1, itup1;

                    void pin() const override {
                        itun1.pin();
                        itup1.pin();
                    }

                    void unpin() const override {
                        itun1.unpin();
                        itup1.unpin();
                    }

                    Scope2(DereferenceScope* scope) : DereferenceScope(scope) {}
                } scp2(&scp);
                auto& itun1 = scp2.itun1;
                auto& itup1 = scp2.itup1;
                INIT_PREFETCH_CONST_ITER(
                    itun1, calculate_offset(u_off - 1, 1, i3_start, n1, n2, n3),
                    (i3_end - i3_start) * n2 * n1, r, scp);
                INIT_PREFETCH_CONST_ITER(
                    itup1, calculate_offset(u_off + 1, 1, i3_start, n1, n2, n3),
                    (i3_end - i3_start) * n2 * n1, r, scp);
                for (int i1 = 1; i1 < n1 - 1; i1++) {
                    *itu += c[0] * *itr + c[1] * (*itun1 + *itup1 + r1[i1]) +
                            c[2] * (r2[i1] + r1[i1 - 1] + r1[i1 + 1]);
                    forward_iters_by_same_step(scp2, 1, 0, 0, n1, n2, n3, itu,
                                               itun1, itup1, itr);
                }
                forward_iters_by_same_step(scp, -n1, 1, 0, n1, n2, n3, it0, it1,
                                           it2, it3, it4, it5, it6, it7, itu,
                                           itr);
            }
            forward_iters_by_same_step(scp, 0, -(n2 - 1 - 1), 1, n1, n2, n3,
                                       it0, it1, it2, it3, it4, it5, it6, it7,
                                       itu, itr);
        }
    }
    _fork_join_end {}

    if (timeron) {
        timer_stop(T_PSINV);
    }

    /*
     * --------------------------------------------------------------------
     * exchange boundary points
     * --------------------------------------------------------------------
     */
    comm3_caller(u, n1, n2, n3, u_off);
}

static void rprj3_caller(DoubleVec& r, int m1k, int m2k, int m3k, DoubleVec& s,
                         int m1j, int m2j, int m3j, int k, size_t r_offset,
                         size_t s_offset) {
    if (timeron) {
        timer_start(T_RPRJ3);
    }
    const int d1 = (m1k == 3) ? 2 : 1;
    const int d2 = (m2k == 3) ? 2 : 1;
    const int d3 = (m3k == 3) ? 2 : 1;

    _fork_join(id, 1, m3j - 1, int64_t, j3_start, j3_end) {
        double x1[M], y1[M], x2, y2;
        struct Scope : public DereferenceScope {
            DoubleVecConstIter it0, it1, it2, it3, it4, it5, it6, it7, it8, it9,
                it10, it11, it12, it13, it14, it15, it16, it17, it18;
            DoubleVecIter it;

            void pin() const override {
                it0.pin();
                it1.pin();
                it2.pin();
                it3.pin();
                it4.pin();
                it5.pin();
                it7.pin();
                it8.pin();
                it9.pin();
                it10.pin();
                it11.pin();
                it12.pin();
                it13.pin();
                it14.pin();
                it15.pin();
                it16.pin();
                it17.pin();
                it18.pin();
                it.pin();
            }
            void unpin() const override {
                it0.unpin();
                it1.unpin();
                it2.unpin();
                it3.unpin();
                it4.unpin();
                it5.unpin();
                it7.unpin();
                it8.unpin();
                it9.unpin();
                it10.unpin();
                it11.unpin();
                it12.unpin();
                it13.unpin();
                it14.unpin();
                it15.unpin();
                it16.unpin();
                it17.unpin();
                it18.unpin();
                it.unpin();
            }
            Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
        } scp(&scope);
        auto& it0 = scp.it0;
        auto& it1 = scp.it1;
        auto& it2 = scp.it2;
        auto& it3 = scp.it3;
        auto& it4 = scp.it4;
        auto& it5 = scp.it5;
        auto& it6 = scp.it6;
        auto& it7 = scp.it7;
        auto& it8 = scp.it8;
        auto& it9 = scp.it9;
        auto& it10 = scp.it10;
        auto& it11 = scp.it11;
        auto& it12 = scp.it12;
        auto& it13 = scp.it13;
        auto& it14 = scp.it14;
        auto& it15 = scp.it15;
        auto& it16 = scp.it16;
        auto& it17 = scp.it17;
        auto& it18 = scp.it18;
        auto& it = scp.it;
        int j1_base = 2 * 1 - d1, j2_base = 2 * 1 - d2,
            j3_base = 2 * j3_start - d3;

        INIT_PREFETCH_CONST_ITER(it0,
                                 calculate_offset(r_offset + j1_base, j2_base,
                                                  j3_base + 1, m1k, m2k, m3k),
                                 2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it1,
            calculate_offset(r_offset + j1_base, j2_base + 2, j3_base + 1, m1k,
                             m2k, m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it2,
            calculate_offset(r_offset + j1_base, j2_base + 1, j3_base, m1k, m2k,
                             m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it3,
            calculate_offset(r_offset + j1_base, j2_base + 1, j3_base + 2, m1k,
                             m2k, m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(it4,
                                 calculate_offset(r_offset + j1_base, j2_base,
                                                  j3_base, m1k, m2k, m3k),
                                 2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(it5,
                                 calculate_offset(r_offset + j1_base, j2_base,
                                                  j3_base + 2, m1k, m2k, m3k),
                                 2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it6,
            calculate_offset(r_offset + j1_base, j2_base + 2, j3_base, m1k, m2k,
                             m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it7,
            calculate_offset(r_offset + j1_base, j2_base + 2, j3_base + 2, m1k,
                             m2k, m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it8,
            calculate_offset(r_offset + j1_base + 1, j2_base, j3_base, m1k, m2k,
                             m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it9,
            calculate_offset(r_offset + j1_base + 1, j2_base, j3_base + 2, m1k,
                             m2k, m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it10,
            calculate_offset(r_offset + j1_base + 1, j2_base + 2, j3_base, m1k,
                             m2k, m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it11,
            calculate_offset(r_offset + j1_base + 1, j2_base + 2, j3_base + 2,
                             m1k, m2k, m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it12,
            calculate_offset(r_offset + j1_base + 1, j2_base, j3_base + 1, m1k,
                             m2k, m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it13,
            calculate_offset(r_offset + j1_base + 1, j2_base + 2, j3_base + 1,
                             m1k, m2k, m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it14,
            calculate_offset(r_offset + j1_base + 1, j2_base + 1, j3_base, m1k,
                             m2k, m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it15,
            calculate_offset(r_offset + j1_base + 1, j2_base + 1, j3_base + 2,
                             m1k, m2k, m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it16,
            calculate_offset(r_offset + j1_base + 1, j2_base + 1, j3_base + 1,
                             m1k, m2k, m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it17,
            calculate_offset(r_offset + j1_base, j2_base + 1, j3_base + 1, m1k,
                             m2k, m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_CONST_ITER(
            it18,
            calculate_offset(r_offset + j1_base + 2, j2_base + 1, j3_base + 1,
                             m1k, m2k, m3k),
            2 * (j3_end - j3_start) * m2k * m1k, r, scp);
        INIT_PREFETCH_ITER(it, calculate_offset(1, 1, j3_start, m1j, m2j, m3j),
                           (j3_end - j3_start) * m2j * m1j, s, scp);

        for (int j3 = j3_start; j3 < j3_end; j3++) {
            for (int j2 = 1; j2 < m2j - 1; j2++) {
                for (int j1 = 1; j1 < m1j; j1++) {
                    int i1 = 2 * j1 - d1;
                    x1[i1] = *it0 + *it1 + *it2 + *it3;
                    y1[i1] = *it4 + *it5 + *it6 + *it7;
                    forward_iters_by_same_step(scp, 2, 0, 0, m1k, m2k, m3k, it0,
                                               it1, it2, it3, it4, it5, it6,
                                               it7);
                }
                for (int j1 = 1; j1 < m1j - 1; j1++) {
                    int i1 = 2 * j1 - d1;
                    y2 = *it8 + *it9 + *it10 + *it11;
                    x2 = *it12 + *it13 + *it14 + *it15;
                    *it = 0.5 * *it16 + 0.25 * (*it17 + *it18 + x2) +
                          0.125 * (x1[i1] + x1[i1 + 2] + y2) +
                          0.0625 * (y1[i1] + y1[i1 + 2]);
                    forward_iters_by_same_step(scp, 2, 0, 0, m1k, m2k, m3k, it8,
                                               it9, it10, it11, it12, it13,
                                               it14, it15, it16, it17, it18);
                    forward_iter(it, 1, 0, 0, m1j, m2j, m3j, scp);
                }
                forward_iters_by_same_step(
                    scp, -2 * (m1j - 1 - 1), 2, 0, m1k, m2k, m3k, it0, it1, it2,
                    it3, it4, it5, it6, it7, it8, it9, it10, it11, it12, it13,
                    it14, it15, it16, it17, it18);
                forward_iter(it, -(m1j - 1 - 1), 1, 0, m1j, m2j, m3j, scp);
            }
            forward_iters_by_same_step(scp, 0, -2 * (m2j - 1 - 1), 2, m1k, m2k,
                                       m3k, it0, it1, it2, it3, it4, it5, it6,
                                       it7, it8, it9, it10, it11, it12, it13,
                                       it14, it15, it16, it17, it18);
            forward_iter(it, 0, -(m2j - 1 - 1), 1, m1j, m2j, m3j, scp);
        }
    }
    _fork_join_end {}

    if (timeron) {
        timer_stop(T_RPRJ3);
    }

    comm3_caller(s, m1j, m2j, m3j, s_offset);
}

static void interp_caller(DoubleVec& z, int mm1, int mm2, int mm3, DoubleVec& u,
                          int n1, int n2, int n3, int /* k */, size_t z_off,
                          size_t u_off) {
    int d1, d2, d3, t1, t2, t3;

    /*
     * --------------------------------------------------------------------
     * note that m = 1037 in globals.h but for this only need to be
     * 535 to handle up to 1024^3
     * integer m
     * parameter( m=535 )
     * --------------------------------------------------------------------
     */

    if (timeron) {
        timer_start(T_INTERP);
    }

    if (n1 != 3 && n2 != 3 && n3 != 3) {
        // std::cout << "enter 1" << std::endl;
        // constexpr size_t UthreadCount = 224;
        // std::size_t sum = 0;
        _fork_join(id, 0, mm3 - 1, int64_t, i3_start, i3_end) {
            double z1[M], z2[M], z3[M];
            struct Scope : public DereferenceScope {
                DoubleVecConstIter it0, it1, it2, it3;

                void pin() const override {
                    it0.pin();
                    it1.pin();
                    it2.pin();
                    it3.pin();
                }

                void unpin() const override {
                    it0.unpin();
                    it1.unpin();
                    it2.unpin();
                    it3.unpin();
                }

                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
            } scp(&scope);
            auto& it0 = scp.it0;
            auto& it1 = scp.it1;
            auto& it2 = scp.it2;
            auto& it3 = scp.it3;
            INIT_PREFETCH_CONST_ITER(
                it0, calculate_offset(z_off, 0, i3_start, mm1, mm2, mm3),
                (i3_end - i3_start) * mm2 * mm1, z, scp);
            INIT_PREFETCH_CONST_ITER(
                it1, calculate_offset(z_off, 0 + 1, i3_start, mm1, mm2, mm3),
                (i3_end - i3_start) * mm2 * mm1, z, scp);
            INIT_PREFETCH_CONST_ITER(
                it2, calculate_offset(z_off, 0, i3_start + 1, mm1, mm2, mm3),
                (i3_end - i3_start) * mm2 * mm1, z, scp);
            INIT_PREFETCH_CONST_ITER(
                it3,
                calculate_offset(z_off, 0 + 1, i3_start + 1, mm1, mm2, mm3),
                (i3_end - i3_start) * mm2 * mm1, z, scp);

            for (int i3 = i3_start; i3 < i3_end; i3++) {
                for (int i2 = 0; i2 < mm2 - 1; i2++) {
                    for (int i1 = 0; i1 < mm1; i1++) {
                        z1[i1] = *it1 + *it0;
                        z2[i1] = *it2 + *it0;
                        z3[i1] = *it3 + *it2 + z1[i1];
                        forward_iters_by_same_step(scp, 1, 0, 0, mm1, mm2, mm3,
                                                   it0, it1, it2, it3);
                    }
                    forward_iters_by_same_step(scp, -mm1, 1, 0, mm1, mm2, mm3,
                                               it0, it1, it2, it3);
                    struct Scope2 : public DereferenceScope {
                        DoubleVecConstIter itz0, itz1;
                        DoubleVecIter itu0, itu1, itu2, itu3, itu4, itu5, itu6,
                            itu7;

                        void pin() const override {
                            itz0.pin();
                            itz1.pin();
                            itu0.pin();
                            itu1.pin();
                            itu2.pin();
                            itu3.pin();
                            itu4.pin();
                            itu5.pin();
                            itu6.pin();
                            itu7.pin();
                        }

                        void unpin() const override {
                            itz0.unpin();
                            itz1.unpin();
                            itu0.unpin();
                            itu1.unpin();
                            itu2.unpin();
                            itu3.unpin();
                            itu4.unpin();
                            itu5.unpin();
                            itu6.unpin();
                            itu7.unpin();
                        }

                        Scope2(DereferenceScope* scope)
                            : DereferenceScope(scope) {}
                    } scp2(&scp);
                    auto& itz0 = scp2.itz0;
                    auto& itz1 = scp2.itz1;
                    auto& itu0 = scp2.itu0;
                    auto& itu1 = scp2.itu1;
                    auto& itu2 = scp2.itu2;
                    auto& itu3 = scp2.itu3;
                    auto& itu4 = scp2.itu4;
                    auto& itu5 = scp2.itu5;
                    auto& itu6 = scp2.itu6;
                    auto& itu7 = scp2.itu7;
                    INIT_PREFETCH_CONST_ITER(
                        itz0, calculate_offset(z_off, i2, i3, mm1, mm2, mm3),
                        2 * (mm1 - 1), z, scp2);
                    INIT_PREFETCH_CONST_ITER(
                        itz1,
                        calculate_offset(z_off + 1, i2, i3, mm1, mm2, mm3),
                        2 * (mm1 - 1), z, scp2);
                    INIT_PREFETCH_ITER(
                        itu0,
                        calculate_offset(u_off, 2 * i2, 2 * i3, n1, n2, n3),
                        2 * (mm1 - 1), u, scp2);
                    INIT_PREFETCH_ITER(
                        itu1,
                        calculate_offset(u_off + 1, 2 * i2, 2 * i3, n1, n2, n3),
                        2 * (mm1 - 1), u, scp2);
                    INIT_PREFETCH_ITER(
                        itu2,
                        calculate_offset(u_off, 2 * i2 + 1, 2 * i3, n1, n2, n3),
                        2 * (mm1 - 1), u, scp2);
                    INIT_PREFETCH_ITER(itu3,
                                       calculate_offset(u_off + 1, 2 * i2 + 1,
                                                        2 * i3, n1, n2, n3),
                                       2 * (mm1 - 1), u, scp2);
                    INIT_PREFETCH_ITER(
                        itu4,
                        calculate_offset(u_off, 2 * i2, 2 * i3 + 1, n1, n2, n3),
                        2 * (mm1 - 1), u, scp2);
                    INIT_PREFETCH_ITER(itu5,
                                       calculate_offset(u_off + 1, 2 * i2,
                                                        2 * i3 + 1, n1, n2, n3),
                                       2 * (mm1 - 1), u, scp2);
                    INIT_PREFETCH_ITER(itu6,
                                       calculate_offset(u_off, 2 * i2 + 1,
                                                        2 * i3 + 1, n1, n2, n3),
                                       2 * (mm1 - 1), u, scp2);
                    INIT_PREFETCH_ITER(itu7,
                                       calculate_offset(u_off + 1, 2 * i2 + 1,
                                                        2 * i3 + 1, n1, n2, n3),
                                       2 * (mm1 - 1), u, scp2);

                    for (int i1 = 0; i1 < mm1 - 1; i1++) {
                        *itu0 += *itz0;
                        *itu1 += 0.5 * (*itz1 + *itz0);
                        *itu2 += 0.5 * z1[i1];
                        *itu3 += 0.25 * (z1[i1] + z1[i1 + 1]);
                        *itu4 += 0.5 * z2[i1];
                        *itu5 += 0.25 * (z2[i1] + z2[i1 + 1]);
                        *itu6 += 0.25 * z3[i1];
                        *itu7 += 0.125 * (z3[i1] + z3[i1 + 1]);
                        forward_iters_by_same_step(scp2, 2, 0, 0, n1, n2, n3,
                                                   itu0, itu1, itu2, itu3, itu4,
                                                   itu5, itu6, itu7);
                        forward_iters_by_same_step(scp2, 2, 0, 0, mm1, mm2, mm3,
                                                   itz0, itz1);
                    }
                }
                forward_iters_by_same_step(scp, 0, -(mm2 - 1), 1, mm1, mm2, mm3,
                                           it0, it1, it2, it3);
            }
        }
        _fork_join_end
    } else {
        if (n1 == 3) {
            d1 = 2;
            t1 = 1;
        } else {
            d1 = 1;
            t1 = 0;
        }
        if (n2 == 3) {
            d2 = 2;
            t2 = 1;
        } else {
            d2 = 1;
            t2 = 0;
        }
        if (n3 == 3) {
            d3 = 2;
            t3 = 1;
        } else {
            d3 = 1;
            t3 = 0;
        }

        {
            struct Scope : public DereferenceScope {
                DoubleVecConstIter it0, it1, it2, it3;
                DoubleVecIter itu0, itu1, itu2, itu3;

                void pin() const override {
                    it0.pin();
                    it1.pin();
                    it2.pin();
                    it3.pin();
                    itu0.pin();
                    itu1.pin();
                    itu2.pin();
                    itu3.pin();
                }
                void unpin() const override {
                    it0.unpin();
                    it1.unpin();
                    it2.unpin();
                    it3.unpin();
                    itu0.unpin();
                    itu1.unpin();
                    itu2.unpin();
                    itu3.unpin();
                }
                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
            };
            _fork_join(id, d3, mm3, int64_t, i3_start, i3_end) {
                for (int i3 = i3_start; i3 < i3_end; i3++) {
                    for (int i2 = d2; i2 <= mm2 - 1; i2++) {
                        Scope scp(&scope);
                        auto& it0 = scp.it0;
                        auto& it1 = scp.it1;
                        auto& it2 = scp.it2;
                        auto& it3 = scp.it3;
                        auto& itu0 = scp.itu0;
                        auto& itu1 = scp.itu1;
                        auto& itu2 = scp.itu2;
                        auto& itu3 = scp.itu3;
                        INIT_PREFETCH_CONST_ITER(
                            it0,
                            calculate_offset(z_off + d1 - 1, i2 - 1, i3 - 1,
                                             mm1, mm2, mm3),
                            mm1 - d1, z, scp);
                        INIT_PREFETCH_CONST_ITER(
                            it1,
                            calculate_offset(z_off + d1, i2 - 1, i3 - 1, mm1,
                                             mm2, mm3),
                            mm1 - d1, z, scp);
                        INIT_PREFETCH_CONST_ITER(
                            it2,
                            calculate_offset(z_off + d1 - 1, i2, i3 - 1, mm1,
                                             mm2, mm3),
                            mm1 - d1, z, scp);
                        INIT_PREFETCH_CONST_ITER(
                            it3,
                            calculate_offset(z_off + d1, i2, i3 - 1, mm1, mm2,
                                             mm3),
                            mm1 - d1, z, scp);

                        INIT_PREFETCH_ITER(
                            itu0,
                            calculate_offset(u_off + 2 * d1 - d1 - 1,
                                             2 * i2 - d2 - 1, 2 * i3 - d3 - 1,
                                             n1, n2, n3),
                            2 * (mm1 - d1), u, scp);
                        INIT_PREFETCH_ITER(
                            itu1,
                            calculate_offset(u_off + 2 * d1 - t1 - 1,
                                             2 * i2 - d2 - 1, 2 * i3 - d3 - 1,
                                             n1, n2, n3),
                            2 * (mm1 - d1), u, scp);
                        INIT_PREFETCH_ITER(
                            itu2,
                            calculate_offset(u_off + 2 * d1 - d1 - 1,
                                             2 * i2 - t2 - 1, 2 * i3 - d3 - 1,
                                             n1, n2, n3),
                            2 * (mm1 - d1), u, scp);
                        INIT_PREFETCH_ITER(
                            itu3,
                            calculate_offset(u_off + 2 * d1 - t1 - 1,
                                             2 * i2 - t2 - 1, 2 * i3 - d3 - 1,
                                             n1, n2, n3),
                            2 * (mm1 - d1), u, scp);
                        for (int i1 = d1; i1 <= mm1 - 1; i1++) {
                            *itu0 += *it0;
                            *itu1 += 0.5 * (*it1 + *it0);
                            *itu2 += 0.5 * (*it2 + *it0);
                            *itu3 += 0.25 * (*it3 + *it2 + *it1 + *it0);
                            forward_iters_by_same_step(scp, 1, 0, 0, mm1, mm2,
                                                       mm3, it0, it1, it2, it3);
                            forward_iters_by_same_step(scp, 2, 0, 0, n1, n2, n3,
                                                       itu0, itu1, itu2, itu3);
                        }
                    }
                }
            }
            _fork_join_end
        }

        {
            // constexpr size_t UthreadCount = 224;
            // std::size_t sum = 0;
            struct Scope : public DereferenceScope {
                DoubleVecConstIter it0, it1, it2, it3, it4, it5, it6, it7;
                DoubleVecIter itu0, itu1, itu2, itu3;

                void pin() const override {
                    it0.pin();
                    it1.pin();
                    it2.pin();
                    it3.pin();
                    it4.pin();
                    it5.pin();
                    it6.pin();
                    it7.pin();
                    itu0.pin();
                    itu1.pin();
                    itu2.pin();
                    itu3.pin();
                }

                void unpin() const override {
                    it0.unpin();
                    it1.unpin();
                    it2.unpin();
                    it3.unpin();
                    it4.unpin();
                    it5.unpin();
                    it6.unpin();
                    it7.unpin();
                    itu0.unpin();
                    itu1.unpin();
                    itu2.unpin();
                    itu3.unpin();
                }

                Scope(DereferenceScope* scope) : DereferenceScope(scope) {}
            };
            _fork_join(id, 1, mm3, int64_t, i3_start, i3_end) {
                for (int i3 = i3_start; i3 < i3_end; i3++) {
                    for (int i2 = d2; i2 <= mm2 - 1; i2++) {
                        Scope scp(&scope);
                        auto& it0 = scp.it0;
                        auto& it1 = scp.it1;
                        auto& it2 = scp.it2;
                        auto& it3 = scp.it3;
                        auto& it4 = scp.it4;
                        auto& it5 = scp.it5;
                        auto& it6 = scp.it6;
                        auto& it7 = scp.it7;
                        auto& itu0 = scp.itu0;
                        auto& itu1 = scp.itu1;
                        auto& itu2 = scp.itu2;
                        auto& itu3 = scp.itu3;
                        INIT_PREFETCH_CONST_ITER(
                            it0,
                            calculate_offset(z_off + d1 - 1, i2 - 1, i3 - 1,
                                             mm1, mm2, mm3),
                            mm1 - d1, z, scp);
                        INIT_PREFETCH_CONST_ITER(
                            it1,
                            calculate_offset(z_off + d1 - 1, i2 - 1, i3, mm1,
                                             mm2, mm3),
                            mm1 - d1, z, scp);
                        INIT_PREFETCH_CONST_ITER(
                            it2,
                            calculate_offset(z_off + d1, i2 - 1, i3 - 1, mm1,
                                             mm2, mm3),
                            mm1 - d1, z, scp);
                        INIT_PREFETCH_CONST_ITER(
                            it3,
                            calculate_offset(z_off + d1 - 1, i2, i3 - 1, mm1,
                                             mm2, mm3),
                            mm1 - d1, z, scp);
                        INIT_PREFETCH_CONST_ITER(
                            it4,
                            calculate_offset(z_off + d1 - 1, i2, i3, mm1, mm2,
                                             mm3),
                            mm1 - d1, z, scp);
                        INIT_PREFETCH_CONST_ITER(
                            it5,
                            calculate_offset(z_off + d1, i2 - 1, i3, mm1, mm2,
                                             mm3),
                            mm1 - d1, z, scp);
                        INIT_PREFETCH_CONST_ITER(
                            it6,
                            calculate_offset(z_off + d1, i2, i3 - 1, mm1, mm2,
                                             mm3),
                            mm1 - d1, z, scp);
                        INIT_PREFETCH_CONST_ITER(
                            it7,
                            calculate_offset(z_off + d1, i2, i3, mm1, mm2, mm3),
                            mm1 - d1, z, scp);

                        INIT_PREFETCH_ITER(
                            itu0,
                            calculate_offset(u_off + 2 * d1 - d1 - 1,
                                             2 * i2 - d2 - 1, 2 * i3 - t3 - 1,
                                             n1, n2, n3),
                            2 * (mm1 - d1), u, scp);
                        INIT_PREFETCH_ITER(
                            itu1,
                            calculate_offset(u_off + 2 * d1 - t1 - 1,
                                             2 * i2 - d2 - 1, 2 * i3 - t3 - 1,
                                             n1, n2, n3),
                            2 * (mm1 - d1), u, scp);
                        INIT_PREFETCH_ITER(
                            itu2,
                            calculate_offset(u_off + 2 * d1 - d1 - 1,
                                             2 * i2 - t1 - 1, 2 * i3 - t3 - 1,
                                             n1, n2, n3),
                            2 * (mm1 - d1), u, scp);
                        INIT_PREFETCH_ITER(
                            itu3,
                            calculate_offset(u_off + 2 * d1 - t1 - 1,
                                             2 * i2 - t2 - 1, 2 * i3 - t3 - 1,
                                             n1, n2, n3),
                            2 * (mm1 - d1), u, scp);

                        for (int i1 = d1; i1 <= mm1 - 1; i1++) {
                            *itu0 += 0.5 * (*it1 + *it0);
                            *itu1 += 0.25 * (*it5 + *it1 + *it2 + *it0);
                            *itu2 += 0.25 * (*it4 + *it1 + *it3 + *it0);
                            *itu3 += 0.125 * (*it7 + *it6 + *it5 + *it4 + *it3 +
                                              *it2 + *it1 + *it0);
                            forward_iters_by_same_step(scp, 1, 0, 0, mm1, mm2,
                                                       mm3, it0, it1, it2, it3,
                                                       it4, it5, it6, it7);
                            forward_iters_by_same_step(scp, 2, 0, 0, n1, n2, n3,
                                                       itu0, itu1, itu2, itu3);
                        }
                    }
                }
            }
            _fork_join_end
        }
    }
    if (timeron) {
        timer_stop(T_INTERP);
    }

    // if (debug_vec[0] >= 1) {
    //     rep_nrm(z, mm1, mm2, mm3, (char*)"z: inter", k - 1);
    //     rep_nrm(u, n1, n2, n3, (char*)"u: inter", k);
    // }
    // if (debug_vec[5] >= k) {
    //     showall(z, mm1, mm2, mm3);
    //     showall(u, n1, n2, n3);
    // }
}

static void mg3P_caller(DoubleVec& u, DoubleVec& v, DoubleVec& r, double a[4],
                        double c[4], int n1, int n2, int n3, int k) {
    int j;
    {
        for (k = lt; k >= lb + 1; k--) {
            j = k - 1;
            rprj3_caller(r, m1[k], m2[k], m3[k], r, m1[j], m2[j], m3[j], k,
                         ir[k], ir[j]);
        }
    }
    k = lb;

    zero3_caller(u, m1[k], m2[k], m3[k], ir[k]);

    psinv_caller(r, u, m1[k], m2[k], m3[k], c, k, ir[k], ir[k]);
    for (k = lb + 1; k <= lt - 1; k++) {
        j = k - 1;
        /*
         * --------------------------------------------------------------------
         * prolongate from level k-1  to k
         * -------------------------------------------------------------------
         */
        zero3_caller(u, m1[k], m2[k], m3[k], ir[k]);

        interp_caller(u, m1[j], m2[j], m3[j], u, m1[k], m2[k], m3[k], k, ir[j],
                      ir[k]);

        /*
         * --------------------------------------------------------------------
         * compute residual for level k
         * --------------------------------------------------------------------
         */
        resid_caller(u, r, r, m1[k], m2[k], m3[k], a, k, ir[k], ir[k], ir[k]);
        /*
         * --------------------------------------------------------------------
         * apply smoother
         * --------------------------------------------------------------------
         */
        psinv_caller(r, u, m1[k], m2[k], m3[k], c, k, ir[k], ir[k]);
    }

    j = lt - 1;
    k = lt;
    interp_caller(u, m1[j], m2[j], m3[j], u, n1, n2, n3, k, ir[j], 0);
    resid_caller(u, v, r, n1, n2, n3, a, k, 0, 0, 0);
    psinv_caller(r, u, n1, n2, n3, c, k, 0, 0);
}

void do_work(void* /* arg */) {
#if defined(DO_NOT_ALLOCATE_ARRAYS_WITH_DYNAMIC_MEMORY_AND_AS_SINGLE_DIMENSION)
    printf(
        " DO_NOT_ALLOCATE_ARRAYS_WITH_DYNAMIC_MEMORY_AND_AS_SINGLE_"
        "DIMENSION "
        "mode on\n");
#endif
    /*
     * -------------------------------------------------------------------------
     * k is the current level. it is passed down through subroutine args
     * and is not global. it is the current iteration
     * -------------------------------------------------------------------------
     */
    int k, it;
    double t, tinit, mflops;

    double a[4], c[4];

    double rnm2, rnmu, epsilon;
    int n1, n2, n3, nit;
    double nn, verify_value, err;
    boolean verified;
    char class_npb;

    int i;
    char* t_names[T_LAST];
    double tmax;

    for (i = T_INIT; i < T_LAST; i++) {
        timer_clear(i);
    }

    timer_start(T_INIT);

    /*
     * ----------------------------------------------------------------------
     * read in and broadcast input data
     * ----------------------------------------------------------------------
     */
    timeron = TRUE;
    t_names[T_INIT] = (char*)"init";
    t_names[T_BENCH] = (char*)"benchmk";
    t_names[T_MG3P] = (char*)"mg3P";
    t_names[T_PSINV] = (char*)"psinv";
    t_names[T_RESID] = (char*)"resid";
    t_names[T_RPRJ3] = (char*)"rprj3";
    t_names[T_INTERP] = (char*)"interp";
    t_names[T_NORM2] = (char*)"norm2";
    t_names[T_COMM3] = (char*)"comm3";
    FILE* fp = fopen("mg.input", "r");
    if (fp != NULL) {
        printf(" Reading from input file mg.input\n");
        if (fscanf(fp, "%d", &lt) != 1) {
            printf(" Error in reading elements\n");
            exit(1);
        }
        while (fgetc(fp) != '\n');
        if (fscanf(fp, "%d%d%d", &nx[lt], &ny[lt], &nz[lt]) != 3) {
            printf(" Error in reading elements\n");
            exit(1);
        }
        while (fgetc(fp) != '\n');
        if (fscanf(fp, "%d", &nit) != 1) {
            printf(" Error in reading elements\n");
            exit(1);
        }
        while (fgetc(fp) != '\n');
        for (i = 0; i <= 7; i++) {
            if (fscanf(fp, "%d", &debug_vec[i]) != 1) {
                printf(" Error in reading elements\n");
                exit(1);
            }
        }
        fclose(fp);
    } else {
        printf(" No input file. Using compiled defaults\n");
        lt = LT_DEFAULT;
        nit = NIT_DEFAULT;
        nx[lt] = NX_DEFAULT;
        ny[lt] = NY_DEFAULT;
        nz[lt] = NZ_DEFAULT;
        for (i = 0; i <= 7; i++) {
            debug_vec[i] = DEBUG_DEFAULT;
        }
    }

    if ((nx[lt] != ny[lt]) || (nx[lt] != nz[lt])) {
        class_npb = 'U';
    } else if (nx[lt] == 32 && nit == 4) {
        class_npb = 'S';
    } else if (nx[lt] == 128 && nit == 4) {
        class_npb = 'W';
    } else if (nx[lt] == 256 && nit == 4) {
        class_npb = 'A';
    } else if (nx[lt] == 256 && nit == 20) {
        class_npb = 'B';
    } else if (nx[lt] == 512 && nit == 20) {
        class_npb = 'C';
    } else if (nx[lt] == 1024 && nit == 50) {
        class_npb = 'D';
    } else if (nx[lt] == 2048 && nit == 50) {
        class_npb = 'E';
    } else {
        class_npb = 'U';
    }

    /*
     * ---------------------------------------------------------------------
     * use these for debug info:
     * ---------------------------------------------------------------------
     * debug_vec(0) = 1 !=> report all norms
     * debug_vec(1) = 1 !=> some setup information
     * debug_vec(1) = 2 !=> more setup information
     * debug_vec(2) = k => at level k or below, show result of resid
     * debug_vec(3) = k => at level k or below, show result of psinv
     * debug_vec(4) = k => at level k or below, show result of rprj
     * debug_vec(5) = k => at level k or below, show result of interp
     * debug_vec(6) = 1 => (unused)
     * debug_vec(7) = 1 => (unused)
     * ---------------------------------------------------------------------
     */
    a[0] = -8.0 / 3.0;
    a[1] = 0.0;
    a[2] = 1.0 / 6.0;
    a[3] = 1.0 / 12.0;

    if (class_npb == 'A' || class_npb == 'S' || class_npb == 'W') {
        /* coefficients for the s(a) smoother */
        c[0] = -3.0 / 8.0;
        c[1] = +1.0 / 32.0;
        c[2] = -1.0 / 64.0;
        c[3] = 0.0;
    } else {
        /* coefficients for the s(b) smoother */
        c[0] = -3.0 / 17.0;
        c[1] = +1.0 / 33.0;
        c[2] = -1.0 / 61.0;
        c[3] = 0.0;
    }

    lb = 1;
    k = lt;

    setup(&n1, &n2, &n3, k);
    // vec_u_zero3(n1, n2, n3);
    alloc_farlib(u_vec, NR);
    std::cout << "u_vec allocated" << std::endl;
    alloc_farlib(v_vec, NV);
    std::cout << "v_vec allocated" << std::endl;
    alloc_farlib(r_vec, NR);
    std::cout << "r_vec allocated" << std::endl;
    {
        auto start = std::chrono::high_resolution_clock::now();

        zran3_caller(v_vec, n1, n2, n3, nx[lt], ny[lt], k);
        auto end = std::chrono::high_resolution_clock::now();

        auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        std::cout << "zran3 " << duration.count() << "micro seconds"
                  << std::endl;
    }

    std::cout << "finish zran3" << std::endl;

    {
        auto start = std::chrono::high_resolution_clock::now();

        norm2u3_caller(v_vec, n1, n2, n3, &rnm2, &rnmu, nx[lt], ny[lt], nz[lt],
                       0);
        auto end = std::chrono::high_resolution_clock::now();

        auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        std::cout << "normu3 " << duration.count() << "micro seconds"
                  << std::endl;
    }

    std::cout << "finish norm2u3" << std::endl;

    printf(
        "\n\n NAS Parallel Benchmarks 4.1 Serial C++ version - MG "
        "Benchmark\n\n");
    printf(" Size: %3dx%3dx%3d (class_npb %1c)\n", nx[lt], ny[lt], nz[lt],
           class_npb);
    printf(" Iterations: %3d\n", nit);

    resid_caller(u_vec, v_vec, r_vec, n1, n2, n3, a, k, 0, 0, 0);
    std::cout << "finish resid" << std::endl;
    {
        auto start = std::chrono::high_resolution_clock::now();
        norm2u3_caller(r_vec, n1, n2, n3, &rnm2, &rnmu, nx[lt], ny[lt], nz[lt],
                       0);
        auto end = std::chrono::high_resolution_clock::now();

        auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        std::cout << "norm2u3 " << duration.count() << "micro seconds"
                  << std::endl;
    }
    std::cout << "finish norm2u3" << std::endl;
    /*
     * ---------------------------------------------------------------------
     * one iteration for startup
     * ---------------------------------------------------------------------
     */
    {
        auto start = std::chrono::high_resolution_clock::now();
        mg3P_caller(u_vec, v_vec, r_vec, a, c, n1, n2, n3, k);
        auto end = std::chrono::high_resolution_clock::now();

        auto duration =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        std::cout << "mg3P " << duration.count() << "micro seconds"
                  << std::endl;
    }

    std::cout << "finish mg3P" << std::endl;
    resid_caller(u_vec, v_vec, r_vec, n1, n2, n3, a, k, 0, 0, 0);
    std::cout << "finish resid" << std::endl;

    setup(&n1, &n2, &n3, k);

    zero3_caller(u_vec, n1, n2, n3, 0);
    zran3_caller(v_vec, n1, n2, n3, nx[lt], ny[lt], k);

    timer_stop(T_INIT);
    tinit = timer_read(T_INIT);
    printf(" Initialization time: %15.3f seconds\n", tinit);

    for (i = T_BENCH; i < T_LAST; i++) {
        timer_clear(i);
    }
    timer_start(T_BENCH);
    FarLib::perf_init();
    FarLib::profile::beehive_profile([&] {
        FarLib::perf_profile([&] {
            if (timeron) {
                timer_start(T_RESID2);
            }
            resid_caller(u_vec, v_vec, r_vec, n1, n2, n3, a, k, 0, 0, 0);
            if (timeron) {
                timer_stop(T_RESID2);
            }
            auto start1 = get_cycles();
            norm2u3_caller(r_vec, n1, n2, n3, &rnm2, &rnmu, nx[lt], ny[lt],
                           nz[lt], 0);
            auto end1 = get_cycles();
            for (it = 1; it <= nit; it++) {
                if ((it == 1) || (it == nit) || ((it % 5) == 0)) {
                    printf("  iter %3d\n", it);
                }
                if (timeron) {
                    timer_start(T_MG3P);
                }
                mg3P_caller(u_vec, v_vec, r_vec, a, c, n1, n2, n3, k);

                if (timeron) {
                    timer_stop(T_MG3P);
                }
                if (timeron) {
                    timer_start(T_RESID2);
                }
                resid_caller(u_vec, v_vec, r_vec, n1, n2, n3, a, k, 0, 0, 0);
                if (timeron) {
                    timer_stop(T_RESID2);
                }
            }
            auto start2 = get_cycles();
            norm2u3_caller(r_vec, n1, n2, n3, &rnm2, &rnmu, nx[lt], ny[lt],
                           nz[lt], 0);
            auto end2 = get_cycles();
        }).print();
    });

    timer_stop(T_BENCH);
    t = timer_read(T_BENCH);

    verified = FALSE;
    verify_value = 0.0;

    printf(" Benchmark completed\n");

    epsilon = 1.0e-8;
    if (class_npb != 'U') {
        if (class_npb == 'S') {
            verify_value = 0.5307707005734e-04;
        } else if (class_npb == 'W') {
            verify_value = 0.6467329375339e-05;
        } else if (class_npb == 'A') {
            verify_value = 0.2433365309069e-05;
        } else if (class_npb == 'B') {
            verify_value = 0.1800564401355e-05;
        } else if (class_npb == 'C') {
            verify_value = 0.5706732285740e-06;
        } else if (class_npb == 'D') {
            verify_value = 0.1583275060440e-09;
        } else if (class_npb == 'E') {
            verify_value = 0.8157592357404e-10;
        }

        err = fabs(rnm2 - verify_value) / verify_value;
        if (err <= epsilon) {
            verified = TRUE;
            printf(" VERIFICATION SUCCESSFUL\n");
            printf(" L2 Norm is %20.13e\n", rnm2);
            printf(" Error is   %20.13e\n", err);
        } else {
            verified = FALSE;
            printf(" VERIFICATION FAILED\n");
            printf(" L2 Norm is             %20.13e\n", rnm2);
            printf(" The correct L2 Norm is %20.13e\n", verify_value);
        }
    } else {
        verified = FALSE;
        printf(" Problem size unknown\n");
        printf(" NO VERIFICATION PERFORMED\n");
    }

    nn = 1.0 * nx[lt] * ny[lt] * nz[lt];

    if (t != 0.0) {
        mflops = 58.0 * nit * nn * 1.0e-6 / t;
    } else {
        mflops = 0.0;
    }

    c_print_results((char*)"MG", class_npb, nx[lt], ny[lt], nz[lt], nit, t,
                    mflops, (char*)"          floating point", verified,
                    (char*)NPBVERSION, (char*)COMPILETIME,
                    (char*)COMPILERVERSION, (char*)CS1, (char*)CS2, (char*)CS3,
                    (char*)CS4, (char*)CS5, (char*)CS6, (char*)CS7);

    /*
     * ---------------------------------------------------------------------
     * more timers
     * ---------------------------------------------------------------------
     */
    if (timeron) {
        tmax = timer_read(T_BENCH);
        if (tmax == 0.0) {
            tmax = 1.0;
        }
        printf("  SECTION   Time (secs)\n");
        for (i = T_BENCH; i < T_LAST; i++) {
            t = timer_read(i);
            if (i == T_RESID2) {
                t = timer_read(T_RESID) - t;
                printf("    --> %8s:%9.3f  (%6.2f%%)\n", "mg-resid", t,
                       t * 100.0 / tmax);
            } else {
                printf("  %-8s:%9.3f  (%6.2f%%)\n", t_names[i], t,
                       t * 100.0 / tmax);
            }
        }
    }
}

/*
 * ---------------------------------------------------------------------
 * bubble does a bubble sort in direction dir
 * ---------------------------------------------------------------------
 */
static void bubble(double ten[][MM], int j1[][MM], int j2[][MM], int j3[][MM],
                   int m, int ind) {
    double temp;
    int i, j_temp;

    if (ind == 1) {
        for (i = 0; i < m - 1; i++) {
            if (ten[ind][i] > ten[ind][i + 1]) {
                temp = ten[ind][i + 1];
                ten[ind][i + 1] = ten[ind][i];
                ten[ind][i] = temp;

                j_temp = j1[ind][i + 1];
                j1[ind][i + 1] = j1[ind][i];
                j1[ind][i] = j_temp;

                j_temp = j2[ind][i + 1];
                j2[ind][i + 1] = j2[ind][i];
                j2[ind][i] = j_temp;

                j_temp = j3[ind][i + 1];
                j3[ind][i + 1] = j3[ind][i];
                j3[ind][i] = j_temp;
            } else {
                return;
            }
        }
    } else {
        for (i = 0; i < m - 1; i++) {
            if (ten[ind][i] < ten[ind][i + 1]) {
                temp = ten[ind][i + 1];
                ten[ind][i + 1] = ten[ind][i];
                ten[ind][i] = temp;

                j_temp = j1[ind][i + 1];
                j1[ind][i + 1] = j1[ind][i];
                j1[ind][i] = j_temp;

                j_temp = j2[ind][i + 1];
                j2[ind][i + 1] = j2[ind][i];
                j2[ind][i] = j_temp;

                j_temp = j3[ind][i + 1];
                j3[ind][i + 1] = j3[ind][i];
                j3[ind][i] = j_temp;
            } else {
                return;
            }
        }
    }
}

/*
 * ---------------------------------------------------------------------
 * power raises an integer, disguised as a double
 * precision real, to an integer power
 * ---------------------------------------------------------------------
 */
static double power(double a, int n) {
    double aj;
    int nj;
    double power;

    power = 1.0;
    nj = n;
    aj = a;

    while (nj != 0) {
        if ((nj % 2) == 1) {
            randlc(&power, aj);
        }
        randlc(&aj, aj);
        nj = nj / 2;
    }

    return power;
}

static void setup(int* n1, int* n2, int* n3, int k) {
    int j;

    int ax, mi[MAXLEVEL + 1][3];
    int ng[MAXLEVEL + 1][3];

    ng[lt][0] = nx[lt];
    ng[lt][1] = ny[lt];
    ng[lt][2] = nz[lt];
    for (ax = 0; ax < 3; ax++) {
        for (k = lt - 1; k >= 1; k--) {
            ng[k][ax] = ng[k + 1][ax] / 2;
        }
    }
    for (k = lt; k >= 1; k--) {
        nx[k] = ng[k][0];
        ny[k] = ng[k][1];
        nz[k] = ng[k][2];
    }

    for (k = lt; k >= 1; k--) {
        for (ax = 0; ax < 3; ax++) {
            mi[k][ax] = 2 + ng[k][ax];
        }

        m1[k] = mi[k][0];
        m2[k] = mi[k][1];
        m3[k] = mi[k][2];
    }

    k = lt;
    is1 = 2 + ng[k][0] - ng[lt][0];
    ie1 = 1 + ng[k][0];
    *n1 = 3 + ie1 - is1;
    is2 = 2 + ng[k][1] - ng[lt][1];
    ie2 = 1 + ng[k][1];
    *n2 = 3 + ie2 - is2;
    is3 = 2 + ng[k][2] - ng[lt][2];
    ie3 = 1 + ng[k][2];
    *n3 = 3 + ie3 - is3;

    ir[lt] = 0;
    for (j = lt - 1; j >= 1; j--) {
        ir[j] = ir[j + 1] + ONE * m1[j + 1] * m2[j + 1] * m3[j + 1];
    }

    if (debug_vec[1] >= 1) {
        printf(" in setup, \n");
        printf("   k  lt  nx  ny  nz  n1  n2  n3 is1 is2 is3 ie1 ie2 ie3\n");
        printf("%4d%4d%4d%4d%4d%4d%4d%4d%4d%4d%4d%4d%4d%4d\n", k, lt, ng[k][0],
               ng[k][1], ng[k][2], *n1, *n2, *n3, is1, is2, is3, ie1, ie2, ie3);
    }
}

int main(int argc, char** argv) {
    FarLib::rdma::Configure config;
#ifndef STANDALONE
    if (argc != 2) {
        std::cout << "usage: " << argv[0] << " <configure file>" << std::endl;
        return -1;
    }
    config.from_file(argv[1]);
#else
    WARN("STANDALONE MODE");
    config.server_addr = "127.0.0.1";
    config.server_port = "1234";
    config.server_buffer_size = 1024L * 1024 * 1024 * 64;
    config.client_buffer_size = 64 * 1024 * 1024 * 1024L;
    config.evict_batch_size = 64 * 1024;
    Server server(config);
    std::thread server_thread([&server] { server.start(); });
#endif
    FarLib::runtime_init(config);
    // ...
    do_work(nullptr);
    std::cout << "The end ..." << std::endl;
    u_vec.clear();
    std::cout << "clear u" << std::endl;
    v_vec.clear();
    std::cout << "clear v" << std::endl;
    r_vec.clear();
    std::cout << "clear r" << std::endl;
    FarLib::runtime_destroy();
#ifdef STANDALONE
    server_thread.join();
#endif
    return 0;
}