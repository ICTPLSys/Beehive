#pragma once
#include <cstdint>
/* CLASS = C */
/*
  c  This file is generated automatically by the setparams utility.
  c  It sets the number of processors and the class_npb of the NPB
  c  in this directory. Do not modify it by hand.
 */
constexpr uint64_t NX_DEFAULT = 1024;
constexpr uint64_t NY_DEFAULT = 1024;
constexpr uint64_t NZ_DEFAULT = 1024;
constexpr uint64_t NIT_DEFAULT = 5;
constexpr uint64_t LM = 10;
constexpr uint64_t LT_DEFAULT = 10;
constexpr uint64_t DEBUG_DEFAULT = 0;
constexpr uint64_t NDIM1 = 10;
constexpr uint64_t NDIM2 = 10;
constexpr uint64_t NDIM3 = 10;
constexpr uint64_t ONE = 1;
constexpr bool CONVERTDOUBLE = false;
const char *COMPILETIME = "15 May 2024";
const char *NPBVERSION = "4.1";
const char *COMPILERVERSION = "13.1.0";
const char *CS1 = "g++";
const char *CS2 = "$(CC)";
const char *CS3 = "-lm ";
const char *CS4 = "-I../common ";
const char *CS5 = "-O1 -fPIC -fno-builtin -fno-builtin -fno-ve...";
const char *CS6 = "-O1 -fPIC -mcmodel=medium -fno-builtin -fno...";
const char *CS7 = "randdp";
