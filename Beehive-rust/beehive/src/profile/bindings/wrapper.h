#include <papi.h>
#define RS_PAPI_CONSTANT(ITEM) static const int RS_##ITEM = (ITEM)
RS_PAPI_CONSTANT(PAPI_L1_DCM ); /*Level 1 data cache misses */
RS_PAPI_CONSTANT(PAPI_L1_ICM ); /*Level 1 instruction cache misses */
RS_PAPI_CONSTANT(PAPI_L2_DCM ); /*Level 2 data cache misses */
RS_PAPI_CONSTANT(PAPI_L2_ICM ); /*Level 2 instruction cache misses */
RS_PAPI_CONSTANT(PAPI_L3_DCM ); /*Level 3 data cache misses */
RS_PAPI_CONSTANT(PAPI_L3_ICM ); /*Level 3 instruction cache misses */
RS_PAPI_CONSTANT(PAPI_L1_TCM ); /*Level 1 total cache misses */
RS_PAPI_CONSTANT(PAPI_L2_TCM ); /*Level 2 total cache misses */
RS_PAPI_CONSTANT(PAPI_L3_TCM ); /*Level 3 total cache misses */
RS_PAPI_CONSTANT(PAPI_CA_SNP ); /*Snoops */
RS_PAPI_CONSTANT(PAPI_CA_SHR ); /*Request for shared cache line (SMP) */
RS_PAPI_CONSTANT(PAPI_CA_CLN ); /*Request for clean cache line (SMP) */
RS_PAPI_CONSTANT(PAPI_CA_INV ); /*Request for cache line Invalidation (SMP) */
RS_PAPI_CONSTANT(PAPI_CA_ITV ); /*Request for cache line Intervention (SMP) */
RS_PAPI_CONSTANT(PAPI_L3_LDM ); /*Level 3 load misses */
RS_PAPI_CONSTANT(PAPI_L3_STM ); /*Level 3 store misses */
RS_PAPI_CONSTANT(PAPI_BRU_IDL); /*Cycles branch units are idle */
RS_PAPI_CONSTANT(PAPI_FXU_IDL); /*Cycles integer units are idle */
RS_PAPI_CONSTANT(PAPI_FPU_IDL); /*Cycles floating point units are idle */
RS_PAPI_CONSTANT(PAPI_LSU_IDL); /*Cycles load/store units are idle */
RS_PAPI_CONSTANT(PAPI_TLB_DM ); /*Data translation lookaside buffer misses */
RS_PAPI_CONSTANT(PAPI_TLB_IM ); /*Instr translation lookaside buffer misses */
RS_PAPI_CONSTANT(PAPI_TLB_TL ); /*Total translation lookaside buffer misses */
RS_PAPI_CONSTANT(PAPI_L1_LDM ); /*Level 1 load misses */
RS_PAPI_CONSTANT(PAPI_L1_STM ); /*Level 1 store misses */
RS_PAPI_CONSTANT(PAPI_L2_LDM ); /*Level 2 load misses */
RS_PAPI_CONSTANT(PAPI_L2_STM ); /*Level 2 store misses */
RS_PAPI_CONSTANT(PAPI_BTAC_M ); /*BTAC miss */
RS_PAPI_CONSTANT(PAPI_PRF_DM ); /*Prefetch data instruction caused a miss */
RS_PAPI_CONSTANT(PAPI_L3_DCH ); /*Level 3 Data Cache Hit */
RS_PAPI_CONSTANT(PAPI_TLB_SD ); /*Xlation lookaside buffer shootdowns (SMP) */
RS_PAPI_CONSTANT(PAPI_CSR_FAL); /*Failed store conditional instructions */
RS_PAPI_CONSTANT(PAPI_CSR_SUC); /*Successful store conditional instructions */
RS_PAPI_CONSTANT(PAPI_CSR_TOT); /*Total store conditional instructions */
RS_PAPI_CONSTANT(PAPI_MEM_SCY); /*Cycles Stalled Waiting for Memory Access */
RS_PAPI_CONSTANT(PAPI_MEM_RCY); /*Cycles Stalled Waiting for Memory Read */
RS_PAPI_CONSTANT(PAPI_MEM_WCY); /*Cycles Stalled Waiting for Memory Write */
RS_PAPI_CONSTANT(PAPI_STL_ICY); /*Cycles with No Instruction Issue */
RS_PAPI_CONSTANT(PAPI_FUL_ICY); /*Cycles with Maximum Instruction Issue */
RS_PAPI_CONSTANT(PAPI_STL_CCY); /*Cycles with No Instruction Completion */
RS_PAPI_CONSTANT(PAPI_FUL_CCY); /*Cycles with Maximum Instruction Completion */
RS_PAPI_CONSTANT(PAPI_HW_INT ); /*Hardware interrupts */
RS_PAPI_CONSTANT(PAPI_BR_UCN ); /*Unconditional branch instructions executed */
RS_PAPI_CONSTANT(PAPI_BR_CN  ); /*Conditional branch instructions executed */
RS_PAPI_CONSTANT(PAPI_BR_TKN ); /*Conditional branch instructions taken */
RS_PAPI_CONSTANT(PAPI_BR_NTK ); /*Conditional branch instructions not taken */
RS_PAPI_CONSTANT(PAPI_BR_MSP ); /*Conditional branch instructions mispred */
RS_PAPI_CONSTANT(PAPI_BR_PRC ); /*Conditional branch instructions corr. pred */
RS_PAPI_CONSTANT(PAPI_FMA_INS); /*FMA instructions completed */
RS_PAPI_CONSTANT(PAPI_TOT_IIS); /*Total instructions issued */
RS_PAPI_CONSTANT(PAPI_TOT_INS); /*Total instructions executed */
RS_PAPI_CONSTANT(PAPI_INT_INS); /*Integer instructions executed */
RS_PAPI_CONSTANT(PAPI_FP_INS ); /*Floating point instructions executed */
RS_PAPI_CONSTANT(PAPI_LD_INS ); /*Load instructions executed */
RS_PAPI_CONSTANT(PAPI_SR_INS ); /*Store instructions executed */
RS_PAPI_CONSTANT(PAPI_BR_INS ); /*Total branch instructions executed */
RS_PAPI_CONSTANT(PAPI_VEC_INS); /*Vector/SIMD instructions executed (could include integer) */
RS_PAPI_CONSTANT(PAPI_RES_STL); /*Cycles processor is stalled on resource */
RS_PAPI_CONSTANT(PAPI_FP_STAL); /*Cycles any FP units are stalled */
RS_PAPI_CONSTANT(PAPI_TOT_CYC); /*Total cycles executed */
RS_PAPI_CONSTANT(PAPI_LST_INS); /*Total load/store inst. executed */
RS_PAPI_CONSTANT(PAPI_SYC_INS); /*Sync. inst. executed */
RS_PAPI_CONSTANT(PAPI_L1_DCH ); /*L1 D Cache Hit */
RS_PAPI_CONSTANT(PAPI_L2_DCH ); /*L2 D Cache Hit */
RS_PAPI_CONSTANT(PAPI_L1_DCA ); /*L1 D Cache Access */
RS_PAPI_CONSTANT(PAPI_L2_DCA ); /*L2 D Cache Access */
RS_PAPI_CONSTANT(PAPI_L3_DCA ); /*L3 D Cache Access */
RS_PAPI_CONSTANT(PAPI_L1_DCR ); /*L1 D Cache Read */
RS_PAPI_CONSTANT(PAPI_L2_DCR ); /*L2 D Cache Read */
RS_PAPI_CONSTANT(PAPI_L3_DCR ); /*L3 D Cache Read */
RS_PAPI_CONSTANT(PAPI_L1_DCW ); /*L1 D Cache Write */
RS_PAPI_CONSTANT(PAPI_L2_DCW ); /*L2 D Cache Write */
RS_PAPI_CONSTANT(PAPI_L3_DCW ); /*L3 D Cache Write */
RS_PAPI_CONSTANT(PAPI_L1_ICH ); /*L1 instruction cache hits */
RS_PAPI_CONSTANT(PAPI_L2_ICH ); /*L2 instruction cache hits */
RS_PAPI_CONSTANT(PAPI_L3_ICH ); /*L3 instruction cache hits */
RS_PAPI_CONSTANT(PAPI_L1_ICA ); /*L1 instruction cache accesses */
RS_PAPI_CONSTANT(PAPI_L2_ICA ); /*L2 instruction cache accesses */
RS_PAPI_CONSTANT(PAPI_L3_ICA ); /*L3 instruction cache accesses */
RS_PAPI_CONSTANT(PAPI_L1_ICR ); /*L1 instruction cache reads */
RS_PAPI_CONSTANT(PAPI_L2_ICR ); /*L2 instruction cache reads */
RS_PAPI_CONSTANT(PAPI_L3_ICR ); /*L3 instruction cache reads */
RS_PAPI_CONSTANT(PAPI_L1_ICW ); /*L1 instruction cache writes */
RS_PAPI_CONSTANT(PAPI_L2_ICW ); /*L2 instruction cache writes */
RS_PAPI_CONSTANT(PAPI_L3_ICW ); /*L3 instruction cache writes */
RS_PAPI_CONSTANT(PAPI_L1_TCH ); /*L1 total cache hits */
RS_PAPI_CONSTANT(PAPI_L2_TCH ); /*L2 total cache hits */
RS_PAPI_CONSTANT(PAPI_L3_TCH ); /*L3 total cache hits */
RS_PAPI_CONSTANT(PAPI_L1_TCA ); /*L1 total cache accesses */
RS_PAPI_CONSTANT(PAPI_L2_TCA ); /*L2 total cache accesses */
RS_PAPI_CONSTANT(PAPI_L3_TCA ); /*L3 total cache accesses */
RS_PAPI_CONSTANT(PAPI_L1_TCR ); /*L1 total cache reads */
RS_PAPI_CONSTANT(PAPI_L2_TCR ); /*L2 total cache reads */
RS_PAPI_CONSTANT(PAPI_L3_TCR ); /*L3 total cache reads */
RS_PAPI_CONSTANT(PAPI_L1_TCW ); /*L1 total cache writes */
RS_PAPI_CONSTANT(PAPI_L2_TCW ); /*L2 total cache writes */
RS_PAPI_CONSTANT(PAPI_L3_TCW ); /*L3 total cache writes */
RS_PAPI_CONSTANT(PAPI_FML_INS); /*FM ins */
RS_PAPI_CONSTANT(PAPI_FAD_INS); /*FA ins */
RS_PAPI_CONSTANT(PAPI_FDV_INS); /*FD ins */
RS_PAPI_CONSTANT(PAPI_FSQ_INS); /*FSq ins */
RS_PAPI_CONSTANT(PAPI_FNV_INS); /*Finv ins */
RS_PAPI_CONSTANT(PAPI_FP_OPS ); /*Floating point operations executed */
RS_PAPI_CONSTANT(PAPI_SP_OPS ); /* Floating point operations executed; optimized to count scaled single precision vector operations */
RS_PAPI_CONSTANT(PAPI_DP_OPS ); /* Floating point operations executed; optimized to count scaled double precision vector operations */
RS_PAPI_CONSTANT(PAPI_VEC_SP ); /* Single precision vector/SIMD instructions */
RS_PAPI_CONSTANT(PAPI_VEC_DP ); /* Double precision vector/SIMD instructions */
RS_PAPI_CONSTANT(PAPI_REF_CYC); 	/* Reference clock cycles */
RS_PAPI_CONSTANT(PAPI_END    ); /* this should always be last! */

RS_PAPI_CONSTANT(PAPI_VERSION);
RS_PAPI_CONSTANT(PAPI_VER_CURRENT);
#undef RS_PAPI_EVENT_CONSTANT
