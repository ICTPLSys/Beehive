.local NEW_SCHEDULE
NEW_SCHEDULE = 1    # 0/1 = close/open
# Please modify NEW_SCUEDULE macro definition in select_schedule at the same time
.text

.macro REGSAVE
    pushq %r15
    pushq %r14
    pushq %r13
    pushq %r12
    pushq %rbx
    pushq %rbp
.endm

.macro REGRESTORE
    popq %rbp
    popq %rbx
    popq %r12
    popq %r13
    popq %r14
    popq %r15
.endm

.if NEW_SCHEDULE
.globl    call_xstack,
.type    call_xstack, @function
.extern  do_invoke
call_xstack:
    # rdi , rsi , rdx , rcx , r8 , r9
    # rdi: context_t *ctx
    # rsi : sp_t sp
    # rdx : void *caller_task, store sp for ooo task if it call
    # rcx: func_t func
    REGSAVE
    mov %rsp, (%rdx)    # store sp to _task->current_sp
    mov %rsi, %rsp      # change the stack
    call *%rcx      # call function
    mov (%rax), %rsp    # rdi = callee task ptr
    REGRESTORE    
    movl    $1, %eax    # return DONE
    ret

.globl    yield_xstack,
.type    yield_xstack, @function
yield_xstack:
    # rdi , rsi , rdx , rcx , r8 , r9
    # rdi: sp_t sp
    # esi: state
    # rdx: void *_task, store sp for task if it call
    REGSAVE
    mov %rsp, (%rdx)
    mov %rdi, %rsp      # restore the stack
    REGRESTORE
    mov %esi, %eax      # return state code
    ret
    
.section    .note.GNU-stack,"",@progbits

.else
    .globl    call_xstack,
    .type    call_xstack, @function
call_xstack:
    # rdi , rsi , rdx , rcx , r8 , r9
    # rdi: context_t *ctx
    # rsi: sp_t sp
    # rdx: func
    REGSAVE
    mov %rsp, (%rdi)    # save the old rsp
    mov %rsi, %rsp      # change the stack
    call *%rdx          # call function
    mov (%rax), %rsp    # restore the stack
    REGRESTORE
    movl    $1, %eax    # return DONE
    ret


    .globl    yield_xstack,
    .type    yield_xstack, @function
yield_xstack:
    # rdi , rsi , rdx , rcx , r8 , r9
    # rdi: context_t *ctx
    # rsi: sp_t sp
    # edx: state
    REGSAVE
    mov %rsp, (%rdi)    # save current sp
    mov %rsi, %rsp      # restore the stack
    REGRESTORE
    mov %edx, %eax      # return state code
    ret

    .section    .note.GNU-stack,"",@progbits
.endif
