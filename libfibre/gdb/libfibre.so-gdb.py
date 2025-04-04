# to automatically load this when libfibre.so is loaded:
# echo add-auto-load-safe-path DIRECTORY >> $HOME/.gdbinit
# or load via 'source DIRECTORY/libfibre.so-gdb.py'

import gdb
import re
from contextlib import contextmanager

class FibreSupport():
    def __init__(self):
        FibreSupport.saved = False

    def stop_handler(event):
        if (gdb.lookup_symbol("_lfFredDebugList")[0] == None):
            print("WARNING: no fibre debugging support - did you enable TESTING_ENABLE_DEBUGGING?")
            return
        FibreSupport.list = []
        FibreSupport.active = {}
        FibreSupport.threads = {}
        # traverse runtime stack list to build internal list of fibres
        _lfFredDebugList = gdb.parse_and_eval("_lfFredDebugList")
        _lfFredDebugLink = gdb.parse_and_eval("_lfFredDebugLink")
        first = _lfFredDebugList['anchorLink'].address
        next = _lfFredDebugList['anchorLink']['link'][_lfFredDebugLink]['next']
        if (next == 0):
            return
        while (next != first):
            FibreSupport.list.append(next)
            next = next['link'][_lfFredDebugLink]['next']
        orig_thread = gdb.selected_thread()
        for thread in gdb.selected_inferior().threads():
            thread.switch()
            currFred = str(gdb.parse_and_eval("Context::currFred"))
            # Cache the registers for this thread, in case it represents
            # a fibre
            rsp = str(gdb.parse_and_eval("$rsp")).split(None, 1)[0]
            rbp = str(gdb.parse_and_eval("$rbp")).split(None, 1)[0]
            rip = str(gdb.parse_and_eval("$rip")).split(None, 1)[0]
            FibreSupport.threads[thread.num] = {
                    'rsp': rsp,
                    'rbp': rbp,
                    'rip': rip,
                    'currFred': currFred
            }
            FibreSupport.active[currFred] = {
                    'rsp': rsp,
                    'rbp': rbp,
                    'rip': rip,
                    'thread' : thread
            }
        orig_thread.switch()
        FibreSupport.saved = True

    # restore() is hooked to continue events via script hooks to 'fibre reset'
    def restore():
        orig_thread = gdb.selected_thread()
        for thread in gdb.selected_inferior().threads():
            thread.switch()
            rsp = FibreSupport.threads[thread.num]['rsp']
            rbp = FibreSupport.threads[thread.num]['rbp']
            rip = FibreSupport.threads[thread.num]['rip']
            currFred = FibreSupport.threads[thread.num]['currFred']

            FibreSupport.prep_frame()
            # restore original register context
            gdb.execute("set $rsp = " + str(rsp))
            gdb.execute("set $rbp = " + str(rbp))
            gdb.execute("set $rip = " + str(rip))
            gdb.execute("set Context::currFred = " + str(currFred))
        orig_thread.switch()
        FibreSupport.saved = False

    def prep_frame():
        # walk stack down to innermost frame
        currframe = gdb.selected_frame()
        frame = currframe
        while (frame != gdb.newest_frame()):
            frame = frame.newer()
        frame.select()
        return currframe

    def set_fibre(arg, silent=False):
        # if current pthread: use current register context
        if (arg == gdb.parse_and_eval("Context::currFred")):
            return True
        # Check active fibre cache in case this fibre is in it
        # (FibreSupport.active is more up-to-date than
        # Fred for retrieving stack pointers)
        argstr = str(arg)
        if (argstr in FibreSupport.active):
            FibreSupport.active[argstr]['thread'].switch()
            rsp = FibreSupport.active[argstr]['rsp']
            rbp = FibreSupport.active[argstr]['rbp']
            rip = FibreSupport.active[argstr]['rip']
        else:
            # retrieve fibre's register context
            ftype = gdb.lookup_type('Fibre').pointer()
            ptr = gdb.Value(arg).cast(ftype)
            rsp = ptr['stackPointer']
            if (rsp == 0):
                if not silent:
                    print("cannot access stack pointer - active in different thread?")
                return False
            ptype = gdb.lookup_type('uintptr_t').pointer()
            rsp = rsp + 40            # cf. STACK_PUSH in src/runtime/RegisterPush.HS
            rbp = gdb.Value(rsp).cast(ptype).dereference()
            rsp = rsp + 8
            rip = gdb.Value(rsp).cast(ptype).dereference()
        # enable fibre's register context
        gdb.execute("set $rsp = " + str(rsp))
        gdb.execute("set $rbp = " + str(rbp))
        gdb.execute("set $rip = " + str(rip))
        # set Context::currFred to point to the correct fred
        gdb.execute("set Context::currFred = " + argstr)
        return True

    def backtrace(arg):
        currframe = FibreSupport.prep_frame()
        # save register context
        currFred = str(gdb.parse_and_eval("Context::currFred"))
        tmprsp = str(gdb.parse_and_eval("$rsp")).split(None, 1)[0]
        tmprbp = str(gdb.parse_and_eval("$rbp")).split(None, 1)[0]
        tmprip = str(gdb.parse_and_eval("$rip")).split(None, 1)[0]
        currthread = gdb.selected_thread()
        # execute backtrace, if possible
        if (FibreSupport.set_fibre(arg)):
            gdb.execute("backtrace")
        # restore register context
        currthread.switch()
        gdb.execute("set $rsp = " + str(tmprsp))
        gdb.execute("set $rbp = " + str(tmprbp))
        gdb.execute("set $rip = " + str(tmprip))
        gdb.execute("set Context::currFred = " + currFred)
        # restore stack frame
        currframe.select()

    # returns frame for fibre
    @contextmanager
    def get_frame(arg):
        currframe = FibreSupport.prep_frame()
        # save register context
        currFred = str(gdb.parse_and_eval("Context::currFred"))
        tmprsp = str(gdb.parse_and_eval("$rsp")).split(None, 1)[0]
        tmprbp = str(gdb.parse_and_eval("$rbp")).split(None, 1)[0]
        tmprip = str(gdb.parse_and_eval("$rip")).split(None, 1)[0]
        currthread = gdb.selected_thread()
        result = None
        try:
            # execute backtrace, if possible
            if (FibreSupport.set_fibre(arg, silent=True)):
                result = gdb.selected_frame()
                yield result
            else:
                yield None
        finally:
            # restore register context
            currthread.switch()
            gdb.execute("set $rsp = " + str(tmprsp))
            gdb.execute("set $rbp = " + str(tmprbp))
            gdb.execute("set $rip = " + str(tmprip))
            gdb.execute("set Context::currFred = " + currFred)
            # restore stack frame
            currframe.select()
        return result

class InfoFibres(gdb.Command):
    """Print list of fibres"""
    header = " Idx\tName\t\tPtr \t\t Frame"

    def __init__(self):
        super(InfoFibres, self).__init__("info fibres", gdb.COMMAND_USER)

    def get_frame_string(self, frame):
        # Print instruction pointer
        result = str(frame.read_register('rip')).split(None, 1)[0]
        result += " in "
        if frame.name() is not None:
            result += frame.name()
        sal = frame.find_sal()
        if sal is not None and sal.symtab is not None:
            result += " at " + sal.symtab.filename + ":" + str(sal.line)
        return result

    def get_fibre_name(self, idx):
        ftype = gdb.lookup_type('Fibre').pointer()              # Get Fibre ptr type
        ptr = gdb.Value(FibreSupport.list[idx]).cast(ftype)     # Get ptr to Fibre at given index
        fibre = ptr.dereference()                               # derefrence fibre ptr to get fibre structure
        return fibre['name'].format_string()[1:-1]              # get name field

    def get_row_for(self, idx, curr, frame, print_frame_info=True):
        result = ""
        if (str(FibreSupport.list[idx]) == curr):
            result += "* "
        else:
            result += "  "
        result += str(idx) + "\t" +  self.get_fibre_name(idx).ljust(15)  + str(FibreSupport.list[idx])

        if frame is not None and print_frame_info == True:
            result +=  '\t' + self.get_frame_string(frame)

        return result

    def print_all_fibres(self, curr, reg):
        print(self.header)
        for i in range(len(FibreSupport.list)):
            with FibreSupport.get_frame(FibreSupport.list[i]) as frame:
                if re.match(reg, self.get_fibre_name(i)):
                    print(self.get_row_for(i, curr, frame))

    def print_grouped_fibres(self, curr, n, reg):
        all_fibres = FibreSupport.list
        groups = {}
        all_output = []
        for i in range(len(FibreSupport.list)):
            with FibreSupport.get_frame(FibreSupport.list[i]) as frame:
                if re.match(reg, self.get_fibre_name(i)):
                    all_output.append((
                        self.get_row_for(i, curr, frame, False),
                        self.get_frame_string(frame),
                    ))

                if frame is None:
                    continue

                # Find stack similarities (up to n frames)
                newframe = frame
                prevframe = None
                rip_str = ""
                for j in range(n+1):
                    rip_str += str(newframe.read_register('rip')).split(None, 1)[0] + ","
                    # Step one frame up ("older" in gdb's vernacular)
                    try:
                        prevframe = newframe
                        newframe = newframe.older()
                        if newframe == None:
                            break
                    except:
                        break
                elem = (i, prevframe.name())
                if rip_str in groups:
                    groups[rip_str].append(elem)
                else:
                    groups[rip_str] = [elem]

        grouped_indices = set()
        sorted_groups = list(groups.items())
        sorted(sorted_groups, key=lambda x: len(x[1])) # Smaller groups first
        for rip, fibres in sorted_groups:
            if len(fibres) <= 1:
                # Skip groups of 1
                continue
            print("Fibre with same call stack from ", fibres[0][1], ":", sep='')
            indices = map(lambda x: x[0], fibres)
            joined_indices = []
            for i in indices:
                grouped_indices.add(i)
                if len(joined_indices) > 0 and (joined_indices[-1][1] + 1) == i:
                    start, end = joined_indices[-1]
                    joined_indices[-1] = (start, i)
                else:
                    joined_indices.append((i,i))
            joined_indices_strs = map(
                lambda x: '%d-%d' % (x[0], x[1]) if x[0] < x[1] else str(x[0]),
                joined_indices
            )
            print(', '.join(list(joined_indices_strs)))
            print() # Extra newline

        # Print anything not grouped
        ungrouped = set(range(len(all_output))) - grouped_indices
        if len(ungrouped) > 0:
            print('All ungrouped fibres')
            print(self.header)
            for i in ungrouped:
                fibre_info, frame_info = all_output[i]
                print(fibre_info, '\t', frame_info)

    def invoke(self, args, from_tty):
        if (not FibreSupport.saved):
            return
        curr = str(gdb.parse_and_eval("Context::currFred"))

        argv = gdb.string_to_argv(args)
        n = -1
        reg = ".*"

        if len(argv) >= 1:
            if argv[0].isdigit():
                n = int(argv[0])
            else:
                reg = argv[0]

        if len(argv) >= 2:
            if  n <= 0 and argv[1].isdigit():
                n = int(argv[1])
            else:
                reg = str(argv[1])

        try:
            if n >= 0:
                self.print_grouped_fibres(curr, n, reg)
            else:
                self.print_all_fibres(curr, reg)
        except ValueError:
            self.print_all_fibres(curr, reg)

class Fibre(gdb.Command):
    def __init__(self):
        super(Fibre, self).__init__("fibre", gdb.COMMAND_SUPPORT, gdb.COMPLETE_NONE, True)

class FibrePtr(gdb.Command):
    """Print backtrace of fibre at pointer (see 'info fibres')"""
    def __init__(self):
        super(FibrePtr, self).__init__("fibre ptr", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        if (not FibreSupport.saved):
            return
        FibreSupport.backtrace(gdb.parse_and_eval(arg))

class FibreIdx(gdb.Command):
    """Print backtrace of fibre at index (see 'info fibres')"""
    def __init__(self):
        super(FibreIdx, self).__init__("fibre idx", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        if (not FibreSupport.saved):
            return
        index = int(gdb.parse_and_eval(arg))
        if (index >= len(FibreSupport.list)):
            print("fibre", index, "does not exist")
            return
        FibreSupport.backtrace(FibreSupport.list[index])

class FibreSetPtr(gdb.Command):
    """Make fibre at pointer the active fibre (see 'info fibres')"""
    def __init__(self):
        super(FibreSetPtr, self).__init__("fibre setptr", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        if (not FibreSupport.saved):
            return
        FibreSupport.prep_frame()
        FibreSupport.set_fibre(gdb.parse_and_eval(arg))

class FibreSetIdx(gdb.Command):
    """Make fibre at index the active fibre (see 'info fibres')"""
    def __init__(self):
        super(FibreSetIdx, self).__init__("fibre setidx", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        if (not FibreSupport.saved):
            return
        index = int(gdb.parse_and_eval(arg))
        if (index >= len(FibreSupport.list)):
            print("fibre", index, "does not exist")
            return
        FibreSupport.prep_frame()
        FibreSupport.set_fibre(FibreSupport.list[index])

class FibreReset(gdb.Command):
    """You must use this command after 'fibre set...',
before continuing the target with 'step', 'next', 'cont', etc..."""
    def __init__(self):
        super(FibreReset, self).__init__("fibre reset", gdb.COMMAND_USER)

    def invoke(self, arg, from_tty):
        if (not FibreSupport.saved):
            return
        FibreSupport.restore()


class FibreBreakPoint(gdb.Breakpoint):
    def __init__(self, fibre, condition,  **kwargs):
        super().__init__(**kwargs)
        self.fibre = fibre
        self.condition = condition

    def stop(self):
        if self.fibre != gdb.parse_and_eval("Context::currFred"):
            return False

        if self.condition is not None:
            if not gdb.parse_and_eval(self.condition):
                return False

        return True

class FibreWatchPoint(gdb.Breakpoint):
    def __init__(self, fibre, wp_class,**kwargs):
        super().__init__(type=gdb.BP_WATCHPOINT ,wp_class=wp_class,  **kwargs)
        self.fibre = fibre

    def stop(self):
        if self.fibre != gdb.parse_and_eval("Context::currFred"):

            return False

        return True

class FibreBreak(gdb.Command):
    """
    Set a breakpoint at given fibre index
    Usage: fibre break <linespec> <fibre_index> [condition]
    <fibre_index> : use 'info fibres' to get index of fibres
    More about linespec: https://sourceware.org/gdb/onlinedocs/gdb/Linespec-Locations.html

    Break points created by this command behaves similar to normal gdb breakpoints
    They can be deleted in a same way, we delete normal breakpoints
    """
    def __init__(self):
        super(FibreBreak, self).__init__("fibre break", gdb.COMMAND_USER)

    def invoke(self, args, from_tty):
        if (not FibreSupport.saved):
            return

        argv = gdb.string_to_argv(args)

        if not 2 <= len(argv) <= 3:
            print("Usage: fibre break <linespec> <fibre_index> [condition]")
            return

        fibre_index = int(argv[1])

        if fibre_index < 0 or fibre_index >= len(FibreSupport.list):
            print("Invalid fibre index")
            return

        spec = argv[0]
        condition = argv[2] if len(argv) == 3 else None
        FibreBreakPoint(FibreSupport.list[fibre_index], spec=spec, condition=condition)

class FibreWatch(gdb.Command):
    """
    Set a breakpoint at given fibre index
    Usage: fibre watch <expr> <fibre_index> [type]
    fibre_index : use 'info fibres' to get index of fibres
    expr: Set a watchpoint for the given expression. GDB will break when the data location is read,
    access or being written
    type: 0 or unspecified -> write, 1 -> read, 2 -> access
    """
    def __init__(self):
        super(FibreWatch, self).__init__("fibre watch", gdb.COMMAND_USER)

    def invoke(self, args, from_tty):
        if (not FibreSupport.saved):
            return

        argv = gdb.string_to_argv(args)

        if not 2 <= len(argv) <= 3:
            print("Usage: fibre watch <expr> <fibre_index> [type]")
            return

        type = int(argv[2]) if len(argv) == 3 else 0

        if not 0 <= type <= 2:
            print("type must be 0(write), 1(read), or 2(access)")
            return

        if type == 0:
            wp_class = gdb.WP_WRITE
        elif type == 1:
            wp_class = gdb.WP_READ
        else:
            wp_class = gdb.WP_ACCESS


        fibre_index = int(argv[1])

        if fibre_index < 0 or fibre_index >= len(FibreSupport.list):
            print("Invalid fibre index")
            return

        spec = argv[0]

        FibreWatchPoint(FibreSupport.list[fibre_index], spec=spec, wp_class=wp_class)


FibreSupport()
InfoFibres()
Fibre()
FibrePtr()
FibreIdx()
FibreSetPtr()
FibreSetIdx()
FibreBreak()
FibreWatch()
FibreReset()
gdb.events.stop.connect(FibreSupport.stop_handler)
