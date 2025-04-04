# to automatically load this when libfibre.so is loaded:
# echo add-auto-load-safe-path DIRECTORY >> $HOME/.gdbinit
# or load via 'source DIRECTORY/libfibre.so-gdb.gdb'

# 'server' keyword disables confirmation dialog when re-loading/re-defining

server define hook-next
fibre reset
end

server define hook-nexti
fibre reset
end

server define hook-step
fibre reset
end

server define hook-stepi
fibre reset
end

server define hook-continue
fibre reset
end

server define hook-finish
fibre reset
end

server define hook-advance
fibre reset
end

server define hook-jump
fibre reset
end

server define hook-signal
fibre reset
end

server define hook-until
fibre reset
end

server define hook-reverse-next
fibre reset
end

server define hook-reverse-nexti
fibre reset
end

server define hook-reverse-step
fibre reset
end

server define hook-reverse-stepi
fibre reset
end

server define hook-reverse-continue
fibre reset
end

server define hook-reverse-finish
fibre reset
end
