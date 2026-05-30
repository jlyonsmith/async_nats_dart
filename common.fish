#!/usr/bin/env fish

function info
    set_color green; echo "👉 "$argv; set_color normal
end

function warning
    set_color yellow; echo "🐓 "$argv; set_color normal
end

function error
    set_color red; echo "💥 "$argv; set_color normal
end