-- atlas: Windower exporter for the Atlas Electron renderer.
-- Reads FFXI's mob array via Windower's API (no cross-process memory
-- needed -- Windower runs in-process) and broadcasts it via UDP to the
-- Electron app on localhost. The renderer handles all map drawing,
-- zoom, filtering, and active-window selection.
--
-- Data contract: one JSON object per UDP datagram, sent at TICK_HZ.
-- Schema documented in PACKET_SCHEMA below.
--
-- Commands:
--   //atlas status     -- show send rate / target / current zone
--   //atlas host <ip>  -- change target host (default 127.0.0.1)
--   //atlas port <n>   -- change target port (default 32123)
--   //atlas rate <hz>  -- change send rate in Hz (default 10)
--   //atlas pause      -- toggle broadcast on/off

_addon.name = 'atlas'
_addon.version = '0.1.0'
_addon.author = 'HB'
_addon.commands = {'atlas', 'at'}

require('logger')
config = require('config')
socket = require('socket')
packets = require('packets')

-- AI EDIT [atlas-scan]: scan-result buffers. Each tick of build_packet
-- drains these into the outgoing packet as `widescan_events` /
-- `bitzer_events` so the radar app holds the state, not the addon.
-- Cleared every send -- pure transit buffers.
local widescan_pending = {}
local bitzer_pending = {}
local clear_widescan_flag = false
local clear_bitzer_flag = false

-- AI EDIT [atlas-scan]: Sortie zones -- all three are Outer Ra'Kaznar
-- basement variants (U1/U2/U3). Mirrors bitzer.lua's safety lock for
-- `//atlas bit`.
local SORTIE_ZONES = { [133] = true, [189] = true, [275] = true }

-- AI EDIT [atlas-scan]: the four Bitzer objects in Sortie. NMs are not
-- included here -- they show up as regular live mobs once within
-- draw distance, no special tracking needed.
local BITZER_NAMES = {
    [837] = 'Bitzer (E)',
    [838] = 'Bitzer (F)',
    [839] = 'Bitzer (G)',
    [840] = 'Bitzer (H)',
}

-- AI EDIT [atlas-bit-flag]: Sortie quadrant entry-point detection.
-- When the player teleports into a quadrant they land within 25y of
-- one of these entry coords. Hitting the trigger area sets the flag;
-- flag persists until either the player gets within 15y of the
-- active quadrant's last-known Bitzer position (matches the radar's
-- proximity-clear) OR the player zones. //atlas bit (no arg) uses
-- this flag; letter override (//atlas bit e/f/g/h) bypasses it.
-- Coords corrected after in-game validation:
-- the original (579.34, -86.54) entry was physically G (not E), and
-- (651.20, -22.21) was physically F (not H). Swapped E<->G and F<->H.
local BITZER_ENTRIES = {
    { index = 837, x = 579.75, y =  42.01, z = 101.82 }, -- E
    { index = 838, x = 651.20, y = -22.21, z = 103.36 }, -- F
    { index = 839, x = 579.34, y = -86.54, z = 102.22 }, -- G
    { index = 840, x = 514.42, y = -20.27, z = 101.46 }, -- H
}
local ENTRY_TRIGGER_RADIUS_SQ = 25 * 25
local BITZER_REACH_RADIUS_SQ  = 15 * 15

local current_quadrant_bitzer = nil
local last_bitzer_pos = {}  -- [index] = { x = ..., y = ... }

local function update_quadrant_flag()
    -- AI EDIT [atlas-bit-flag]: only scan the trigger areas when the
    -- player is actually in a Sortie zone. Cheap early-out everywhere
    -- else and avoids any chance of a stray same-coord match in an
    -- unrelated zone.
    local info = windower.ffxi.get_info()
    if not info or not SORTIE_ZONES[info.zone] then return end

    local me = windower.ffxi.get_mob_by_target('me')
    if not me then return end
    -- Entry-set: 3D distance so a different floor that happens to
    -- share x/y can't trigger the entry on the wrong basement.
    for _, e in ipairs(BITZER_ENTRIES) do
        local dx = me.x - e.x
        local dy = me.y - e.y
        local dz = (me.z or 0) - e.z
        if dx * dx + dy * dy + dz * dz < ENTRY_TRIGGER_RADIUS_SQ then
            if current_quadrant_bitzer ~= e.index then
                current_quadrant_bitzer = e.index
                log(('entered quadrant for %s'):format(BITZER_NAMES[e.index]))
            end
            return
        end
    end
    -- Reach-clear: drop the flag once the player gets close to the
    -- active quadrant's last-known Bitzer position. Bitzer positions
    -- are recorded by the 0x0F4 / 0x00D / 0x00E hooks below.
    if current_quadrant_bitzer then
        local pos = last_bitzer_pos[current_quadrant_bitzer]
        if pos then
            local dx = me.x - pos.x
            local dy = me.y - pos.y
            if dx * dx + dy * dy < BITZER_REACH_RADIUS_SQ then
                log(('reached %s, clearing quadrant flag'):format(BITZER_NAMES[current_quadrant_bitzer]))
                current_quadrant_bitzer = nil
            end
        end
    end
end

--[[ -----------------------------------------------------------------
     PACKET_SCHEMA (informational, kept here as source of truth):

     {
       "v": 1,                       -- schema version
       "char_name": "Hellblade",
       "char_id":   12345678,
       "zone_id":   230,             -- FFXI zone id
       "ts":        1748275200.123,  -- os.clock() at send
       "player": {
         "x": -150.2, "y": 73.8, "z": 0,
         "heading": 1.57,            -- radians
         "hpp": 100, "mpp": 100, "tpp": 1250,
         "status": 0                 -- 0=idle, 1=engaged, 2=KO, 3=casting, ...
       },
       "mobs": [
         {
           "id":         18234567,
           "index":      512,
           "name":       "Apex Toad",
           "x": -140, "y": 80, "z": 0,
           "hpp":        87,
           "claim_id":   12345678,    -- 0 = unclaimed
           "spawn_type": 16,          -- bitfield: 1=PC, 2=NPC, 16=Mob, ...
           "is_npc":     true,
           "valid":      true         -- mob.valid_target
         },
         ...
       ]
     }
----------------------------------------------------------------- ]]

local defaults = {
    host = '127.0.0.1',
    port = 32123,
    rate_hz = 10,
    enabled = true,
    -- Only mobs within this many yalms of the player are included in
    -- the packet. Set to 0 for unlimited (no cull). Default matches
    -- FFXI's draw distance for non-widescan classes.
    max_distance = 50,
}
local settings = config.load(defaults)

local udp = nil
local pkts_sent = 0
local last_status_log = 0
-- AI EDIT [apradar-ffxideck-lessons]: ffxideckcrash.lua taught us that
-- aggressive socket lifecycle ops inside FFXI's process crash the game.
-- We open ONE udp socket on load, never close/reopen per tick. Polling
-- runs via coroutine.schedule (not prerender) so the scheduler isn't
-- hammered every frame, and every socket call is pcall-wrapped so a
-- transient failure can't unwind into the host.
local running = false
local socket_ok = false

local function open_socket()
    local ok, sock = pcall(socket.udp)
    if not ok or not sock then
        socket_ok = false
        log('failed to create UDP socket: ' .. tostring(sock))
        return false
    end
    local ok2, err = pcall(sock.settimeout, sock, 0)
    if not ok2 then
        socket_ok = false
        log('settimeout failed: ' .. tostring(err))
        pcall(sock.close, sock)
        return false
    end
    udp = sock
    socket_ok = true
    return true
end

-- AI EDIT [atlas-scan]: hook the same incoming packets bitzer.lua hooks.
-- 0x0F4 -> widescan result (one packet per mob), 0x00D/0x00E -> mob
-- update (used to track Bitzer object positions in Sortie). All work
-- is pcall-wrapped so a parse failure can't unwind into Windower's
-- host process.
windower.register_event('incoming chunk', function(id, data)
    if id == 0x0F4 then
        local ok = pcall(function()
            local me = windower.ffxi.get_mob_by_target('me')
            if not me then return end

            -- AI EDIT [atlas-widescan-fields]: parse via Windower's
            -- schema (libs/packets/fields.lua) so we get Name + Type
            -- straight from the packet. The previous raw-byte path
            -- only had Index + x/y and relied on get_mob_by_index for
            -- the name, which returns nil for mobs outside the
            -- client's draw distance -- defeating the entire point of
            -- widescan. PacketViewer confirmed Name is populated for
            -- both Type 1 (Friendly) and Type 2 (Enemy), as a slugged
            -- 16-char form ("Ashen Tiger" -> "AshenTiger").
            local p = packets.parse('incoming', data)
            if not p or not p['Index'] then return end

            local idx = p['Index']
            local x = me.x + (p['X Offset'] or 0)
            local y = me.y + (p['Y Offset'] or 0)

            -- Strip char[16] null padding.
            local name = p['Name']
            if name then
                name = name:gsub('%z.*$', '')
                if name == '' then name = nil end
            end

            -- Use the live mob array for richer info (real id + exact
            -- spawn_type / is_npc) when the mob IS in draw distance.
            -- Out-of-draw-distance results rely on the packet Name.
            local mob = windower.ffxi.get_mob_by_index(idx)
            if (not name or name == '') and mob and mob.name then
                name = mob.name
            end
            if not name or name == '' then return end

            -- Widescan Type -> rough spawn_type / is_npc so the radar's
            -- mob-filter vs npc-filter routing picks the right side.
            -- ws-mob enum: 0 = Other, 1 = Friendly, 2 = Enemy.
            local ws_type = p['Type']
            local is_npc, spawn_type
            if mob and mob.spawn_type then
                spawn_type = mob.spawn_type
                is_npc = mob.is_npc and true or false
            elseif ws_type == 'Enemy' or ws_type == 2 then
                spawn_type, is_npc = 16, true   -- monster
            else
                spawn_type, is_npc = 2, true    -- NPC / Other
            end

            widescan_pending[#widescan_pending + 1] = {
                id         = (mob and mob.id) or idx,
                index      = idx,
                name       = name,
                x          = x,
                y          = y,
                z          = me.z,  -- z not in packet; player's is a good approx
                spawn_type = spawn_type,
                is_npc     = is_npc,
            }

            -- Bitzer-specific staging (unchanged behavior).
            if BITZER_NAMES[idx] then
                bitzer_pending[#bitzer_pending + 1] = {
                    index = idx,
                    name  = BITZER_NAMES[idx],
                    x     = x,
                    y     = y,
                }
                last_bitzer_pos[idx] = { x = x, y = y }
            end
        end)
        if not ok then
            -- Silent: malformed widescan packets shouldn't spam the chat log
        end
    elseif id == 0x00D or id == 0x00E then
        local ok = pcall(function()
            local p = packets.parse('incoming', data)
            local idx = p['Index']
            if not BITZER_NAMES[idx] then return end
            if p['X'] == 0 and p['Y'] == 0 then return end
            -- Only treat as active when the entity reports alive
            -- (status 0); bitzer.lua uses the same gate to avoid
            -- "reviving" entries from corpse packets.
            local is_bitzer = (idx >= 837 and idx <= 840)
            if is_bitzer or p['Status'] == 0 then
                bitzer_pending[#bitzer_pending + 1] = {
                    index = idx,
                    name  = BITZER_NAMES[idx],
                    x     = p['X'],
                    y     = p['Y'],
                }
                last_bitzer_pos[idx] = { x = p['X'], y = p['Y'] }
            end
        end)
        if not ok then end
    end
end)

-- AI EDIT [atlas-bit-flag]: //atlas bit trigger. Uses the quadrant
-- flag set by update_quadrant_flag() when the player walked into a
-- quadrant entry area. Letter override bypasses the flag entirely.
local function smart_ping_bitzer(letter)
    local manual_map = { e = 837, f = 838, g = 839, h = 840 }
    local target_index = manual_map[letter and letter:lower() or '']
    if not target_index then
        target_index = current_quadrant_bitzer
    end
    if target_index then
        local ok = pcall(function()
            packets.inject(packets.new('outgoing', 0x016, { ['Target Index'] = target_index }))
        end)
        if ok then log(('bitzer ping idx=%d'):format(target_index)) end
    else
        log('no Bitzer quadrant set; walk near a quadrant entry or use //atlas bit <e|f|g|h>')
    end
end

-- AI EDIT [atlas-scan]: widescan trigger. Outgoing 0x0F4 Flags=1 is
-- the standard widescan request -- same packet bitzer.lua's `ping`
-- command sends.
local function trigger_widescan()
    local ok = pcall(function()
        packets.inject(packets.new('outgoing', 0x0F4, { ['Flags'] = 1 }))
    end)
    if not ok then log('widescan trigger failed') end
end

-- AI EDIT [atlas-widescan-cooldown]: client-side rate limit on //at ws.
-- 5s between requests, since spamming the command faster than the
-- server's own cooldown wastes a packet and clutters the log. Tracked
-- in wall-clock seconds via os.time().
local last_widescan_at = 0
local WIDESCAN_COOLDOWN_S = 5

--[[ -----------------------------------------------------------------
     Minimal JSON encoder (avoids pulling in a dependency).
     Handles only the value types we actually emit: numbers, strings,
     booleans, nil, arrays, objects. Strings get backslash-escaped
     for the few characters JSON requires.
----------------------------------------------------------------- ]]
local function json_escape_str(s)
    s = s:gsub('\\', '\\\\')
    s = s:gsub('"',  '\\"')
    s = s:gsub('\n', '\\n')
    s = s:gsub('\r', '\\r')
    s = s:gsub('\t', '\\t')
    s = s:gsub('[\1-\31]', function(c) return ('\\u%04x'):format(c:byte()) end)
    return s
end

local function json_encode(v)
    local t = type(v)
    if t == 'nil' or v == nil then return 'null' end
    if t == 'boolean' then return v and 'true' or 'false' end
    if t == 'number' then
        if v ~= v then return 'null' end           -- NaN
        if v == math.huge or v == -math.huge then return 'null' end
        if v == math.floor(v) and math.abs(v) < 1e15 then
            return tostring(math.floor(v))
        end
        -- AI EDIT [atlas-precision]: 2-decimal cap on floats. 0.01y
        -- horizontal precision is well past anything visible on the
        -- radar, and 0.01 rad heading precision is ~0.5 degrees --
        -- plenty for the arrow direction.
        return ('%.2f'):format(v):gsub('%.?0+$', '')
    end
    if t == 'string' then return '"' .. json_escape_str(v) .. '"' end
    if t == 'table' then
        -- Array if all keys are 1..N
        local n = 0
        for _ in pairs(v) do n = n + 1 end
        local is_array = (n == #v) and (n > 0 or next(v) == nil)
        if is_array then
            local out = {}
            for i = 1, n do out[i] = json_encode(v[i]) end
            return '[' .. table.concat(out, ',') .. ']'
        else
            local out = {}
            for k, val in pairs(v) do
                out[#out + 1] = '"' .. json_escape_str(tostring(k)) .. '":' .. json_encode(val)
            end
            return '{' .. table.concat(out, ',') .. '}'
        end
    end
    return 'null'
end

--[[ -----------------------------------------------------------------
     Snapshot the world into the packet table.
----------------------------------------------------------------- ]]
local function build_packet()
    local player = windower.ffxi.get_player()
    if not player then return nil end

    local info = windower.ffxi.get_info()
    if not info then return nil end

    local self_mob = windower.ffxi.get_mob_by_target('me')
    if not self_mob then return nil end

    -- AI EDIT [apradar-target]: include the player's currently targeted
    -- entity id so the renderer can render it always-visible regardless
    -- of filter settings, and color it distinctly.
    local target_mob = windower.ffxi.get_mob_by_target('t')
    local target_id = target_mob and target_mob.id or 0

    -- AI EDIT [apradar-mh-flag]: forward the FFXI client's mog_house
    -- flag so the renderer can swap to its blank-interior fallback
    -- map even when the zone id itself has a real map.ini entry.
    -- (Some mog house variants live in regular zones flagged via this
    -- boolean rather than via a unique zone id.)
    local pkt = {
        v = 1,
        char_name = player.name,
        char_id   = player.id,
        zone_id   = info.zone,
        mog_house = info.mog_house and true or false,
        ts        = os.clock(),
        player = {
            x = self_mob.x,
            y = self_mob.y,
            z = self_mob.z,
            heading = self_mob.heading,
            hpp = player.vitals and player.vitals.hpp or 100,
            mpp = player.vitals and player.vitals.mpp or 100,
            tpp = player.vitals and player.vitals.tp or 0,
            status = self_mob.status,
            target_id = target_id,
        },
        mobs = {},
    }

    local max_d_sq = settings.max_distance * settings.max_distance
    local cull = settings.max_distance > 0

    -- AI EDIT [apradar-mob-cap]: hard caps + position sanity filter.
    -- Two issues drove this:
    --   1. FFXI's mob array keeps stale slots from previous zones with
    --      x=y=z=0 -- they pass the 50y distance check against any
    --      player near origin and inflate the packet to hundreds of
    --      "mobs". (Observed: 526 mobs reported in a fresh zone.)
    --   2. A single UDP datagram caps at 65507 bytes. ~150 bytes per
    --      mob entry means anything past ~400 entries gets dropped or
    --      silently truncated by the OS.
    -- Solution: skip entries whose position is exactly (0,0,0), and
    -- cap the result list at MOB_CAP regardless. 200 entries is
    -- comfortably under the UDP limit (~30KB) and far more than the
    -- ~50 entities ever visible within FFXI's draw distance.
    -- AI EDIT [atlas-scan]: drain scan buffers into the outgoing packet.
    -- The radar app owns the persistent scan state; the addon only
    -- forwards events (deltas) and clear-flags. Empty arrays / absent
    -- flags are the steady state when no scan is happening.
    if #widescan_pending > 0 then
        pkt.widescan_events = widescan_pending
        widescan_pending = {}
    end
    if #bitzer_pending > 0 then
        pkt.bitzer_events = bitzer_pending
        bitzer_pending = {}
    end
    if clear_widescan_flag then
        pkt.clear_widescan = true
        clear_widescan_flag = false
    end
    if clear_bitzer_flag then
        pkt.clear_bitzer = true
        clear_bitzer_flag = false
    end

    local MOB_CAP = 200
    local count = 0
    for _, m in pairs(windower.ffxi.get_mob_array()) do
        if count >= MOB_CAP then break end
        -- AI EDIT [apradar-send-all-renderer-filters]: phantom filter
        -- moved client-side so a "Show Hidden" toggle in the renderer
        -- can reveal them on demand. Addon still drops truly bogus
        -- entries at (0,0,0) -- those are uninitialized array slots,
        -- never real entities the renderer would want to see.
        if m and m.id and m.id ~= player.id and m.x and m.y
                and not (m.x == 0 and m.y == 0 and (m.z or 0) == 0) then
            local include = true
            if cull then
                local dx, dy = m.x - self_mob.x, m.y - self_mob.y
                if dx * dx + dy * dy > max_d_sq then include = false end
            end
            if include then
                pkt.mobs[#pkt.mobs + 1] = {
                    id         = m.id,
                    index      = m.index,
                    name       = m.name,
                    x          = m.x,
                    y          = m.y,
                    z          = m.z,
                    hpp        = m.hpp,
                    claim_id   = m.claim_id or 0,
                    spawn_type = m.spawn_type,
                    is_npc     = m.is_npc and true or false,
                    valid      = m.valid_target and true or false,
                }
                count = count + 1
            end
        end
    end

    return pkt
end

--[[ -----------------------------------------------------------------
     Tick loop -- self-rescheduling coroutine, NOT a prerender hook.
     Following ffxideck's v2.1 pattern: polling via coroutine.schedule
     gives the addon control over its own cadence and keeps the
     prerender pipeline lean. Every socket op is pcall-wrapped.
----------------------------------------------------------------- ]]
local function send_packet()
    if not (running and socket_ok and udp and settings.enabled) then return end

    local pkt = build_packet()
    if not pkt then return end

    -- sendto is non-blocking (settimeout=0). pcall keeps a transient
    -- winsock failure from propagating into Windower's host.
    local ok, err = pcall(udp.sendto, udp, json_encode(pkt), settings.host, settings.port)
    if ok then
        pkts_sent = pkts_sent + 1
    elseif (os.clock() - last_status_log) > 30 then
        last_status_log = os.clock()
        log('sendto error: ' .. tostring(err))
    end
end

local function tick_loop()
    if not running then return end
    -- AI EDIT [atlas-bit-flag]: refresh the quadrant flag every tick
    -- so entry-area triggers and reach-clears fire at the addon's
    -- normal cadence without needing a separate scheduler.
    update_quadrant_flag()
    send_packet()
    local interval = 1 / math.max(1, settings.rate_hz)
    coroutine.schedule(tick_loop, interval)
end

--[[ -----------------------------------------------------------------
     Events
----------------------------------------------------------------- ]]
windower.register_event('load', function()
    open_socket()
    running = true
    coroutine.schedule(tick_loop, 1 / math.max(1, settings.rate_hz))
    log(('exporter loaded -- target=%s:%d rate=%dHz cull=%dy'):format(
        settings.host, settings.port, settings.rate_hz, settings.max_distance))
end)

windower.register_event('unload', function()
    running = false
    if udp then
        pcall(udp.close, udp)
        udp = nil
    end
    socket_ok = false
end)

-- AI EDIT [atlas-bit-flag]: zone changes invalidate the quadrant
-- state -- the bitzer positions captured on the old basement are
-- meaningless in the new zone. Stays in lock-step with the radar's
-- own zone-change clear in main.js.
windower.register_event('zone change', function()
    current_quadrant_bitzer = nil
    last_bitzer_pos = {}
end)

windower.register_event('addon command', function(cmd, ...)
    cmd = (cmd or 'help'):lower()
    local args = {...}

    if cmd == 'status' then
        log(('host=%s port=%d rate=%dHz cull=%dy enabled=%s sent=%d ws_buf=%d bit_buf=%d'):format(
            settings.host, settings.port, settings.rate_hz,
            settings.max_distance, tostring(settings.enabled), pkts_sent,
            #widescan_pending, #bitzer_pending))
    elseif cmd == 'ws' or cmd == 'widescan' then
        -- AI EDIT [atlas-widescan-cooldown]: refuse if within the 5s
        -- window since the last request.
        local now = os.time()
        local elapsed = now - last_widescan_at
        if elapsed < WIDESCAN_COOLDOWN_S then
            log(('widescan on cooldown (%ds left)'):format(WIDESCAN_COOLDOWN_S - elapsed))
        else
            -- AI EDIT [atlas-scan]: trigger a widescan. Results flow in via
            -- the 0x0F4 hook and ship out in the next regular packet.
            trigger_widescan()
            last_widescan_at = now
            log('widescan request sent')
        end
    elseif cmd == 'bit' or cmd == 'bitzer' then
        -- AI EDIT [atlas-scan]: Sortie-only safety lock matches bitzer.lua.
        local info = windower.ffxi.get_info()
        if not info or not SORTIE_ZONES[info.zone] then
            log(('//atlas %s is only available in Sortie zones'):format(cmd))
        else
            smart_ping_bitzer(args[1])
        end
    elseif cmd == 'test' then
        -- AI EDIT [atlas-test]: dev/test helper, same pattern as
        -- bitzer.lua's 'test' command. Reads the player's current
        -- target and stages it into the bitzer table as if it had
        -- come from a real 0x00D response. Lets you exercise the
        -- radar's marker + off-screen arrow + proximity-clear flow
        -- against any stationary entity outside Sortie.
        local target = windower.ffxi.get_mob_by_target('t')
        if not target then
            log('test: no target -- target an entity first, then //at test')
        else
            bitzer_pending[#bitzer_pending + 1] = {
                index = target.index,
                name  = target.name,
                x     = target.x,
                y     = target.y,
            }
            last_bitzer_pos[target.index] = { x = target.x, y = target.y }
            log(('test: added %s (idx=%d) at (%.1f, %.1f)'):format(target.name, target.index, target.x, target.y))
        end
    elseif cmd == 'clear' then
        -- AI EDIT [atlas-scan]: clear flags ride the next outgoing packet
        -- so the radar empties its tables in lock-step with the addon's
        -- own pending buffer.
        local what = args[1] and args[1]:lower() or 'all'
        if what == 'ws' or what == 'widescan' then
            widescan_pending = {}
            clear_widescan_flag = true
            log('widescan cleared')
        elseif what == 'bit' or what == 'bitzer' then
            bitzer_pending = {}
            clear_bitzer_flag = true
            current_quadrant_bitzer = nil
            last_bitzer_pos = {}
            log('bitzer cleared')
        else
            widescan_pending = {}
            bitzer_pending = {}
            clear_widescan_flag = true
            clear_bitzer_flag = true
            current_quadrant_bitzer = nil
            last_bitzer_pos = {}
            log('widescan + bitzer cleared')
        end
    elseif cmd == 'host' and args[1] then
        settings.host = args[1]
        settings:save()
        open_socket()
        log('host = ' .. settings.host)
    elseif cmd == 'port' and args[1] then
        local n = tonumber(args[1])
        if n and n > 0 and n < 65536 then
            settings.port = n
            settings:save()
            log('port = ' .. settings.port)
        end
    elseif cmd == 'rate' and args[1] then
        local n = tonumber(args[1])
        if n and n >= 1 and n <= 60 then
            settings.rate_hz = n
            settings:save()
            log('rate = ' .. settings.rate_hz .. ' Hz')
        end
    elseif cmd == 'cull' and args[1] then
        local n = tonumber(args[1])
        if n and n >= 0 then
            settings.max_distance = n
            settings:save()
            log('cull = ' .. settings.max_distance .. ' yalms (0 = unlimited)')
        end
    elseif cmd == 'pause' or cmd == 'toggle' then
        settings.enabled = not settings.enabled
        settings:save()
        log('enabled = ' .. tostring(settings.enabled))
    else
        log('--- atlas commands ---')
        log('//atlas status        -- show config + send count')
        log('//atlas host <ip>     -- set destination host')
        log('//atlas port <n>      -- set destination port')
        log('//atlas rate <hz>     -- send rate in Hz (1..60)')
        log('//atlas cull <yalms>  -- distance cull radius (0 = unlimited)')
        log('//atlas pause         -- toggle broadcast on/off')
        log('//atlas ws            -- trigger widescan')
        log('//atlas bit [e|f|g|h] -- trigger Bitzer ping (Sortie only)')
        log('//atlas clear [ws|bit] -- clear scan results (both if no arg)')
        log('//atlas test         -- stage your target as a fake bitzer (dev/test)')
    end
end)
