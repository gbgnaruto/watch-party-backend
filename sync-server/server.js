const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { spawn, exec } = require('child_process');
const util = require('util');
const path = require('path');
const fs = require('fs');

const execAsync = util.promisify(exec);
const app = express();

app.use(cors());
app.use(express.json());

// Ensure public directory exists
const publicDir = path.join(__dirname, 'public');
if (!fs.existsSync(publicDir)) {
    fs.mkdirSync(publicDir, { recursive: true });
}
app.use('/public', express.static(publicDir));

// ── Disk Cleanup — Auto-delete stream folders older than 4 hours ──
const STREAM_MAX_AGE_MS = 4 * 60 * 60 * 1000; // 4 hours
const CLEANUP_INTERVAL_MS = 30 * 60 * 1000;    // Every 30 minutes

function cleanOldStreams() {
    try {
        if (!fs.existsSync(publicDir)) return;
        const entries = fs.readdirSync(publicDir);
        let deleted = 0, freed = 0;

        for (const name of entries) {
            if (!name.startsWith('stream_')) continue;
            const dir = path.join(publicDir, name);
            try {
                const stat = fs.statSync(dir);
                if (!stat.isDirectory()) continue;
                const ageMs = Date.now() - stat.mtimeMs;
                if (ageMs < STREAM_MAX_AGE_MS) continue;

                const files = fs.readdirSync(dir);
                for (const f of files) {
                    try { freed += fs.statSync(path.join(dir, f)).size; } catch(_) {}
                    try { fs.unlinkSync(path.join(dir, f)); } catch(_) {}
                }
                fs.rmdirSync(dir);
                deleted++;

                for (const [jid, job] of Object.entries(jobs)) {
                    if (job.streamId === name) {
                        delete jobs[jid];
                        break;
                    }
                }
            } catch(e) {
                console.warn(`[Cleanup] Could not remove ${name}:`, e.message);
            }
        }

        if (deleted > 0) {
            const mb = (freed / 1024 / 1024).toFixed(1);
            console.log(`[Cleanup] Removed ${deleted} stream(s), freed ${mb} MB`);
        }
    } catch(e) {
        console.error('[Cleanup] Error scanning public dir:', e.message);
    }
}

cleanOldStreams();
setInterval(cleanOldStreams, CLEANUP_INTERVAL_MS);

// Manual cleanup endpoint
app.post('/api/cleanup', (req, res) => {
    cleanOldStreams();
    const dirs = fs.existsSync(publicDir) ? fs.readdirSync(publicDir).filter(n => n.startsWith('stream_')).length : 0;
    res.json({ status: 'ok', remainingStreams: dirs });
});

// Disk usage endpoint
app.get('/api/disk', (req, res) => {
    try {
        let totalBytes = 0;
        const streams = [];
        if (fs.existsSync(publicDir)) {
            for (const name of fs.readdirSync(publicDir)) {
                if (!name.startsWith('stream_')) continue;
                const dir = path.join(publicDir, name);
                let size = 0;
                try {
                    for (const f of fs.readdirSync(dir)) {
                        try { size += fs.statSync(path.join(dir, f)).size; } catch(_) {}
                    }
                } catch(_) {}
                const ageMins = Math.round((Date.now() - fs.statSync(dir).mtimeMs) / 60000);
                totalBytes += size;
                streams.push({ name, sizeMb: (size/1024/1024).toFixed(1), ageMins });
            }
        }
        res.json({
            totalMb: (totalBytes/1024/1024).toFixed(1),
            streamCount: streams.length,
            streams: streams.sort((a,b) => b.ageMins - a.ageMins)
        });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// Root Keep-Alive Route
app.get('/', (req, res) => {
    res.status(200).send('SyncTube Backend is Awake and Running! 🚀');
});

// In-memory job store
const jobs = {};
const LIVE_START_SEGMENTS = 5;

// --- Conversion Route (Asynchronous Event-Loop Safe) ---
app.post('/api/convert', async (req, res) => {
    const { videoUrl } = req.body;

    if (!videoUrl || typeof videoUrl !== 'string') {
        return res.status(400).json({ error: 'Valid Video URL required' });
    }
    try {
        new URL(videoUrl);
    } catch (_) {
        return res.status(400).json({ error: 'Invalid URL format' });
    }

    const jobId    = `job_${Date.now()}_${Math.random().toString(36).slice(2,7)}`;
    const streamId = `stream_${Date.now()}`;
    const streamDir = path.join(publicDir, streamId);
    if (!fs.existsSync(streamDir)) fs.mkdirSync(streamDir, { recursive: true });

    const outputPath = path.join(streamDir, 'playlist.m3u8');

    jobs[jobId] = {
        status: 'pending',
        streamId,
        streamDir,
        manifestUrl: `/public/${streamId}/playlist.m3u8`,
        startedAt: Date.now(),
        segments: 0
    };

    res.json({ status: 'queued', jobId });

    // Execute background tasks asynchronously to prevent blocking the event loop
    (async () => {
        let numAudio = 1;
        let videoTitle = '';
        let videoDuration = 0;
        let audioLangs = [];

        try {
            const safeUrl = videoUrl.replace(/"/g, '\"');
            const { stdout: probeJson } = await execAsync(
                `ffprobe -v quiet -print_format json -show_format -show_streams "${safeUrl}"`,
                { timeout: 20000 }
            );
            const probe = JSON.parse(probeJson);

            videoTitle = probe.format?.tags?.title || probe.format?.tags?.TITLE || '';
            videoDuration = parseFloat(probe.format?.duration || 0);

            const audioStreams = (probe.streams || []).filter(s => s.codec_type === 'audio');
            numAudio = audioStreams.length || 1;
            audioLangs = audioStreams.map((s, i) => {
                const lang = s.tags?.language || s.tags?.LANGUAGE || '';
                const title = s.tags?.title || s.tags?.TITLE || '';
                const langMap = { eng:'English', hin:'Hindi', jpn:'Japanese', tam:'Tamil',
                                  tel:'Telugu', fra:'French', spa:'Spanish', kor:'Korean',
                                  ara:'Arabic', por:'Portuguese', deu:'German', zho:'Chinese' };
                const label = title || langMap[lang] || (lang ? lang.toUpperCase() : `Track ${i+1}`);
                return { index: i, lang, label };
            });
        } catch(e) {
            console.log('[Probe] ffprobe failed:', e.message);
        }

        jobs[jobId].title = videoTitle;
        jobs[jobId].duration = videoDuration;
        jobs[jobId].audioLangs = audioLangs;

        const thumbPath = path.join(streamDir, 'thumb.jpg');
        try {
            await execAsync(
                `ffmpeg -y -ss 10 -i "${videoUrl.replace(/"/g,'\"')}" -frames:v 1 -q:v 2 -vf scale=320:-1 "${thumbPath}"`,
                { timeout: 25000 }
            );
            jobs[jobId].thumbUrl = `/public/${streamId}/thumb.jpg`;
        } catch(e) {
            console.log('[Thumb] Thumbnail generation failed:', e.message);
        }

        const commonInput = [
            '-user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            '-headers', 'Referer: https://www.google.com/\r\nAccept: */*\r\nAccept-Language: en-US,en;q=0.9\r\n',
            '-reconnect', '1', '-reconnect_streamed', '1', '-reconnect_delay_max', '5',
            '-i', videoUrl
        ];

        let args;
        if (numAudio <= 1) {
            args = [
                '-y',
                ...commonInput,
                '-map', '0:v:0', '-map', '0:a:0',
                '-c:v', 'copy', '-c:a', 'aac', '-ac', '2', '-b:a', '192k',
                '-max_muxing_queue_size', '9999',
                '-f', 'hls', '-hls_time', '6', '-hls_list_size', '0',
                '-hls_flags', 'append_list',
                '-hls_segment_filename', path.join(streamDir, 'seg_%03d.ts'),
                outputPath
            ];
        } else {
            args = [
                '-y',
                ...commonInput,
                '-map', '0:v:0', '-map', '0:a:0',
                '-c:v', 'copy', '-c:a', 'aac', '-ac', '2', '-b:a', '192k',
                '-max_muxing_queue_size', '9999',
                '-f', 'hls', '-hls_time', '6', '-hls_list_size', '0',
                '-hls_flags', 'append_list',
                '-hls_segment_filename', path.join(streamDir, 'seg_0_%03d.ts'),
                path.join(streamDir, 'stream_0.m3u8')
            ];

            for (let i = 1; i < numAudio; i++) {
                const aArgs = [
                    '-y',
                    ...commonInput,
                    '-map', `0:a:${i}`,
                    '-vn',
                    '-c:a', 'aac', '-ac', '2', '-b:a', '192k',
                    '-f', 'hls', '-hls_time', '6', '-hls_list_size', '0',
                    '-hls_flags', 'append_list',
                    '-hls_segment_filename', path.join(streamDir, `seg_${i}_%03d.ts`),
                    path.join(streamDir, `stream_${i}.m3u8`)
                ];
                const aProc = spawn('ffmpeg', aArgs);
                aProc.stderr.on('data', () => {});
                aProc.on('error', () => {});
            }

            jobs[jobId]._buildMaster = () => {
                try {
                    let master = '#EXTM3U\n#EXT-X-VERSION:3\n\n';
                    for (let i = 0; i < numAudio; i++) {
                        const label = audioLangs[i]?.label || `Track ${i+1}`;
                        const lang  = audioLangs[i]?.lang  || 'und';
                        const def   = i === 0 ? 'YES' : 'NO';
                        const uri   = i === 0 ? 'stream_0.m3u8' : `stream_${i}.m3u8`;
                        master += `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="${label}",LANGUAGE="${lang}",DEFAULT=${def},AUTOSELECT=${def},URI="${uri}"\n`;
                    }
                    master += `\n#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO="audio"\nstream_0.m3u8\n`;
                    fs.writeFileSync(outputPath, master);
                } catch(e) {}
            };
        }

        const proc = spawn('ffmpeg', args);

        const segWatcher = fs.watch(streamDir, (event, filename) => {
            if (filename && filename.endsWith('.ts')) {
                const segs = fs.readdirSync(streamDir).filter(f => f.endsWith('.ts')).length;
                jobs[jobId].segments = segs;
                if (jobs[jobId].status === 'pending' && segs >= LIVE_START_SEGMENTS) {
                    jobs[jobId].status = 'live';
                }
            }
        });

        let ffmpegBuffer = '';
        proc.stderr.on('data', d => {
            ffmpegBuffer += d.toString();
            const lines = ffmpegBuffer.split('\r');
            ffmpegBuffer = lines.pop();
            for (const line of lines) {
                if (!line.includes('time=')) continue;
                const m = line.match(/time=([\d:]+\.?\d*)/);
                if (m) {
                    const parts = m[1].split(':').map(Number);
                    const secs = parts.length === 3 ? parts[0]*3600 + parts[1]*60 + parts[2] : parts[0]*60 + parts[1];
                    jobs[jobId].progress = {
                        currentTime: secs,
                        duration: jobs[jobId].duration || 0,
                        pct: jobs[jobId].duration > 0 ? Math.min(99, Math.round((secs / jobs[jobId].duration) * 100)) : null
                    };
                }
            }
        });

        proc.on('close', code => {
            segWatcher.close();
            if (code === 0) {
                if (jobs[jobId]?._buildMaster) {
                    jobs[jobId]._buildMaster();
                }
                jobs[jobId].status = 'done';
            } else {
                if (jobs[jobId]?.status === 'pending') {
                    jobs[jobId].status = 'error';
                    jobs[jobId].error = `FFmpeg exited with code ${code}`;
                }
            }
        });

        proc.on('error', err => {
            segWatcher.close();
            if (jobs[jobId].status === 'pending') {
                jobs[jobId].status = 'error';
                jobs[jobId].error = err.message;
            }
        });
    })().catch(err => console.error('[Convert Background Error]', err));
});

// --- Job Status Polling Route ---
app.get('/api/convert/status/:jobId', (req, res) => {
    const job = jobs[req.params.jobId];
    if (!job) return res.status(404).json({ error: 'Job not found' });

    if (job.status === 'live' || job.status === 'done') {
        return res.json({
            status: 'Success',
            manifestUrl: job.manifestUrl,
            segments: job.segments,
            live: job.status === 'live',
            title: job.title || '',
            duration: job.duration || 0,
            audioLangs: job.audioLangs || [],
            thumbUrl: job.thumbUrl ? `https://${req.get('host')}${job.thumbUrl}` : ''
        });
    }
    if (job.status === 'error') {
        return res.json({ status: 'Error', error: job.error });
    }
    res.json({
        status: 'pending',
        segments: job.segments || 0,
        progress: job.progress || null,
        title: job.title || '',
        duration: job.duration || 0
    });
});

// Health check
app.get('/api/health', (req, res) => {
    exec('ffmpeg -version 2>&1', (err, stdout) => {
        if (err) return res.status(500).json({ status: 'error', ffmpeg: 'NOT FOUND' });
        res.json({ status: 'ok', ffmpeg: stdout.split('\n')[0] });
    });
});

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

const rooms = {};

io.on('connection', (socket) => {
    console.log(`User connected: ${socket.id}`);
    emitActiveRooms();

    socket.on('join_room', (data, callback) => {
        const { roomId, roomName, password, username, userId, photo } = data;

        if (rooms[roomId] && rooms[roomId].password && rooms[roomId].password !== password) {
            return callback({ success: false, message: "Incorrect password." });
        }

        if (!rooms[roomId]) {
            rooms[roomId] = {
                id: roomId, name: roomName, password: password || null,
                host: null, users: [], playlist: [], currentVideo: null
            };
        }

        const room = rooms[roomId];
        const existingUserIndex = room.users.findIndex(u => u.userId === userId);
        let assignHost = false;
        let assignCoHost = false;
        let isARefresh = false;

        if (existingUserIndex !== -1) {
            isARefresh = true;
            const oldUserInstance = room.users[existingUserIndex];

            if (oldUserInstance.timeoutId) {
                clearTimeout(oldUserInstance.timeoutId);
            }

            assignHost = oldUserInstance.isHost;
            assignCoHost = oldUserInstance.isCoHost;
            room.users.splice(existingUserIndex, 1);
        } else if (room.users.filter(u => !u.isPendingRemoval).length === 0) {
            assignHost = true;
        }

        const userObj = { 
            socketId: socket.id, 
            userId, 
            username, 
            photo, 
            isHost: assignHost, 
            isCoHost: assignCoHost,
            isPendingRemoval: false,
            timeoutId: null 
        };

        room.users.push(userObj);

        if (assignHost) room.host = socket.id;

        socket.join(roomId);
        callback({ success: true });

        socket.emit('room_data', { isHost: assignHost, isCoHost: assignCoHost, playlist: room.playlist, currentVideo: room.currentVideo });
        io.to(roomId).emit('update_users', room.users.filter(u => !u.isPendingRemoval));

        if (!isARefresh) {
            io.to(roomId).emit('chat_message', { system: true, text: `${username} joined the party 🍿` });
        }

        emitActiveRooms();
    });

    socket.on('transfer_host', (data) => {
        const room = rooms[data.roomId];
        if (room && room.host === socket.id) {
            room.host = data.targetId;
            room.users.forEach(u => {
                if (u.socketId === socket.id) u.isHost = false;
                if (u.socketId === data.targetId) { u.isHost = true; u.isCoHost = false; }
            });
            io.to(data.roomId).emit('update_users', room.users.filter(u => !u.isPendingRemoval));
            io.to(data.roomId).emit('chat_message', { system: true, text: `👑 The Host Crown was transferred!` });
        }
    });

    socket.on('toggle_cohost', (data) => {
        const room = rooms[data.roomId];
        if (room && room.host === socket.id) {
            const targetUser = room.users.find(u => u.socketId === data.targetId);
            if(targetUser) {
                targetUser.isCoHost = !targetUser.isCoHost;
                io.to(data.roomId).emit('update_users', room.users.filter(u => !u.isPendingRemoval));
                const msg = targetUser.isCoHost ? `⭐ ${targetUser.username} was granted Co-Host power!` : `🔒 ${targetUser.username}'s Co-Host power was revoked.`;
                io.to(data.roomId).emit('chat_message', { system: true, text: msg });
            }
        }
    });

    socket.on('change_video', (data) => {
        const room = rooms[data.roomId];
        const user = room?.users.find(u => u.socketId === socket.id);
        if (room && user && (user.isHost || user.isCoHost)) {
            room.currentVideo = { src: data.src, name: data.name, index: data.index, time: 0, state: 1 };
            io.to(data.roomId).emit('load_video', room.currentVideo);
        }
    });

    // BUG FIX #2 APPLIED HERE: Allow anyone to add to the playlist, and broadcast it to everyone.
    socket.on('update_playlist', (data) => {
        const room = rooms[data.roomId];
        if (room) {
            room.playlist = data.playlist;
            io.to(data.roomId).emit('sync_playlist', room.playlist);
        }
    });

    socket.on('play_video', (data) => {
        const room = rooms[data.roomId];
        const user = room?.users.find(u => u.socketId === socket.id);
        if (room && user && (user.isHost || user.isCoHost)) socket.to(data.roomId).emit('sync_play', data.time);
    });

    socket.on('pause_video', (data) => {
        const room = rooms[data.roomId];
        const user = room?.users.find(u => u.socketId === socket.id);
        if (room && user && (user.isHost || user.isCoHost)) socket.to(data.roomId).emit('sync_pause', data.time);
    });

    socket.on('broadcast_sync_data', (data) => {
        const room = rooms[data.roomId];
        if (room && room.host === socket.id) {
            if (room.currentVideo) {
                room.currentVideo.time = data.time;
                room.currentVideo.state = data.state;
            }
            socket.to(data.roomId).emit('host_send_sync', { time: data.time, state: data.state });
        }
    });

    // Chat broadcast logic 
    socket.on('chat_message', (data) => { 
        if (rooms[data.roomId]) {
            io.to(data.roomId).emit('chat_message', data); 
        }
    });
    
    socket.on('voice_join', (data) => { socket.to(data.roomId).emit('voice_user_joined', { socketId: socket.id }); });
    socket.on('webrtc_offer', (data) => { io.to(data.target).emit('webrtc_offer', { sender: socket.id, sdp: data.sdp }); });
    socket.on('webrtc_answer', (data) => { io.to(data.target).emit('webrtc_answer', { sender: socket.id, sdp: data.sdp }); });
    socket.on('webrtc_ice', (data) => { io.to(data.target).emit('webrtc_ice', { sender: socket.id, candidate: data.candidate }); });

    socket.on('disconnect', () => {
        for (const roomId in rooms) {
            const room = rooms[roomId];
            const userIndex = room.users.findIndex(u => u.socketId === socket.id);

            if (userIndex !== -1) {
                const user = room.users[userIndex];
                socket.to(roomId).emit('voice_user_left', { socketId: socket.id });

                user.isPendingRemoval = true;

                user.timeoutId = setTimeout(() => {
                    const currentRoom = rooms[roomId];
                    if (currentRoom) {
                        const freshInstance = currentRoom.users.find(u => u.userId === user.userId && !u.isPendingRemoval);

                        if (!freshInstance) {
                            currentRoom.users = currentRoom.users.filter(u => u.userId !== user.userId);
                            io.to(roomId).emit('chat_message', { system: true, text: `${user.username} left the party 👋` });

                            if (user.isHost && currentRoom.users.length > 0) {
                                currentRoom.host = currentRoom.users[0].socketId;
                                currentRoom.users[0].isHost = true;
                                currentRoom.users[0].isCoHost = false;
                                io.to(roomId).emit('chat_message', { system: true, text: `👑 ${currentRoom.users[0].username} is the new Room Host` });
                            }

                            io.to(roomId).emit('update_users', currentRoom.users.filter(u => !u.isPendingRemoval));
                        }

                        if (currentRoom.users.length === 0) {
                            delete rooms[roomId];
                        }
                    }
                    emitActiveRooms();
                }, 3000);

                io.to(roomId).emit('update_users', room.users.filter(u => !u.isPendingRemoval));
                break;
            }
        }
    });

    function emitActiveRooms() {
        const publicRooms = Object.values(rooms)
            .filter(r => !r.password)
            .map(r => ({ 
                id: r.id, 
                name: r.name, 
                users: r.users.filter(u => !u.isPendingRemoval).length 
            }));
        io.emit('active_rooms', publicRooms);
    }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => { console.log(`✅ SyncTube Server running on port ${PORT}`); });
