const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(cors());
app.use(express.json());

const publicDir = path.join(__dirname, 'public');
if (!fs.existsSync(publicDir)) {
    fs.mkdirSync(publicDir);
}
app.use('/public', express.static(publicDir));

const STREAM_MAX_AGE_MS = 4 * 60 * 60 * 1000; 
const CLEANUP_INTERVAL_MS = 30 * 60 * 1000;    

function cleanOldStreams() {
    try {
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
            } catch(e) {}
        }
    } catch(e) {}
}

cleanOldStreams();
setInterval(cleanOldStreams, CLEANUP_INTERVAL_MS);

const jobs = {};
const LIVE_START_SEGMENTS = 3; // Increased buffer threshold to prevent initial starvation & buffering

// --- Cancel Conversion Endpoint ---
app.post('/api/cancel-conversion', (req, res) => {
    const { jobId } = req.body;
    if (jobId && jobs[jobId]) {
        const job = jobs[jobId];
        if (job.process && typeof job.process.kill === 'function') {
            try { job.process.kill('SIGKILL'); } catch(e) {}
        }
        try {
            if (fs.existsSync(job.streamDir)) {
                fs.rmSync(job.streamDir, { recursive: true, force: true });
            }
        } catch(e) {}
        
        jobs[jobId].status = 'cancelled';
        delete jobs[jobId];
        return res.json({ status: 'ok', message: 'Conversion cancelled successfully' });
    }
    res.status(404).json({ error: 'Job not found' });
});

app.post('/api/convert', (req, res) => {
    const { videoUrl } = req.body;
    if (!videoUrl || typeof videoUrl !== 'string') {
        return res.status(400).json({ error: 'Valid Video URL required' });
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
        segments: 0,
        process: null
    };

    res.json({ status: 'queued', jobId });

    setTimeout(() => {
        let videoTitle = 'Direct Link';
        let videoDuration = 0;
        let audioLangs = [];
        const { execSync } = require('child_process');

        try {
            const probeJson = execSync(
                `ffprobe -v quiet -print_format json -show_format -show_streams "${videoUrl.replace(/"/g,'\\"')}"`,
                { timeout: 10000 }
            ).toString();
            const probe = JSON.parse(probeJson);

            videoTitle = probe.format?.tags?.title || probe.format?.tags?.TITLE || 'Direct Link';
            videoDuration = parseFloat(probe.format?.duration || 0);

            const audioStreams = (probe.streams || []).filter(s => s.codec_type === 'audio');
            audioLangs = audioStreams.map((s, i) => {
                const lang = s.tags?.language || s.tags?.LANGUAGE || '';
                const title = s.tags?.title || s.tags?.TITLE || '';
                const langMap = { eng:'English', hin:'Hindi', jpn:'Japanese', tam:'Tamil',
                                  tel:'Telugu', fra:'French', spa:'Spanish', kor:'Korean',
                                  ara:'Arabic', por:'Portuguese', deu:'German', zho:'Chinese' };
                const label = title || langMap[lang] || (lang ? lang.toUpperCase() : `Track ${i+1}`);
                return { index: i, lang, label };
            });
        } catch(e) {}

        if (jobs[jobId]) {
            jobs[jobId].title = videoTitle;
            jobs[jobId].duration = videoDuration;
            jobs[jobId].audioLangs = audioLangs;
        }

        // Upgraded FFmpeg arguments: re-encode with ultrafast libx264 & strict 2s keyframes to eliminate buffering stalls
        const args = [
            '-y',
            '-user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            '-reconnect', '1', '-reconnect_streamed', '1', '-reconnect_delay_max', '2',
            '-i', videoUrl,
            '-map', '0:v:0', '-map', '0:a:0?',
            '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23',
            '-g', '48', '-keyint_min', '48', '-sc_threshold', '0',
            '-force_key_frames', 'expr:gte(t,n_forced*2)',
            '-c:a', 'aac', '-ac', '2', '-b:a', '128k',
            '-f', 'hls', '-hls_time', '2', '-hls_list_size', '0',
            '-hls_flags', 'independent_segments+omit_endlist+delete_segments',
            '-hls_segment_filename', path.join(streamDir, 'seg_%03d.ts'),
            outputPath
        ];

        const proc = spawn('ffmpeg', args);
        if (jobs[jobId]) jobs[jobId].process = proc;

        const segWatcher = fs.watch(streamDir, (event, filename) => {
            if (filename && filename.endsWith('.ts')) {
                try {
                    const segs = fs.readdirSync(streamDir).filter(f => f.endsWith('.ts')).length;
                    if (jobs[jobId]) {
                        jobs[jobId].segments = segs;
                        if (jobs[jobId].status === 'pending' && segs >= LIVE_START_SEGMENTS) {
                            jobs[jobId].status = 'live';
                        }
                    }
                } catch(e) {}
            }
        });

        proc.on('close', code => {
            try { segWatcher.close(); } catch(e) {}
            if (jobs[jobId]) {
                if (code === 0) jobs[jobId].status = 'done';
                else if (jobs[jobId].status === 'pending') jobs[jobId].status = 'error';
            }
        });

        proc.on('error', err => {
            try { segWatcher.close(); } catch(e) {}
            if (jobs[jobId] && jobs[jobId].status === 'pending') {
                jobs[jobId].status = 'error';
                jobs[jobId].error = err.message;
            }
        });
    }, 50);
});

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
            subtitles: job.subtitles || []
        });
    }
    if (job.status === 'error') {
        return res.json({ status: 'Error', error: job.error });
    }
    res.json({
        status: 'pending',
        segments: job.segments || 0,
        title: job.title || '',
        duration: job.duration || 0
    });
});

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*", methods: ["GET", "POST"] } });
const rooms = {};

io.on('connection', (socket) => {
    socket.on('join_room', (data, callback) => {
        const { roomId, roomName, password, username, userId, photo } = data;
        if (!rooms[roomId]) {
            rooms[roomId] = { id: roomId, name: roomName, password: password || null, host: socket.id, users: [], playlist: [], currentVideo: null };
        }
        const room = rooms[roomId];
        room.users.push({ socketId: socket.id, userId, username, photo, isHost: room.users.length === 0, isPendingRemoval: false });
        socket.join(roomId);
        callback({ success: true });
        io.to(roomId).emit('update_users', room.users);
    });

    socket.on('disconnect', () => {});
});

server.listen(process.env.PORT || 3000, () => { console.log('Server running'); });
