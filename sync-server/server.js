const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const https = require('https');
const httpLib = require('http');
const { spawn, execSync } = require('child_process');
const ffmpegPath = require('ffmpeg-static');
const ffprobePath = require('ffprobe-static').path;

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

const UPLOAD_DIR = path.join(__dirname, 'uploads');
const HLS_DIR = path.join(__dirname, 'hls_output');
[UPLOAD_DIR, HLS_DIR].forEach(d => { if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true }); });

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());
app.use('/hls', express.static(HLS_DIR));

// ---------------------------------------------------------------------------
// Disk Cleanup: Purge streams older than 4 hours every 30 minutes
// ---------------------------------------------------------------------------
const STREAM_MAX_AGE_MS = 4 * 60 * 60 * 1000; 
const CLEANUP_INTERVAL_MS = 30 * 60 * 1000;    

function cleanOldStreams() {
    try {
        const entries = fs.readdirSync(HLS_DIR);
        let deleted = 0;
        for (const name of entries) {
            const dir = path.join(HLS_DIR, name);
            try {
                const stat = fs.statSync(dir);
                if (!stat.isDirectory()) continue;
                if (Date.now() - stat.mtimeMs < STREAM_MAX_AGE_MS) continue;

                fs.rmSync(dir, { recursive: true, force: true });
                deleted++;
            } catch(e) {}
        }
        if (deleted > 0) console.log(`[Cleanup] Removed ${deleted} old stream folders.`);
    } catch(e) {
        console.error('[Cleanup] Error:', e.message);
    }
}
cleanOldStreams();
setInterval(cleanOldStreams, CLEANUP_INTERVAL_MS);

// ---------------------------------------------------------------------------
// File Downloader
// ---------------------------------------------------------------------------
const BLOCKED_HOSTS = ['youtube.com', 'youtu.be', 'vimeo.com', 'netflix.com', 'twitch.tv', 'dailymotion.com'];
function downloadToFile(url, destPath, redirects = 0, jobRef = null) {
  return new Promise((resolve, reject) => {
    let host;
    try { host = new URL(url).hostname.replace(/^www\./, ''); } catch (e) { return reject(new Error('Invalid URL')); }
    if (BLOCKED_HOSTS.some(h => host.endsWith(h))) {
      return reject(new Error('Direct downloads from streaming platforms are not supported.'));
    }
    if (redirects > 5) return reject(new Error('Too many redirects'));

    const lib = url.startsWith('https') ? https : httpLib;
    const req = lib.get(url, { headers: { 'User-Agent': 'SyncTube/1.0' } }, (res) => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        return resolve(downloadToFile(res.headers.location, destPath, redirects + 1, jobRef));
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('Failed to fetch URL: HTTP ' + res.statusCode)); }
      const fileStream = fs.createWriteStream(destPath);
      if (jobRef) jobRef.fileStream = fileStream;
      res.pipe(fileStream);
      fileStream.on('finish', () => fileStream.close(resolve));
      fileStream.on('error', reject);
    });
    req.on('error', reject);
    req.setTimeout(30000, () => req.destroy(new Error('Download timed out')));
  });
}

// ---------------------------------------------------------------------------
// Conversion Pipeline (Multi-Audio, Codec Safe & Instant Playback)
// ---------------------------------------------------------------------------
const jobs = {};

function ffprobeStreams(filePath) {
  return new Promise((resolve, reject) => {
    const args = ['-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', filePath];
    const proc = spawn(ffprobePath, args);
    let out = '', err = '';
    proc.stdout.on('data', d => out += d);
    proc.stderr.on('data', d => err += d);
    proc.on('close', code => {
      if (code !== 0) return reject(new Error(err || 'ffprobe failed'));
      try { resolve(JSON.parse(out)); } catch (e) { reject(e); }
    });
  });
}

function extractSubtitles(inputPath, outDir, subtitleStreams, jobRef) {
  return Promise.all(subtitleStreams.map((s, i) => new Promise((resolve) => {
    const outFile = path.join(outDir, `sub_${i}.vtt`);
    const proc = spawn(ffmpegPath, ['-y', '-i', inputPath, '-map', `0:${s.index}`, outFile]);
    if (jobRef) jobRef.processes.push(proc);
    proc.on('close', () => resolve({ file: `sub_${i}.vtt`, language: s.tags?.language || 'und', title: s.tags?.title || `Subtitle ${i + 1}` }));
    proc.on('error', () => resolve(null));
  }))).then(list => list.filter(Boolean));
}

function buildMasterPlaylist(outDir, audioLangs) {
  try {
    let master = '#EXTM3U\n#EXT-X-VERSION:3\n\n';
    const numAudio = audioLangs.length || 1;
    for (let i = 0; i < numAudio; i++) {
      const label = audioLangs[i]?.title || audioLangs[i]?.language?.toUpperCase() || `Track ${i + 1}`;
      const lang  = audioLangs[i]?.language || 'und';
      const def   = i === 0 ? 'YES' : 'NO';
      const uri   = i === 0 ? 'stream_0.m3u8' : `stream_${i}.m3u8`;
      master += `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="${label}",LANGUAGE="${lang}",DEFAULT=${def},AUTOSELECT=${def},URI="${uri}"\n`;
    }
    master += `\n#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO="audio"\nstream_0.m3u8\n`;
    fs.writeFileSync(path.join(outDir, 'master.m3u8'), master);
  } catch (e) {
    console.error('[Master Playlist] Failed to write:', e.message);
  }
}

async function runConversionJob(jobId, inputPath) {
  const outDir = path.join(HLS_DIR, jobId);
  fs.mkdirSync(outDir, { recursive: true });
  const job = jobs[jobId];
  if (!job) return;

  try {
    job.status = 'probing';
    const probe = await ffprobeStreams(inputPath);
    
    // Extract real video title from metadata or fallback to filename
    const detectedTitle = probe.format?.tags?.title || probe.format?.tags?.TITLE || path.basename(inputPath, path.extname(inputPath));
    job.title = detectedTitle;

    const videoStream = (probe.streams || []).find(s => s.codec_type === 'video');
    const videoCodec = videoStream?.codec_name || 'h264';

    const audioStreams = probe.streams.filter(s => s.codec_type === 'audio')
      .map(s => ({ index: s.index, language: s.tags?.language, title: s.tags?.title }));
    const subtitleStreams = probe.streams.filter(s => s.codec_type === 'subtitle' && s.codec_name !== 'hdmv_pgs_subtitle');
    
    if (audioStreams.length === 0) throw new Error('No audio streams found in source file');

    job.status = 'extracting_subtitles';
    job.subtitles = await extractSubtitles(inputPath, outDir, subtitleStreams, job);

    job.status = 'encoding';
    job.masterUrl = `/hls/${jobId}/master.m3u8`;

    buildMasterPlaylist(outDir, audioStreams);

    // Spawn extra audio-only streams in parallel for tracks 1..N
    for (let i = 1; i < audioStreams.length; i++) {
      const aArgs = [
        '-y', '-i', inputPath,
        '-map', `0:${audioStreams[i].index}`, '-vn',
        '-c:a', 'aac', '-ac', '2', '-b:a', '128k',
        '-f', 'hls', '-hls_time', '4', '-hls_list_size', '0',
        '-hls_flags', 'append_list',
        '-hls_segment_filename', path.join(outDir, `seg_${i}_%03d.ts`),
        path.join(outDir, `stream_${i}.m3u8`)
      ];
      const aProc = spawn(ffmpegPath, aArgs);
      job.processes.push(aProc);
      aProc.stderr.on('data', () => {}); 
    }

    const isCodecSupported = ['h264', 'avc1'].includes(videoCodec.toLowerCase());
    const videoArgs = isCodecSupported 
      ? ['-c:v', 'copy'] 
      : ['-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '22', '-pix_fmt', 'yuv420p'];

    const mainArgs = [
      '-y', '-i', inputPath,
      '-map', '0:v:0', '-map', `0:${audioStreams[0].index}`,
      ...videoArgs,
      '-c:a', 'aac', '-ac', '2', '-b:a', '192k',
      '-max_muxing_queue_size', '9999',
      '-f', 'hls', '-hls_time', '4', '-hls_list_size', '0',
      '-hls_flags', 'append_list',
      '-hls_segment_filename', path.join(outDir, 'seg_0_%03d.ts'),
      path.join(outDir, 'stream_0.m3u8')
    ];

    await new Promise((resolve, reject) => {
      const proc = spawn(ffmpegPath, mainArgs);
      job.processes.push(proc);
      let stderrBuf = '';
      let isLiveSignaled = false;

      // TRIGGER PLAYBACK AT 2 SEGMENTS (~12 seconds) FOR INSTANT START!
      const segWatcher = fs.watch(outDir, (event, filename) => {
        if (!isLiveSignaled && filename && filename.endsWith('.ts')) {
          try {
            const segs = fs.readdirSync(outDir).filter(f => f.endsWith('.ts')).length;
            job.segments = segs;
            if (segs >= 2) {
              isLiveSignaled = true;
              job.status = 'done'; 
              console.log(`[Instant Stream] Job ${jobId} buffered 2 segments. Starting playback instantly!`);
            }
          } catch(e) {}
        }
      });

      proc.stderr.on('data', d => {
        stderrBuf = (stderrBuf + d.toString()).slice(-20000);
        const m = d.toString().match(/time=(\d+):(\d+):(\d+\.\d+)/);
        if (m) job.lastTimeMark = `${m[1]}:${m[2]}:${m[3]}`;
      });

      const maxMs = parseInt(process.env.FFMPEG_TIMEOUT_MS, 10) || 25 * 60 * 1000;
      const timeoutHandle = setTimeout(() => { proc.kill('SIGKILL'); }, maxMs);

      proc.on('close', (code, signal) => {
        clearTimeout(timeoutHandle);
        try { segWatcher.close(); } catch(e) {}
        if (code === 0) {
          if (!isLiveSignaled) job.status = 'done';
          return resolve();
        }
        if (!!signal || code > 128) return reject(new Error('Conversion terminated (OOM Kill)'));
        reject(new Error(`ffmpeg exited with code ${code}`));
      });
      proc.on('error', reject);
    });

  } catch (err) {
    if (jobs[jobId]) {
      jobs[jobId].status = 'error';
      jobs[jobId].error = err.message;
    }
    fs.rm(outDir, { recursive: true, force: true }, () => {});
  } finally {
    fs.rm(inputPath, { force: true }, () => {});
  }
}

function safeExtensionFromUrl(url) {
  const FALLBACK = 'mp4';
  try {
    const pathname = new URL(url).pathname;
    const match = pathname.match(/\.([a-zA-Z0-9]{2,5})$/);
    return match ? match[1].toLowerCase() : FALLBACK;
  } catch (e) {
    return FALLBACK;
  }
}

app.post('/api/convert-from-url', async (req, res) => {
  const { url } = req.body || {};
  if (!url) return res.status(400).json({ error: 'Missing url' });

  const jobId = crypto.randomBytes(6).toString('hex');
  jobs[jobId] = { status: 'queued', processes: [], fileStream: null, title: 'Converted Video' };
  res.json({ jobId });

  const ext = safeExtensionFromUrl(url);
  const destPath = path.join(UPLOAD_DIR, `${jobId}.${ext}`);
  try {
    jobs[jobId].status = 'downloading';
    await downloadToFile(url, destPath, 0, jobs[jobId]);
    runConversionJob(jobId, destPath);
  } catch (err) {
    if (jobs[jobId]) {
      jobs[jobId].status = 'error';
      jobs[jobId].error = err.message;
    }
  }
});

// ---------------------------------------------------------------------------
// Cancel Conversion Endpoint
// ---------------------------------------------------------------------------
app.post('/api/convert/cancel', (req, res) => {
  const { jobId } = req.body;
  if (!jobId || !jobs[jobId]) {
    return res.status(404).json({ error: 'Job not found' });
  }

  const job = jobs[jobId];
  try {
    if (job.fileStream && typeof job.fileStream.destroy === 'function') {
      job.fileStream.destroy();
    }
    if (job.processes && Array.isArray(job.processes)) {
      job.processes.forEach(p => {
        try { p.kill('SIGKILL'); } catch(e) {}
      });
    }
    const outDir = path.join(HLS_DIR, jobId);
    if (fs.existsSync(outDir)) {
      fs.rmSync(outDir, { recursive: true, force: true });
    }
    const files = fs.readdirSync(UPLOAD_DIR);
    files.forEach(f => {
      if (f.startsWith(jobId)) {
        try { fs.unlinkSync(path.join(UPLOAD_DIR, f)); } catch(e) {}
      }
    });
  } catch(e) {
    console.error('[Cancel] Error cleaning up job:', e.message);
  }

  delete jobs[jobId];
  res.json({ status: 'ok', message: 'Conversion cancelled successfully' });
});

app.get('/api/convert/status/:jobId', (req, res) => {
  const job = jobs[req.params.jobId];
  if (!job) return res.status(404).json({ error: 'Unknown job' });
  res.json({
    status: job.status,
    masterUrl: job.masterUrl || '',
    title: job.title || 'Converted Video',
    subtitles: job.subtitles || [],
    error: job.error || null,
    lastTimeMark: job.lastTimeMark || null
  });
});

// ---------------------------------------------------------------------------
// Room State & Socket Handling
// ---------------------------------------------------------------------------
const roomsData = {};

function broadcastActiveRooms() {
  const activeRooms = Object.keys(roomsData)
    .filter(roomId => roomsData[roomId].count > 0)
    .map(roomId => ({ roomId, count: roomsData[roomId].count }));
  io.emit('active-rooms', activeRooms);
}

function currentPlaybackTime(room) {
  if (!room.playbackState || room.playbackState !== 'playing') return room.currentVideoTime || 0;
  const elapsed = (Date.now() - (room.lastUpdatedAt || Date.now())) / 1000;
  return (room.currentVideoTime || 0) + elapsed;
}

async function broadcastRoomState(roomId) {
  const room = roomsData[roomId];
  if (!room) return;
  const sockets = await io.in(roomId).fetchSockets();
  sockets.forEach(s => {
    s.emit('room-info', { count: room.count, queue: room.queue, isAdmin: room.admin === s.id });
  });
  const memberList = Object.entries(room.members).map(([id, username]) => ({ id, username, isHost: id === room.admin }));
  io.to(roomId).emit('members-update', memberList);
}

io.on('connection', (socket) => {
  let currentRoom = null;
  let currentUser = null;

  broadcastActiveRooms();

  socket.on('join-room', (data) => {
    const roomId = data.roomId;
    const username = (data.username || "Guest").slice(0, 20);

    currentRoom = roomId;
    currentUser = username;
    socket.join(roomId);

    if (!roomsData[roomId]) {
      roomsData[roomId] = {
        count: 0, admin: socket.id, members: {}, queue: [], voiceUsers: new Set(),
        sourceType: 'none', currentVideoId: null, currentRawUrl: null, currentRawTitle: null,
        currentRawSubtitles: null, autoplayNext: true,
        currentVideoTime: 0, playbackState: 'paused', lastUpdatedAt: Date.now()
      };
    }
    const room = roomsData[roomId];
    room.count++;
    room.members[socket.id] = username;
    
    socket.emit('room-info', { count: room.count, queue: room.queue, isAdmin: room.admin === socket.id, autoplayNext: room.autoplayNext });

    broadcastActiveRooms();
    broadcastRoomState(roomId);

    socket.emit('initial-sync', {
      sourceType: room.sourceType, videoId: room.currentVideoId, rawUrl: room.currentRawUrl,
      title: room.currentRawTitle, time: currentPlaybackTime(room), playbackState: room.playbackState,
      subtitles: room.currentRawSubtitles
    });

    io.to(roomId).emit('chat-message', { type: 'system', text: `${username.toUpperCase()} JOINED THE ROOM`, timestamp: Date.now() });
  });

  socket.on('video-command', (data) => {
    const room = roomsData[data.roomId];
    if (!room || room.admin !== socket.id) return;

    if (data.type === 'play') { room.playbackState = 'playing'; room.lastUpdatedAt = Date.now(); }
    if (data.type === 'pause') { room.currentVideoTime = currentPlaybackTime(room); room.playbackState = 'paused'; room.lastUpdatedAt = Date.now(); }
    if (data.type === 'seek') { room.currentVideoTime = data.time; room.lastUpdatedAt = Date.now(); room.playbackState = 'playing'; }
    if (data.type === 'change') {
      room.sourceType = 'youtube'; room.currentVideoId = data.videoId; room.currentRawUrl = null; room.currentRawTitle = null;
      room.currentVideoTime = 0; room.playbackState = 'playing'; room.lastUpdatedAt = Date.now();
    }
    if (data.type === 'change-raw') {
      room.sourceType = 'raw'; room.currentRawUrl = data.url; room.currentRawTitle = data.title || null; room.currentVideoId = null;
      room.currentRawSubtitles = data.subtitles || null;
      room.currentVideoTime = 0; room.playbackState = 'playing'; room.lastUpdatedAt = Date.now();
    }
    socket.to(data.roomId).emit('sync-video', data);
  });

  socket.on('set-autoplay-next', (data) => {
    const room = roomsData[data.roomId];
    if (!room || room.admin !== socket.id) return;
    room.autoplayNext = !!data.enabled;
    io.to(data.roomId).emit('autoplay-next-changed', room.autoplayNext);
  });

  socket.on('add-to-queue', (data) => {
    const room = roomsData[data.roomId];
    if (!room) return;
    room.queue.push(data.video);
    io.to(data.roomId).emit('queue-update', room.queue);
  });

  socket.on('pop-queue', (roomId) => {
    const room = roomsData[roomId];
    if (room && room.admin === socket.id && room.queue.length > 0) {
      room.queue.shift();
      io.to(roomId).emit('queue-update', room.queue);
    }
  });

  socket.on('clear-queue', (roomId) => {
    const room = roomsData[roomId];
    if (room && room.admin === socket.id) {
      room.queue = [];
      io.to(roomId).emit('queue-update', room.queue);
    }
  });

  socket.on('reorder-queue', (data) => {
    const room = roomsData[data.roomId];
    if (!room || room.admin !== socket.id) return;
    const { from, to } = data;
    const item = room.queue.splice(from, 1)[0];
    room.queue.splice(to, 0, item);
    io.to(data.roomId).emit('queue-update', room.queue);
  });

  socket.on('chat-message', (data) => {
    io.to(data.roomId).emit('chat-message', { type: 'user', user: data.user, text: data.text, timestamp: Date.now() });
  });

  socket.on('typing', (data) => {
    socket.to(data.roomId).emit('typing', { user: data.user, isTyping: data.isTyping });
  });

  socket.on('transfer-host', (data) => {
    const room = roomsData[data.roomId];
    if (!room || room.admin !== socket.id) return;
    if (!room.members[data.targetId]) return;
    room.admin = data.targetId;
    broadcastRoomState(data.roomId);
    io.to(data.roomId).emit('chat-message', { type: 'system', text: `${room.members[data.targetId].toUpperCase()} IS NOW HOST`, timestamp: Date.now() });
  });

  socket.on('voice-join', (data) => {
    if (roomsData[data.roomId]) {
      roomsData[data.roomId].voiceUsers.add(data.user);
      io.to(data.roomId).emit('voice-participants', Array.from(roomsData[data.roomId].voiceUsers));
    }
  });

  socket.on('voice-leave', (data) => {
    if (roomsData[data.roomId]) {
      roomsData[data.roomId].voiceUsers.delete(data.user);
      io.to(data.roomId).emit('voice-participants', Array.from(roomsData[data.roomId].voiceUsers));
    }
  });

  socket.on('voice-speaking', (data) => {
    socket.to(data.roomId).emit('voice-speaking', { user: data.user, speaking: data.speaking });
  });

  socket.on('voice-chunk', (data) => {
    socket.to(data.roomId).emit('voice-chunk', { user: data.user, data: data.data });
  });

  socket.on('disconnect', () => {
    const room = roomsData[currentRoom];
    if (!room) return;

    room.count--;
    delete room.members[socket.id];

    if (currentUser && room.voiceUsers.has(currentUser)) {
      room.voiceUsers.delete(currentUser);
      io.to(currentRoom).emit('voice-participants', Array.from(room.voiceUsers));
    }
    if (currentUser) {
      io.to(currentRoom).emit('chat-message', { type: 'system', text: `${currentUser.toUpperCase()} LEFT THE ROOM`, timestamp: Date.now() });
    }
    if (room.count > 0 && room.admin === socket.id) {
      const remainingIds = Object.keys(room.members);
      if (remainingIds.length > 0) room.admin = remainingIds[0];
    }
    if (room.count <= 0) delete roomsData[currentRoom];
    else broadcastRoomState(currentRoom);
    broadcastActiveRooms();
  });
});

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`SyncTube Pro Server running on port ${PORT}`);
});
