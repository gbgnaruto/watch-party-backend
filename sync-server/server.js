const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const https = require('https');
const httpLib = require('http');
const { spawn } = require('child_process');
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
// UPGRADE 1: Auto-Disk Cleanup (Prevents the server from crashing when full)
// Deletes converted streams older than 4 hours every 30 minutes.
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
// Downloader
// ---------------------------------------------------------------------------
const BLOCKED_HOSTS = ['youtube.com', 'youtu.be', 'vimeo.com', 'netflix.com', 'twitch.tv', 'dailymotion.com'];
function downloadToFile(url, destPath, redirects = 0) {
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
        return resolve(downloadToFile(res.headers.location, destPath, redirects + 1));
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('Failed to fetch URL: HTTP ' + res.statusCode)); }
      const fileStream = fs.createWriteStream(destPath);
      res.pipe(fileStream);
      fileStream.on('finish', () => fileStream.close(resolve));
      fileStream.on('error', reject);
    });
    req.on('error', reject);
    req.setTimeout(30000, () => req.destroy(new Error('Download timed out')));
  });
}

// ---------------------------------------------------------------------------
// Conversion pipeline
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

function buildFfmpegArgs(inputPath, outDir, audioStreams) {
  const args = ['-y', '-i', inputPath];
  args.push('-map', '0:v:0');
  audioStreams.forEach(a => args.push('-map', `0:${a.index}`));

  // UPGRADE 2: Zero-CPU Fast Copy (-c:v copy)
  // Instead of re-encoding the video, we just slice it instantly. Blazing fast.
  args.push('-c:v', 'copy');
  
  audioStreams.forEach((a, i) => args.push(`-c:a:${i}`, 'aac', `-b:a:${i}`, '192k', `-ac:${i}`, '2'));

  const streamMapParts = [];
  streamMapParts.push('v:0,name:source,agroup:aud');

  audioStreams.forEach((a, ai) => {
    const safeLang = (a.language || 'und').replace(/[^a-zA-Z0-9]/g, '');
    streamMapParts.push(`a:${ai},agroup:aud,language:${safeLang}${ai === 0 ? ',default:YES' : ''}`);
  });

  args.push(
    '-var_stream_map', streamMapParts.join(' '),
    '-f', 'hls', '-hls_time', '6', '-hls_list_size', '0', 
    
    // Added append_list so hls.js knows segments are continuously being added
    '-hls_flags', 'independent_segments+append_list', 
    
    '-master_pl_name', 'master.m3u8',
    '-hls_segment_filename', path.join(outDir, 'stream_%v_data%03d.ts'),
    path.join(outDir, 'stream_%v.m3u8')
  );
  return args;
}

function extractSubtitles(inputPath, outDir, subtitleStreams) {
  return Promise.all(subtitleStreams.map((s, i) => new Promise((resolve) => {
    const outFile = path.join(outDir, `sub_${i}.vtt`);
    const proc = spawn(ffmpegPath, ['-y', '-i', inputPath, '-map', `0:${s.index}`, outFile]);
    proc.on('close', () => resolve({ file: `sub_${i}.vtt`, language: s.tags?.language || 'und', title: s.tags?.title || `Subtitle ${i + 1}` }));
    proc.on('error', () => resolve(null));
  }))).then(list => list.filter(Boolean));
}

async function runConversionJob(jobId, inputPath) {
  const outDir = path.join(HLS_DIR, jobId);
  fs.mkdirSync(outDir, { recursive: true });

  try {
    jobs[jobId].status = 'probing';
    const probe = await ffprobeStreams(inputPath);
    const audioStreams = probe.streams.filter(s => s.codec_type === 'audio')
      .map(s => ({ index: s.index, language: s.tags?.language, title: s.tags?.title }));
    const subtitleStreams = probe.streams.filter(s => s.codec_type === 'subtitle' && s.codec_name !== 'hdmv_pgs_subtitle');
    if (audioStreams.length === 0) throw new Error('No audio streams found in source file');

    jobs[jobId].status = 'extracting_subtitles';
    jobs[jobId].subtitles = await extractSubtitles(inputPath, outDir, subtitleStreams);

    jobs[jobId].status = 'encoding';
    jobs[jobId].masterUrl = `/hls/${jobId}/master.m3u8`;

    await new Promise((resolve, reject) => {
      const proc = spawn(ffmpegPath, buildFfmpegArgs(inputPath, outDir, audioStreams));
      
      let stderrBuf = '';
      const MAX_BUF = 20000;
      let isLiveSignaled = false;

      // UPGRADE 3: The 30-Second Live Edge Watcher
      // Automatically signals the frontend to start playing once 5 segments (30 sec) exist
      const segWatcher = fs.watch(outDir, (event, filename) => {
        if (!isLiveSignaled && filename && filename.endsWith('.ts')) {
          const segs = fs.readdirSync(outDir).filter(f => f.endsWith('.ts')).length;
          // Trigger playback at 5 segments
          if (segs >= 5) {
            isLiveSignaled = true;
            jobs[jobId].status = 'done'; // Tricks the frontend into playing immediately
            console.log(`[Live Stream] Job ${jobId} buffered 30 seconds. Playing now!`);
          }
        }
      });

      proc.stderr.on('data', d => {
        stderrBuf = (stderrBuf + d.toString()).slice(-MAX_BUF);
        const m = d.toString().match(/time=(\d+):(\d+):(\d+\.\d+)/);
        if (m) jobs[jobId].lastTimeMark = `${m[1]}:${m[2]}:${m[3]}`;
      });

      const maxMs = parseInt(process.env.FFMPEG_TIMEOUT_MS, 10) || 25 * 60 * 1000;
      const timeoutHandle = setTimeout(() => { proc.kill('SIGKILL'); }, maxMs);

      proc.on('close', (code, signal) => {
        clearTimeout(timeoutHandle);
        segWatcher.close();
        
        if (code === 0) {
          // If the video was very short and finished before hitting 5 segments, mark it done now
          if (!isLiveSignaled) jobs[jobId].status = 'done';
          console.log(`[FFmpeg] Job ${jobId} finished completely.`);
          return resolve();
        }

        const lines = stderrBuf.split('\n').map(l => l.trim()).filter(Boolean);
        const errorLines = lines.filter(l => /error|invalid|failed|no such|could not|unsupported|unable to/i.test(l));
        const relevant = (errorLines.length ? errorLines : lines.slice(-8)).slice(-8).join('\n');

        if (!!signal || code > 128) return reject(new Error(`Conversion terminated (OOM Kill)`));
        reject(new Error(`ffmpeg exited with code ${code}\n${relevant}`));
      });
      proc.on('error', reject);
    });

  } catch (err) {
    jobs[jobId].status = 'error';
    jobs[jobId].error = err.message;
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
  jobs[jobId] = { status: 'queued' };
  res.json({ jobId });

  const ext = safeExtensionFromUrl(url);
  const destPath = path.join(UPLOAD_DIR, `${jobId}.${ext}`);
  try {
    jobs[jobId].status = 'downloading';
    await downloadToFile(url, destPath);
    runConversionJob(jobId, destPath);
  } catch (err) {
    jobs[jobId].status = 'error';
    jobs[jobId].error = err.message;
  }
});

app.get('/api/convert/status/:jobId', (req, res) => {
  const job = jobs[req.params.jobId];
  if (!job) return res.status(404).json({ error: 'Unknown job' });
  res.json(job);
});

// ---------------------------------------------------------------------------
// Room state & Sockets
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

  socket.on('send-reaction', (data) => {
    const room = roomsData[data.roomId];
    if (!room) return;
    const ALLOWED = ['❤️', '😂', '😮', '👏', '🔥', '👍'];
    if (!ALLOWED.includes(data.emoji)) return;
    const username = room.members[socket.id] || 'Guest';
    io.to(data.roomId).emit('reaction', { emoji: data.emoji, user: username });
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
