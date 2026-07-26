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
// Disk Cleanup: Purge streams and old jobs older than 4 hours every 30 minutes
// ---------------------------------------------------------------------------
const STREAM_MAX_AGE_MS = 4 * 60 * 60 * 1000; 
const CLEANUP_INTERVAL_MS = 30 * 60 * 1000;    

function cleanOldStreams() {
    try {
        const protectedJobIds = new Set();
        Object.values(roomsData).forEach(room => {
            const m = room.currentRawUrl && room.currentRawUrl.match(/\/hls\/([a-f0-9]+)\//);
            if (m) protectedJobIds.add(m[1]);
        });

        const entries = fs.readdirSync(HLS_DIR);
        let deleted = 0;
        for (const name of entries) {
            if (protectedJobIds.has(name)) continue;
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

        // Clean up expired jobs from memory
        const now = Date.now();
        for (const [jobId, job] of Object.entries(jobs)) {
            if (job.completedAt && now - job.completedAt > STREAM_MAX_AGE_MS) {
                delete jobs[jobId];
            }
        }
    } catch(e) {
        console.error('[Cleanup] Error:', e.message);
    }
}

// ---------------------------------------------------------------------------
// File Downloader (with robust absolute/relative redirect resolution)
// ---------------------------------------------------------------------------
const BLOCKED_HOSTS = ['youtube.com', 'youtu.be', 'vimeo.com', 'netflix.com', 'twitch.tv', 'dailymotion.com'];
function downloadToFile(url, destPath, redirects = 0) {
  return new Promise((resolve, reject) => {
    let parsedUrl;
    try { parsedUrl = new URL(url); } catch (e) { return reject(new Error('Invalid URL')); }
    
    const host = parsedUrl.hostname.replace(/^www\./, '');
    if (BLOCKED_HOSTS.some(h => host.endsWith(h))) {
      return reject(new Error('Direct downloads from streaming platforms are not supported.'));
    }
    if (redirects > 5) return reject(new Error('Too many redirects'));

    const lib = parsedUrl.protocol === 'https:' ? https : httpLib;
    const req = lib.get(url, { headers: { 'User-Agent': 'SyncTube/1.0' } }, (res) => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        res.resume();
        let nextUrl;
        try {
          nextUrl = new URL(res.headers.location, url).href;
        } catch (e) {
          return reject(new Error('Invalid redirect location URL'));
        }
        return resolve(downloadToFile(nextUrl, destPath, redirects + 1));
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
// Conversion Pipeline
// ---------------------------------------------------------------------------
const jobs = {};

const LOUDNORM_I = process.env.LOUDNORM_I || '-16';
const LOUDNORM_TP = process.env.LOUDNORM_TP || '-1.5';
const LOUDNORM_LRA = process.env.LOUDNORM_LRA || '11';
const LOUDNORM_FILTER = `loudnorm=I=${LOUDNORM_I}:TP=${LOUDNORM_TP}:LRA=${LOUDNORM_LRA}`;

const SILENCE_NOISE_DB = process.env.SILENCE_NOISE_DB || '-30dB';
const SILENCE_MIN_DURATION = process.env.SILENCE_MIN_DURATION || '0.5';

const ABR_FALLBACK_HEIGHT = 480;
const ABR_FALLBACK_MIN_SOURCE_HEIGHT = 540;

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

function extractSubtitles(inputPath, outDir, subtitleStreams) {
  return Promise.all(subtitleStreams.map((s, i) => new Promise((resolve) => {
    const outFile = path.join(outDir, `sub_${i}.vtt`);
    const proc = spawn(ffmpegPath, ['-y', '-i', inputPath, '-map', `0:${s.index}`, outFile]);
    proc.on('close', () => resolve({ file: `sub_${i}.vtt`, language: s.tags?.language || 'und', title: s.tags?.title || `Subtitle ${i + 1}` }));
    proc.on('error', () => resolve(null));
  }))).then(list => list.filter(Boolean));
}

function detectSilence(inputPath) {
  return new Promise((resolve) => {
    const args = ['-i', inputPath, '-af', `silencedetect=noise=${SILENCE_NOISE_DB}:d=${SILENCE_MIN_DURATION}`, '-f', 'null', '-'];
    const proc = spawn(ffmpegPath, args);
    let buf = '';
    proc.stderr.on('data', d => { buf += d.toString(); });
    const finish = () => {
      const starts = [...buf.matchAll(/silence_start:\s*([\d.]+)/g)].map(m => parseFloat(m[1]));
      const ends = [...buf.matchAll(/silence_end:\s*([\d.]+)/g)].map(m => parseFloat(m[1]));
      const silences = [];
      for (let i = 0; i < Math.min(starts.length, ends.length); i++) silences.push({ start: starts[i], end: ends[i] });
      resolve(silences);
    };
    proc.on('close', finish);
    proc.on('error', () => resolve([]));
  });
}

function writePlaceholderPlaylist(outDir, filename) {
  const placeholder = '#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n';
  try { fs.writeFileSync(path.join(outDir, filename), placeholder); } catch (e) {}
}

function buildMasterPlaylist(outDir, audioTracks, videoRenditions) {
  try {
    let master = '#EXTM3U\n#EXT-X-VERSION:3\n\n';
    audioTracks.forEach((a, i) => {
      const label = a.title || (a.language ? a.language.toUpperCase() : `Track ${i + 1}`);
      const lang = a.language || 'und';
      const def = i === 0 ? 'YES' : 'NO';
      master += `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="${label}",LANGUAGE="${lang}",DEFAULT=${def},AUTOSELECT=${def},URI="audio_${i}.m3u8"\n`;
    });
    master += '\n';
    videoRenditions.forEach(v => {
      master += `#EXT-X-STREAM-INF:BANDWIDTH=${v.bandwidth},AUDIO="audio"${v.resolution ? ',RESOLUTION=' + v.resolution : ''}\n${v.uri}\n`;
    });
    fs.writeFileSync(path.join(outDir, 'master.m3u8'), master);
  } catch (e) {
    console.error('[Master Playlist] Failed to write:', e.message);
  }
}

function runAudioTrackPass(inputPath, outDir, streamIndex, trackLabel) {
  const args = [
    '-y', '-i', inputPath,
    '-map', `0:${streamIndex}`, '-vn',
    '-af', LOUDNORM_FILTER,
    '-c:a', 'aac', '-ac', '2', '-b:a', '128k',
    '-f', 'hls', '-hls_time', '6', '-hls_list_size', '0',
    '-hls_flags', 'append_list',
    '-hls_segment_filename', path.join(outDir, `segA_${trackLabel}_%03d.ts`),
    path.join(outDir, `audio_${trackLabel}.m3u8`)
  ];
  return new Promise((resolve) => {
    const proc = spawn(ffmpegPath, args);
    let errTail = '';
    proc.stderr.on('data', d => { errTail = (errTail + d.toString()).slice(-2000); });
    proc.on('close', code => {
      if (code !== 0) console.error(`[Audio track ${trackLabel}] ffmpeg exited ${code}:`, errTail.slice(-400));
      resolve();
    });
    proc.on('error', (e) => { console.error(`[Audio track ${trackLabel}] spawn error:`, e.message); resolve(); });
  });
}

function runVideoRenditionPass(jobId, outDir, inputPath, opts) {
  return new Promise((resolve, reject) => {
    const args = [
      '-y', '-i', inputPath,
      '-map', '0:v:0',
      ...opts.videoArgs,
      '-an',
      '-f', 'hls', '-hls_time', '6', '-hls_list_size', '0',
      '-hls_flags', 'append_list',
      '-hls_segment_filename', path.join(outDir, `${opts.segPrefix}_%03d.ts`),
      path.join(outDir, opts.playlistName)
    ];
    const proc = spawn(ffmpegPath, args);
    let stderrBuf = '';
    let liveSignaled = false;

    const checkBuffered = () => {
      if (liveSignaled || !opts.onBuffered) return;
      let segs = 0;
      try { segs = fs.readdirSync(outDir).filter(f => f.startsWith(opts.segPrefix) && f.endsWith('.ts')).length; } catch (e) {}
      if (segs >= 5) { liveSignaled = true; opts.onBuffered(); }
    };
    let segWatcher = null;
    if (opts.onBuffered) {
      try { segWatcher = fs.watch(outDir, () => checkBuffered()); } catch (e) {}
    }
    const pollHandle = opts.onBuffered ? setInterval(checkBuffered, 2000) : null;

    proc.stderr.on('data', d => {
      stderrBuf = (stderrBuf + d.toString()).slice(-20000);
      if (opts.isPrimary && jobs[jobId]) {
        const m = d.toString().match(/time=(\d+):(\d+):(\d+\.\d+)/);
        if (m) jobs[jobId].lastTimeMark = `${m[1]}:${m[2]}:${m[3]}`;
      }
    });

    const maxMs = parseInt(process.env.FFMPEG_TIMEOUT_MS, 10) || 25 * 60 * 1000;
    const timeoutHandle = setTimeout(() => proc.kill('SIGKILL'), maxMs);

    proc.on('close', (code, signal) => {
      clearTimeout(timeoutHandle);
      if (pollHandle) clearInterval(pollHandle);
      if (segWatcher) segWatcher.close();

      if (code === 0) return resolve();

      if (!opts.isPrimary) {
        console.error(`[${opts.playlistName}] ffmpeg exited ${code}${signal ? ' (signal ' + signal + ')' : ''} — fallback rendition unavailable, source rendition unaffected.`);
        return resolve();
      }

      const lines = stderrBuf.split('\n').map(l => l.trim()).filter(Boolean);
      const errorLines = lines.filter(l => /error|invalid|failed|no such|could not|unsupported|unable to/i.test(l));
      const relevant = (errorLines.length ? errorLines : lines.slice(-8)).slice(-8).join('\n');
      const looksLikeSystemKill = !!signal || code > 128;
      if (looksLikeSystemKill) {
        return reject(new Error(
          `Conversion was terminated unexpectedly (${signal ? 'signal ' + signal : 'exit code ' + code}), most likely from running out of memory or CPU on this server.` +
          (errorLines.length ? `\n\nLast ffmpeg output before termination:\n${relevant}` : '')
        ));
      }
      reject(new Error(`ffmpeg exited with code ${code}\n${relevant}`));
    });
    proc.on('error', (e) => { if (opts.isPrimary) reject(e); else resolve(); });
  });
}

async function runConversionJob(jobId, inputPath) {
  const outDir = path.join(HLS_DIR, jobId);
  fs.mkdirSync(outDir, { recursive: true });

  try {
    if (!jobs[jobId]) return;
    jobs[jobId].status = 'probing';
    const probe = await ffprobeStreams(inputPath);
    const audioStreams = probe.streams.filter(s => s.codec_type === 'audio')
      .map(s => ({ index: s.index, language: s.tags?.language, title: s.tags?.title }));
    const subtitleStreams = probe.streams.filter(s => s.codec_type === 'subtitle' && s.codec_name !== 'hdmv_pgs_subtitle');
    const videoStream = probe.streams.find(s => s.codec_type === 'video');

    if (audioStreams.length === 0) throw new Error('No audio streams found in source file');
    if (!videoStream) throw new Error('No video stream found in source file');

    if (!jobs[jobId]) return;
    jobs[jobId].status = 'extracting_subtitles';
    jobs[jobId].subtitles = await extractSubtitles(inputPath, outDir, subtitleStreams);

    if (!jobs[jobId]) return;
    jobs[jobId].status = 'encoding';
    jobs[jobId].masterUrl = `/hls/${jobId}/master.m3u8`;

    const sourceIsH264 = videoStream.codec_name === 'h264';
    const threads = process.env.FFMPEG_THREADS || '2';
    const sourceVideoArgs = sourceIsH264
      ? ['-c:v', 'copy']
      : ['-c:v', 'libx264', '-crf', '18', '-preset', 'veryfast', '-threads', threads, '-g', '48', '-keyint_min', '48', '-sc_threshold', '0'];
    if (sourceIsH264) sourceVideoArgs.push('-avoid_negative_ts', 'make_zero');

    const sourceBandwidth = parseInt(videoStream.bit_rate, 10)
      || parseInt(videoStream.tags?.BPS, 10)
      || parseInt(probe.format?.bit_rate, 10)
      || 2000000;

    const sourceWidth = parseInt(videoStream.width, 10) || 0;
    const sourceHeight = parseInt(videoStream.height, 10) || 0;
    const createFallback = sourceHeight > ABR_FALLBACK_MIN_SOURCE_HEIGHT;
    const fallbackWidth = sourceHeight ? Math.round((sourceWidth * ABR_FALLBACK_HEIGHT) / sourceHeight / 2) * 2 : null;

    audioStreams.forEach((a, i) => writePlaceholderPlaylist(outDir, `audio_${i}.m3u8`));
    if (createFallback) writePlaceholderPlaylist(outDir, 'video_480p.m3u8');

    const videoRenditions = [];
    if (createFallback) {
      videoRenditions.push({
        bandwidth: 800000,
        uri: 'video_480p.m3u8',
        resolution: fallbackWidth ? `${fallbackWidth}x${ABR_FALLBACK_HEIGHT}` : null
      });
    }
    videoRenditions.push({
      bandwidth: sourceBandwidth,
      uri: 'video_source.m3u8',
      resolution: (sourceWidth && sourceHeight) ? `${sourceWidth}x${sourceHeight}` : null
    });
    buildMasterPlaylist(outDir, audioStreams, videoRenditions);

    const backgroundPromises = [];

    backgroundPromises.push(
      detectSilence(inputPath)
        .then(silences => { if (jobs[jobId]) jobs[jobId].silences = silences; })
        .catch(() => { if (jobs[jobId]) jobs[jobId].silences = []; })
    );

    audioStreams.forEach((a, i) => {
      backgroundPromises.push(runAudioTrackPass(inputPath, outDir, a.index, String(i)));
    });

    if (createFallback) {
      backgroundPromises.push(runVideoRenditionPass(jobId, outDir, inputPath, {
        videoArgs: ['-c:v', 'libx264', '-preset', 'veryfast', '-vf', `scale=-2:${ABR_FALLBACK_HEIGHT}`, '-b:v', '800k', '-maxrate', '856k', '-bufsize', '1200k', '-threads', threads],
        segPrefix: 'segV_480p',
        playlistName: 'video_480p.m3u8',
        isPrimary: false
      }));
    }

    const primaryPromise = runVideoRenditionPass(jobId, outDir, inputPath, {
      videoArgs: sourceVideoArgs,
      segPrefix: 'segV_source',
      playlistName: 'video_source.m3u8',
      isPrimary: true,
      onBuffered: () => {
        if (jobs[jobId]) {
          jobs[jobId].status = 'streaming';
          console.log(`[Live Stream] Job ${jobId} buffered ~30s. Client can start playback.`);
        }
      }
    }).then(() => {
      if (jobs[jobId]) {
        jobs[jobId].status = 'done';
        jobs[jobId].completedAt = Date.now();
      }
    });

    const [primaryOutcome] = await Promise.allSettled([primaryPromise, Promise.allSettled(backgroundPromises)]);
    if (primaryOutcome.status === 'rejected') throw primaryOutcome.reason;

  } catch (err) {
    if (jobs[jobId]) {
      jobs[jobId].status = 'error';
      jobs[jobId].error = err.message;
      jobs[jobId].completedAt = Date.now();
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
  jobs[jobId] = { status: 'queued', createdAt: Date.now() };
  res.json({ jobId });

  const ext = safeExtensionFromUrl(url);
  const destPath = path.join(UPLOAD_DIR, `${jobId}.${ext}`);
  try {
    if (jobs[jobId]) jobs[jobId].status = 'downloading';
    await downloadToFile(url, destPath);
    runConversionJob(jobId, destPath);
  } catch (err) {
    if (jobs[jobId]) {
      jobs[jobId].status = 'error';
      jobs[jobId].error = err.message;
      jobs[jobId].completedAt = Date.now();
    }
  }
});

app.get('/api/convert/status/:jobId', (req, res) => {
  const job = jobs[req.params.jobId];
  if (!job) return res.status(404).json({ error: 'Unknown job' });
  res.json(job);
});

// ---------------------------------------------------------------------------
// Room State & Socket Handling
// ---------------------------------------------------------------------------
const roomsData = {};
cleanOldStreams();
setInterval(cleanOldStreams, CLEANUP_INTERVAL_MS);

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
    if (typeof from !== 'number' || typeof to !== 'number') return;
    if (from < 0 || from >= room.queue.length || to < 0 || to >= room.queue.length) return;
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

// ---------------------------------------------------------------------------
// Complete System Diagnostic Endpoint
// ---------------------------------------------------------------------------
app.get('/api/diagnostic', (req, res) => {
    const memUsage = process.memoryUsage();
    const formatBytes = (bytes) => `${(bytes / 1024 / 1024).toFixed(2)} MB`;

    const jobStats = { total: 0, queued: 0, downloading: 0, encoding: 0, streaming: 0, done: 0, error: 0 };
    Object.values(jobs).forEach(job => {
        jobStats.total++;
        if (jobStats[job.status] !== undefined) jobStats[job.status]++;
    });

    const roomStats = { totalRooms: 0, totalUsers: 0, totalVoiceUsers: 0 };
    const activeRoomsDetails = {};
    
    Object.entries(roomsData).forEach(([roomId, room]) => {
        roomStats.totalRooms++;
        roomStats.totalUsers += room.count;
        roomStats.totalVoiceUsers += room.voiceUsers.size;
        
        activeRoomsDetails[roomId] = {
            users: room.count,
            voiceUsers: room.voiceUsers.size,
            sourceType: room.sourceType,
            playbackState: room.playbackState
        };
    });

    let hlsFolderCount = 0;
    try {
        hlsFolderCount = fs.readdirSync(HLS_DIR).filter(f => fs.statSync(path.join(HLS_DIR, f)).isDirectory()).length;
    } catch (e) {
        hlsFolderCount = 'Error reading directory';
    }

    res.json({
        server: {
            uptime_seconds: Math.floor(process.uptime()),
            memory: {
                rss: formatBytes(memUsage.rss),
                heapTotal: formatBytes(memUsage.heapTotal),
                heapUsed: formatBytes(memUsage.heapUsed),
            }
        },
        storage: {
            active_hls_stream_folders: hlsFolderCount,
            stream_max_age_hours: STREAM_MAX_AGE_MS / 1000 / 60 / 60
        },
        conversion_jobs: jobStats,
        websockets: {
            ...roomStats,
            rooms: activeRoomsDetails
        }
    });
});

// SPA fallback for any unmatched GET route.
app.use((req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`SyncTube Pro Server running on port ${PORT}`);
});
