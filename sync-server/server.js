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
// Optional cloud storage
// ---------------------------------------------------------------------------
const S3_ENABLED = !!process.env.S3_BUCKET;
let s3Client = null;
if (S3_ENABLED) {
  const { S3Client } = require('@aws-sdk/client-s3');
  s3Client = new S3Client({
    region: process.env.S3_REGION || 'auto',
    endpoint: process.env.S3_ENDPOINT || undefined,
    credentials: { accessKeyId: process.env.S3_ACCESS_KEY, secretAccessKey: process.env.S3_SECRET_KEY }
  });
}

async function uploadDirToS3(jobId, localDir) {
  const { Upload } = require('@aws-sdk/lib-storage');
  const files = [];
  (function walk(dir, prefix) {
    fs.readdirSync(dir, { withFileTypes: true }).forEach(entry => {
      const full = path.join(dir, entry.name);
      const key = `${prefix}/${entry.name}`;
      if (entry.isDirectory()) walk(full, key); else files.push({ full, key });
    });
  })(localDir, `hls/${jobId}`);

  const contentType = (name) => {
    if (name.endsWith('.m3u8')) return 'application/vnd.apple.mpegurl';
    if (name.endsWith('.ts')) return 'video/mp2t';
    if (name.endsWith('.vtt')) return 'text/vtt';
    return 'application/octet-stream';
  };

  await Promise.all(files.map(f => new Upload({
    client: s3Client,
    params: {
      Bucket: process.env.S3_BUCKET, Key: f.key, Body: fs.createReadStream(f.full),
      ContentType: contentType(f.full), ACL: process.env.S3_ACL || 'public-read'
    }
  }).done()));

  const base = process.env.CDN_BASE_URL || `https://${process.env.S3_BUCKET}.s3.${process.env.S3_REGION || 'us-east-1'}.amazonaws.com`;
  return `${base}/hls/${jobId}`;
}

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
  const LADDER = [
    { name: '1080p', height: 1080, vbitrate: '5000k', maxrate: '5350k', bufsize: '7500k' },
    { name: '720p', height: 720, vbitrate: '2800k', maxrate: '2996k', bufsize: '4200k' },
    { name: '480p', height: 480, vbitrate: '1400k', maxrate: '1498k', bufsize: '2100k' }
  ];
  const args = ['-y', '-i', inputPath];
  LADDER.forEach(() => args.push('-map', '0:v:0'));
  audioStreams.forEach(a => args.push('-map', `0:${a.index}`));

  LADDER.forEach((rung, i) => {
    args.push(
      `-c:v:${i}`, 'libx264', `-filter:v:${i}`, `scale=-2:${rung.height}`,
      `-b:v:${i}`, rung.vbitrate, `-maxrate:v:${i}`, rung.maxrate, `-bufsize:v:${i}`, rung.bufsize,
      '-preset', 'veryfast', '-g', '48', '-keyint_min', '48', '-sc_threshold', '0'
    );
  });
  audioStreams.forEach((a, i) => args.push(`-c:a:${i}`, 'aac', `-b:a:${i}`, '128k', `-ac:${i}`, '2'));

  const streamMapParts = [];
  LADDER.forEach((rung, vi) => streamMapParts.push(`v:${vi},a:0,name:${rung.name},agroup:aud`));
  audioStreams.forEach((a, ai) => {
    const lang = a.language || 'und';
    const label = a.title || `Track ${ai + 1} (${lang})`;
    streamMapParts.push(`a:${ai},agroup:aud,name:${label},language:${lang}${ai === 0 ? ',default:yes' : ''}`);
  });

  args.push(
    '-var_stream_map', streamMapParts.join(' '),
    '-f', 'hls', '-hls_time', '6', '-hls_list_size', '0', '-hls_flags', 'independent_segments',
    '-master_pl_name', 'master.m3u8',
    '-hls_segment_filename', path.join(outDir, 'stream_%v/data%03d.ts'),
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
    await new Promise((resolve, reject) => {
      const proc = spawn(ffmpegPath, buildFfmpegArgs(inputPath, outDir, audioStreams));
      let stderrTail = '';
      proc.stderr.on('data', d => {
        stderrTail = (stderrTail + d.toString()).slice(-4000);
        const m = d.toString().match(/time=(\d+):(\d+):(\d+\.\d+)/);
        if (m) jobs[jobId].lastTimeMark = `${m[1]}:${m[2]}:${m[3]}`;
      });
      proc.on('close', code => code === 0 ? resolve() : reject(new Error('ffmpeg exited ' + code + '\n' + stderrTail)));
      proc.on('error', reject);
    });

    if (S3_ENABLED) {
      jobs[jobId].status = 'uploading';
      const baseUrl = await uploadDirToS3(jobId, outDir);
      jobs[jobId].masterUrl = `${baseUrl}/master.m3u8`;
      fs.rm(outDir, { recursive: true, force: true }, () => {});
    } else {
      jobs[jobId].masterUrl = `/hls/${jobId}/master.m3u8`;
    }

    jobs[jobId].status = 'done';
  } catch (err) {
    jobs[jobId].status = 'error';
    jobs[jobId].error = err.message;
    fs.rm(outDir, { recursive: true, force: true }, () => {});
  } finally {
    fs.rm(inputPath, { force: true }, () => {});
  }
}

app.post('/api/convert-from-url', async (req, res) => {
  const { url } = req.body || {};
  if (!url) return res.status(400).json({ error: 'Missing url' });

  const jobId = crypto.randomBytes(6).toString('hex');
  jobs[jobId] = { status: 'queued' };
  res.json({ jobId });

  const ext = (url.split('?')[0].split('.').pop() || 'mp4').slice(0, 5);
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
// Room state
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
        currentVideoTime: 0, playbackState: 'paused', lastUpdatedAt: Date.now()
      };
    }
    const room = roomsData[roomId];
    room.count++;
    room.members[socket.id] = username;
    
    // IMMEDATE HOST CONFIRMATION FIX: Bypasses network lag to grant the creator instant control
    socket.emit('room-info', { count: room.count, queue: room.queue, isAdmin: room.admin === socket.id });

    broadcastActiveRooms();
    broadcastRoomState(roomId);

    socket.emit('initial-sync', {
      sourceType: room.sourceType, videoId: room.currentVideoId, rawUrl: room.currentRawUrl,
      title: room.currentRawTitle, time: currentPlaybackTime(room), playbackState: room.playbackState
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
      room.currentVideoTime = 0; room.playbackState = 'playing'; room.lastUpdatedAt = Date.now();
    }

    socket.to(data.roomId).emit('sync-video', data);
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

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`SyncTube Pro Server running on port ${PORT}`);
});
