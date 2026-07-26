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
// Disk Cleanup: Purge streams older than 4 hours every 30 minutes
// ---------------------------------------------------------------------------
const STREAM_MAX_AGE_MS = 4 * 60 * 60 * 1000; 
const CLEANUP_INTERVAL_MS = 30 * 60 * 1000;    

function cleanOldStreams() {
    try {
        // Never delete a folder that's still the active stream for a live room,
        // no matter how old it is — a long movie or marathon session can
        // easily outlast the age threshold below.
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
    } catch(e) {
        console.error('[Cleanup] Error:', e.message);
    }
}
// Started further down, once roomsData exists (see below) — cleanOldStreams
// now cross-references active rooms, so it can't run before that's declared.

// ---------------------------------------------------------------------------
// File Downloader
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
// Conversion Pipeline
// ---------------------------------------------------------------------------
const jobs = {};

// --- Tunables (env-overridable) ---
// EBU R128-style loudness targets. -16 LUFS integrated / -1.5 dBTP / 11 LU
// range is a standard streaming target. Single-pass ("dynamic") mode is used
// by default so loudness correction doesn't delay the live-edge start —
// true two-pass EBU R128 compliance requires fully analyzing the audio
// before encoding can begin, which would push back "start playing in ~30s"
// by roughly however long it takes to decode the whole source's audio.
const LOUDNORM_I = process.env.LOUDNORM_I || '-16';
const LOUDNORM_TP = process.env.LOUDNORM_TP || '-1.5';
const LOUDNORM_LRA = process.env.LOUDNORM_LRA || '11';
const LOUDNORM_FILTER = `loudnorm=I=${LOUDNORM_I}:TP=${LOUDNORM_TP}:LRA=${LOUDNORM_LRA}`;

const SILENCE_NOISE_DB = process.env.SILENCE_NOISE_DB || '-30dB';
const SILENCE_MIN_DURATION = process.env.SILENCE_MIN_DURATION || '0.5';

const ABR_FALLBACK_HEIGHT = 480;
const ABR_FALLBACK_MIN_SOURCE_HEIGHT = 540; // skip the fallback rendition entirely if the source is already small

function ffprobeStreams(filePath) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(filePath)) {
      return reject(new Error(`File not found: ${filePath}`));
    }
    
    const args = ['-v', 'quiet', '-print_format', 'json', '-show_streams', '-show_format', filePath];
    const proc = spawn(ffprobePath, args);
    let out = '', err = '';
    
    proc.stdout.on('data', d => out += d);
    proc.stderr.on('data', d => err += d);
    
    proc.on('close', code => {
      if (code !== 0) {
        return reject(new Error(`ffprobe failed (exit ${code}): ${err.trim() || 'no error details'}`));
      }
      try { 
        resolve(JSON.parse(out)); 
      } catch (e) { 
        reject(new Error(`Failed to parse ffprobe JSON: ${e.message}`));
      }
    });
    
    proc.on('error', (e) => {
      reject(new Error(`Failed to spawn ffprobe: ${e.message}`));
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

// Detects silent gaps in the source audio so the client can auto-skip dead
// air. Runs as its own independent, audio-only, no-file-output pass — cheap,
// and fully decoupled from the video pipeline so it never delays playback.
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
  // A minimal but valid empty HLS playlist. Written before the master
  // playlist references it, so if a player eagerly fetches every listed
  // rendition right after loading the master (hls.js does this for
  // alternate-audio groups, and sometimes for ABR video levels on a cold
  // start too), it gets a technically-valid empty playlist instead of a
  // 404 — ffmpeg overwrites this with real segments moments later once its
  // own background encode actually starts producing output.
  const placeholder = '#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n';
  try { fs.writeFileSync(path.join(outDir, filename), placeholder); } catch (e) {}
}

// audioTracks: [{ index, language, title }], video renditions carry no audio
// of their own — every rendition (including the "default" one) lives in a
// shared alternate-audio group, so an ABR downgrade to a cheaper video
// rendition never forces the client to also fetch the full-bitrate stream
// just to get its audio.
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

// One loudness-normalized, audio-only HLS rendition per source audio track.
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

// A video-only HLS rendition (no audio at all — see buildMasterPlaylist).
// Used for both the source-quality primary rendition and the optional 480p
// ABR fallback. Only the primary rendition drives job status/timing and can
// fail the whole job; a failed fallback rendition just means hls.js won't
// have a low-bandwidth option, which is a soft degradation, not an error.
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
      // fs.watch can miss events or misbehave on some filesystems/platforms,
      // so it's paired with a plain poll as a reliable fallback.
      try { segWatcher = fs.watch(outDir, () => checkBuffered()); } catch (e) {}
    }
    const pollHandle = opts.onBuffered ? setInterval(checkBuffered, 2000) : null;

    proc.stderr.on('data', d => {
      stderrBuf = (stderrBuf + d.toString()).slice(-20000);
      if (opts.isPrimary) {
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
    jobs[jobId].status = 'probing';
    const probe = await ffprobeStreams(inputPath);
    const audioStreams = probe.streams.filter(s => s.codec_type === 'audio')
      .map(s => ({ index: s.index, language: s.tags?.language, title: s.tags?.title }));
    const subtitleStreams = probe.streams.filter(s => s.codec_type === 'subtitle' && s.codec_name !== 'hdmv_pgs_subtitle');
    const videoStream = probe.streams.find(s => s.codec_type === 'video');

    if (audioStreams.length === 0) throw new Error('No audio streams found in source file');
    if (!videoStream) throw new Error('No video stream found in source file');

    jobs[jobId].status = 'extracting_subtitles';
    jobs[jobId].subtitles = await extractSubtitles(inputPath, outDir, subtitleStreams);

    jobs[jobId].status = 'encoding';
    jobs[jobId].masterUrl = `/hls/${jobId}/master.m3u8`;

    // Only stream-copy the video when it's already H.264 — that's the one
    // codec virtually every browser can decode via MSE/hls.js. Anything else
    // (HEVC, VP9, AV1, etc.) gets re-encoded, or it simply won't play back
    // for most viewers even though the "conversion" itself would succeed.
    const sourceIsH264 = videoStream.codec_name === 'h264';
    const threads = process.env.FFMPEG_THREADS || '2';
    const sourceVideoArgs = sourceIsH264
      ? ['-c:v', 'copy']
      : ['-c:v', 'libx264', '-crf', '18', '-preset', 'veryfast', '-threads', threads, '-g', '48', '-keyint_min', '48', '-sc_threshold', '0'];
    if (sourceIsH264) sourceVideoArgs.push('-avoid_negative_ts', 'make_zero');

    // ffprobe reports bitrate inconsistently across containers — MKV sources
    // commonly tag it as a BPS metadata field rather than the top-level
    // bit_rate property MP4 uses, so check both before falling back.
    const sourceBandwidth = parseInt(videoStream.bit_rate, 10)
      || parseInt(videoStream.tags?.BPS, 10)
      || parseInt(probe.format?.bit_rate, 10)
      || 2000000;

    const sourceWidth = parseInt(videoStream.width, 10) || 0;
    const sourceHeight = parseInt(videoStream.height, 10) || 0;
    const createFallback = sourceHeight > ABR_FALLBACK_MIN_SOURCE_HEIGHT;
    const fallbackWidth = sourceHeight ? Math.round((sourceWidth * ABR_FALLBACK_HEIGHT) / sourceHeight / 2) * 2 : null;

    // Placeholders for every rendition the master is about to reference,
    // written before the master file itself — see writePlaceholderPlaylist.
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

    // --- Everything below runs in parallel with the primary video pass ---
    const backgroundPromises = [];

    // Skip-silence detection — independent, audio-only, never blocks playback.
    backgroundPromises.push(
      detectSilence(inputPath)
        .then(silences => { jobs[jobId].silences = silences; })
        .catch(() => { jobs[jobId].silences = []; })
    );

    // One loudness-normalized HLS rendition per source audio track.
    audioStreams.forEach((a, i) => {
      backgroundPromises.push(runAudioTrackPass(inputPath, outDir, a.index, String(i)));
    });

    // Lightweight 480p fallback rendition for hls.js's automatic ABR downgrade
    // on slow connections. Non-critical: if it fails, the source rendition
    // still plays fine, there's just no low-bandwidth option to fall back to.
    if (createFallback) {
      backgroundPromises.push(runVideoRenditionPass(jobId, outDir, inputPath, {
        videoArgs: ['-c:v', 'libx264', '-preset', 'veryfast', '-vf', `scale=-2:${ABR_FALLBACK_HEIGHT}`, '-b:v', '800k', '-maxrate', '856k', '-bufsize', '1200k', '-threads', threads],
        segPrefix: 'segV_480p',
        playlistName: 'video_480p.m3u8',
        isPrimary: false
      }));
    }

    // --- Primary source-quality video rendition (the critical path) ---
    const primaryPromise = runVideoRenditionPass(jobId, outDir, inputPath, {
      videoArgs: sourceVideoArgs,
      segPrefix: 'segV_source',
      playlistName: 'video_source.m3u8',
      isPrimary: true,
      onBuffered: () => {
        jobs[jobId].status = 'streaming';
        console.log(`[Live Stream] Job ${jobId} buffered ~30s. Client can start playback.`);
      }
    }).then(() => { jobs[jobId].status = 'done'; });

    // Wait for the primary rendition AND every background task, regardless of
    // which finishes or fails first — otherwise `finally` below could delete
    // the shared input file while a background process is still reading it.
    const [primaryOutcome] = await Promise.allSettled([primaryPromise, Promise.allSettled(backgroundPromises)]);
    if (primaryOutcome.status === 'rejected') throw primaryOutcome.reason;

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

// SPA fallback for any unmatched GET route. Deliberately app.use (not
// app.get('*', ...)) — Express 5's router rejects a bare '*' pattern outright
// since it moved to path-to-regexp v6, which requires a named wildcard. Plain
// middleware has no path-pattern parsing at all, so it works on both.
app.use((req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`SyncTube Pro Server running on port ${PORT}`);
});
