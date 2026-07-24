const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const { spawn, execSync } = require('child_process');
const path = require('path');
const fs = require('fs');
const https = require('https');
const httpLib = require('http');

// Use ffmpeg-static / ffprobe-static so Render doesn't need system FFmpeg installed.
// `npm i ffmpeg-static ffprobe-static` — the binary paths are resolved automatically.
let ffmpegPath = 'ffmpeg';   // fallback to system if package not installed
let ffprobePath = 'ffprobe';
try { ffmpegPath  = require('ffmpeg-static');            } catch(_) {}
try { ffprobePath = require('ffprobe-static').path;      } catch(_) {}
console.log(`[FFmpeg]  binary: ${ffmpegPath}`);
console.log(`[FFprobe] binary: ${ffprobePath}`);

// ---------------------------------------------------------------------------
// Optional S3 / Cloudflare R2 cloud storage for HLS output.
// Set S3_BUCKET env var on Render to enable — falls back to local disk.
// Requires: npm i @aws-sdk/client-s3 @aws-sdk/lib-storage
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
    console.log('[S3] Cloud storage enabled');
}

async function uploadDirToS3(streamId, localDir) {
    const { Upload } = require('@aws-sdk/lib-storage');
    const files = [];
    (function walk(dir, prefix) {
        fs.readdirSync(dir, { withFileTypes: true }).forEach(entry => {
            const full = path.join(dir, entry.name);
            const key  = `${prefix}/${entry.name}`;
            if (entry.isDirectory()) walk(full, key); else files.push({ full, key });
        });
    })(localDir, `hls/${streamId}`);

    const contentType = name => {
        if (name.endsWith('.m3u8')) return 'application/vnd.apple.mpegurl';
        if (name.endsWith('.ts'))   return 'video/mp2t';
        if (name.endsWith('.vtt'))  return 'text/vtt';
        return 'application/octet-stream';
    };

    await Promise.all(files.map(f => new Upload({
        client: s3Client,
        params: { Bucket: process.env.S3_BUCKET, Key: f.key, Body: fs.createReadStream(f.full), ContentType: contentType(f.full), ACL: process.env.S3_ACL || 'public-read' }
    }).done()));

    const base = process.env.CDN_BASE_URL || `https://${process.env.S3_BUCKET}.s3.${process.env.S3_REGION||'us-east-1'}.amazonaws.com`;
    return `${base}/hls/${streamId}`;
}

// ---------------------------------------------------------------------------
// Download a direct video URL to local disk before conversion.
// Blocked hosts: streaming platforms that must not be downloaded.
// ---------------------------------------------------------------------------
const BLOCKED_HOSTS = ['youtube.com','youtu.be','vimeo.com','netflix.com','twitch.tv','dailymotion.com'];

function downloadToFile(url, destPath, redirects = 0) {
    return new Promise((resolve, reject) => {
        let host;
        try { host = new URL(url).hostname.replace(/^www\./, ''); } catch(e) { return reject(new Error('Invalid URL')); }
        if (BLOCKED_HOSTS.some(h => host.endsWith(h))) return reject(new Error('Direct downloads from streaming platforms are not supported.'));
        if (redirects > 5) return reject(new Error('Too many redirects'));

        const lib = url.startsWith('https') ? https : httpLib;
        const req = lib.get(url, { headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Referer': 'https://www.google.com/'
        }}, res => {
            if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {
                res.resume();
                return resolve(downloadToFile(res.headers.location, destPath, redirects + 1));
            }
            if (res.statusCode !== 200) { res.resume(); return reject(new Error('HTTP ' + res.statusCode + ' Forbidden — link may have expired')); }
            const file = fs.createWriteStream(destPath);
            res.pipe(file);
            file.on('finish', () => file.close(resolve));
            file.on('error', reject);
        });
        req.on('error', reject);
        req.setTimeout(60000, () => req.destroy(new Error('Download timed out')));
    });
}

// ffprobe wrapper — returns parsed JSON with streams + format
function ffprobeStreams(filePath) {
    return new Promise((resolve, reject) => {
        const args = ['-v','quiet','-print_format','json','-show_streams','-show_format', filePath];
        const proc = spawn(ffprobePath, args);
        let out = '', err = '';
        proc.stdout.on('data', d => out += d);
        proc.stderr.on('data', d => err += d);
        proc.on('close', code => {
            if (code !== 0) return reject(new Error(err || 'ffprobe failed'));
            try { resolve(JSON.parse(out)); } catch(e) { reject(e); }
        });
    });
}

// Extract subtitle tracks to VTT files (skips PGS bitmap subs)
function extractSubtitles(inputPath, outDir, subtitleStreams) {
    return Promise.all(subtitleStreams.map((s, i) => new Promise(resolve => {
        const outFile = path.join(outDir, `sub_${i}.vtt`);
        const proc = spawn(ffmpegPath, ['-y','-i',inputPath,'-map',`0:${s.index}`, outFile]);
        proc.on('close', () => resolve({ file: `sub_${i}.vtt`, language: s.tags?.language || 'und', title: s.tags?.title || `Subtitle ${i+1}` }));
        proc.on('error', () => resolve(null));
    }))).then(list => list.filter(Boolean));
}

// Build ABR (adaptive bitrate) FFmpeg args: 3 video qualities + N audio tracks
// Each quality renders a separate HLS playlist so viewers auto-adjust to bandwidth.
function buildABRArgs(inputPath, outDir, audioStreams) {
    const LADDER = [
        { name:'1080p', height:1080, vbitrate:'5000k', maxrate:'5350k', bufsize:'7500k' },
        { name:'720p',  height:720,  vbitrate:'2800k', maxrate:'2996k', bufsize:'4200k' },
        { name:'480p',  height:480,  vbitrate:'1400k', maxrate:'1498k', bufsize:'2100k' }
    ];
    const args = ['-y', '-i', inputPath];
    LADDER.forEach(() => args.push('-map', '0:v:0'));
    audioStreams.forEach(a => args.push('-map', `0:${a.index}`));

    LADDER.forEach((rung, i) => args.push(
        `-c:v:${i}`, 'libx264', `-filter:v:${i}`, `scale=-2:${rung.height}`,
        `-b:v:${i}`, rung.vbitrate, `-maxrate:v:${i}`, rung.maxrate, `-bufsize:v:${i}`, rung.bufsize,
        '-preset', 'veryfast', '-g', '48', '-keyint_min', '48', '-sc_threshold', '0'
    ));
    audioStreams.forEach((a, i) => args.push(`-c:a:${i}`, 'aac', `-b:a:${i}`, '128k', `-ac:${i}`, '2'));

    const streamMapParts = [];
    LADDER.forEach((rung, vi) => streamMapParts.push(`v:${vi},a:0,name:${rung.name},agroup:aud`));
    audioStreams.forEach((a, ai) => {
        const lang  = a.language || 'und';
        const label = a.title || `Track ${ai+1} (${lang})`;
        streamMapParts.push(`a:${ai},agroup:aud,name:${label},language:${lang}${ai===0?',default:yes':''}`);
    });

    args.push(
        '-var_stream_map', streamMapParts.join(' '),
        '-f', 'hls', '-hls_time', '6', '-hls_list_size', '0', '-hls_flags', 'independent_segments',
        '-master_pl_name', 'playlist.m3u8',
        '-hls_segment_filename', path.join(outDir, 'stream_%v/seg%03d.ts'),
        path.join(outDir, 'stream_%v.m3u8')
    );
    return args;
}

const app = express();
app.use(cors());
app.use(express.json()); // Essential for parsing POST bodies

// Ensure the root public directory exists
const publicDir = path.join(__dirname, 'public');
if (!fs.existsSync(publicDir)) {
    fs.mkdirSync(publicDir);
}
app.use('/public', express.static(publicDir));

// ── Disk Cleanup — auto-delete stream folders older than 4 hours ──
// Render free tier has ~500MB disk. A 2hr film generates ~1.5GB of .ts segments.
// Without cleanup the server crashes on the 2nd or 3rd conversion.
const STREAM_MAX_AGE_MS = 4 * 60 * 60 * 1000; // 4 hours
const CLEANUP_INTERVAL_MS = 30 * 60 * 1000;    // run every 30 minutes

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

                // Calculate folder size before deleting
                const files = fs.readdirSync(dir);
                for (const f of files) {
                    try { freed += fs.statSync(path.join(dir, f)).size; } catch(_) {}
                    try { fs.unlinkSync(path.join(dir, f)); } catch(_) {}
                }
                fs.rmdirSync(dir);
                deleted++;

                // Also remove from jobs store
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

// Run cleanup on startup (clears leftover files from previous deploys)
cleanOldStreams();
// Then run every 30 minutes
setInterval(cleanOldStreams, CLEANUP_INTERVAL_MS);

// Manual cleanup endpoint — callable from browser for immediate purge
app.post('/api/cleanup', (req, res) => {
    cleanOldStreams();
    const dirs = fs.readdirSync(publicDir).filter(n => n.startsWith('stream_')).length;
    res.json({ status: 'ok', remainingStreams: dirs });
});

// Disk usage endpoint — shows how full the server is
app.get('/api/disk', (req, res) => {
    try {
        let totalBytes = 0;
        const streams = [];
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
        res.json({
            totalMb: (totalBytes/1024/1024).toFixed(1),
            streamCount: streams.length,
            streams: streams.sort((a,b) => b.ageMins - a.ageMins)
        });
    } catch(e) {
        res.status(500).json({ error: e.message });
    }
});

// Keep-Alive Pulse Route
app.get('/', (req, res) => {
    res.status(200).send('SyncTube Backend is Awake and Running! 🚀');
});

// In-memory job store — tracks all active/completed conversions
const jobs = {};

// How many 6-second segments must exist before we tell the frontend to start playing
// 5 segments = 30 seconds of buffer — enough to start smoothly
const LIVE_START_SEGMENTS = 5;

// --- STEP 1: Start live-streaming conversion, return job ID immediately ---
app.post('/api/convert', (req, res) => {
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

    // Register job immediately
    jobs[jobId] = {
        status: 'pending',
        streamId,
        streamDir,
        manifestUrl: `/public/${streamId}/playlist.m3u8`,
        startedAt: Date.now(),
        segments: 0
    };

    // Reply instantly — frontend polls status
    res.json({ status: 'queued', jobId });

    // ── Feature 4+7: Probe for title, duration, audio track names ──
    let numAudio = 1;
    let videoTitle = '';
    let videoDuration = 0;
    let audioLangs = [];
    const { execSync } = require('child_process');

    try {
        const probeJson = execSync(
            `ffprobe -v quiet -print_format json -show_format -show_streams "${videoUrl.replace(/"/g,'\"')}"`,
            { timeout: 20000 }
        ).toString();
        const probe = JSON.parse(probeJson);

        // Title from metadata
        videoTitle = probe.format?.tags?.title || probe.format?.tags?.TITLE || '';
        videoDuration = parseFloat(probe.format?.duration || 0);

        // Audio streams with real language names
        const audioStreams = (probe.streams || []).filter(s => s.codec_type === 'audio');
        numAudio = audioStreams.length || 1;
        audioLangs = audioStreams.map((s, i) => {
            const lang = s.tags?.language || s.tags?.LANGUAGE || '';
            const title = s.tags?.title || s.tags?.TITLE || '';
            // Build a human-readable label: prefer title, then language code mapped to name
            const langMap = { eng:'English', hin:'Hindi', jpn:'Japanese', tam:'Tamil',
                              tel:'Telugu', fra:'French', spa:'Spanish', kor:'Korean',
                              ara:'Arabic', por:'Portuguese', deu:'German', zho:'Chinese' };
            const label = title || langMap[lang] || (lang ? lang.toUpperCase() : `Track ${i+1}`);
            return { index: i, lang, label };
        });

        console.log(`[Probe] Job ${jobId}: title="${videoTitle}" duration=${videoDuration}s audio=${numAudio}`);
        console.log(`[Probe] Audio tracks:`, audioLangs.map(a => a.label).join(', '));

    } catch(e) {
        console.log('[Probe] ffprobe failed:', e.message);
    }

    // Store metadata in job for frontend to retrieve
    jobs[jobId].title    = videoTitle;
    jobs[jobId].duration = videoDuration;
    jobs[jobId].audioLangs = audioLangs;

    // ── Feature 8: Generate thumbnail from frame at 10s ──
    const thumbPath = path.join(streamDir, 'thumb.jpg');
    try {
        execSync(
            `ffmpeg -y -ss 10 -i "${videoUrl.replace(/"/g,'\"')}" -frames:v 1 -q:v 2 -vf scale=320:-1 "${thumbPath}"`,
            { timeout: 25000 }
        );
        jobs[jobId].thumbUrl = `/public/${streamId}/thumb.jpg`;
        console.log(`[Thumb] Job ${jobId}: thumbnail generated`);
    } catch(e) {
        console.log('[Thumb] Thumbnail generation failed:', e.message);
    }

    // ── Build FFmpeg args for multi-audio HLS ──
    // FFmpeg 4.3 strategy: transcode video+primary audio together,
    // then transcode each extra audio track separately.
    // We then craft a master playlist that links them all as EXT-X-MEDIA groups.
    // This is the most reliable approach for HLS.js audioTracks support.

    if (numAudio <= 1) {
        // ── Single audio: simple single-output HLS ──
        const args = [
            '-y',
            '-user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            '-headers', 'Referer: https://www.google.com/\r\nAccept: */*\r\nAccept-Language: en-US,en;q=0.9\r\n',
            '-reconnect', '1', '-reconnect_streamed', '1', '-reconnect_delay_max', '5',
            '-i', videoUrl,
            '-map', '0:v:0', '-map', '0:a:0',
            '-c:v', 'copy', '-c:a', 'aac', '-ac', '2', '-b:a', '192k',
            '-max_muxing_queue_size', '9999',
            '-f', 'hls', '-hls_time', '6', '-hls_list_size', '0',
            '-hls_flags', 'append_list',
            '-hls_segment_filename', path.join(streamDir, 'seg_%03d.ts'),
            outputPath
        ];
    }
    // fall through to spawn below

    // ── Multi-audio: one FFmpeg pass per audio track + video ──
    // Pass 0: video + audio track 0 (default)
    // Pass 1..N: audio tracks 1..N only (no video, much faster)
    const commonInput = [
        '-user_agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        '-headers', 'Referer: https://www.google.com/\r\nAccept: */*\r\nAccept-Language: en-US,en;q=0.9\r\n',
        '-reconnect', '1', '-reconnect_streamed', '1', '-reconnect_delay_max', '5',
        '-i', videoUrl
    ];

    // Primary pass: video + track 0
    const args = [
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

    // Spawn extra audio-only passes for tracks 1..N (parallel)
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
        const aProc = spawn(ffmpegPath, aArgs);
        aProc.stderr.on('data', () => {}); // suppress
        aProc.on('close', code => console.log(`[FFmpeg] Audio track ${i} done (code ${code})`));
        aProc.on('error', e => console.error(`[FFmpeg] Audio track ${i} error:`, e.message));
    }

    // After primary pass finishes, build the master playlist
    // This is called from the proc.on('close') handler below
    jobs[jobId]._buildMaster = () => {
        try {
            const label0 = audioLangs[0]?.label || 'Track 1';
            const lang0  = audioLangs[0]?.lang  || 'und';

            let master = '#EXTM3U\n#EXT-X-VERSION:3\n\n';

            // Add EXT-X-MEDIA entries for each audio track
            for (let i = 0; i < numAudio; i++) {
                const label = audioLangs[i]?.label || `Track ${i+1}`;
                const lang  = audioLangs[i]?.lang  || 'und';
                const def   = i === 0 ? 'YES' : 'NO';
                const uri   = i === 0 ? 'stream_0.m3u8' : `stream_${i}.m3u8`;
                master += `#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="${label}",LANGUAGE="${lang}",DEFAULT=${def},AUTOSELECT=${def},URI="${uri}"\n`;
            }

            // Video stream pointing to stream_0 (which has video+default audio)
            master += `\n#EXT-X-STREAM-INF:BANDWIDTH=2000000,AUDIO="audio"\nstream_0.m3u8\n`;
            fs.writeFileSync(path.join(streamDir, 'playlist.m3u8'), master);
            console.log(`[Master] Job ${jobId}: master playlist written with ${numAudio} audio tracks`);
        } catch(e) {
            console.error('[Master] Failed to write master playlist:', e.message);
        }
    };

    console.log(`[FFmpeg] Live job ${jobId} started for: ${videoUrl}`);
    const proc = spawn(ffmpegPath, args);

    // Watch for new segment files — update segment count in real time
    const segWatcher = fs.watch(streamDir, (event, filename) => {
        if (filename && filename.endsWith('.ts')) {
            const segs = fs.readdirSync(streamDir).filter(f => f.endsWith('.ts')).length;
            jobs[jobId].segments = segs;

            // Once we have enough buffer, tell the frontend it can start playing
            if (jobs[jobId].status === 'pending' && segs >= LIVE_START_SEGMENTS) {
                console.log(`[FFmpeg] Job ${jobId} live — ${segs} segments ready, signalling frontend`);
                jobs[jobId].status = 'live';
            }
        }
    });

    // ── Feature 5: Parse FFmpeg stderr for real-time progress ──
    let ffmpegBuffer = '';
    proc.stderr.on('data', d => {
        ffmpegBuffer += d.toString();
        const lines = ffmpegBuffer.split('\r');
        ffmpegBuffer = lines.pop(); // keep incomplete line

        for (const line of lines) {
            if (!line.includes('time=')) continue;
            // Parse time= field: time=01:23:45.67
            const m = line.match(/time=([\d:]+\.?\d*)/);
            if (m) {
                const parts = m[1].split(':').map(Number);
                const secs = parts.length === 3
                    ? parts[0]*3600 + parts[1]*60 + parts[2]
                    : parts[0]*60 + parts[1];
                jobs[jobId].progress = {
                    currentTime: secs,
                    duration: jobs[jobId].duration || 0,
                    pct: jobs[jobId].duration > 0
                        ? Math.min(99, Math.round((secs / jobs[jobId].duration) * 100))
                        : null
                };
            }
        }
    });

    proc.on('close', code => {
        segWatcher.close();
        if (code === 0) {
            // Build master playlist for multi-audio before marking done
            if (jobs[jobId]?._buildMaster) {
                jobs[jobId]._buildMaster();
                delete jobs[jobId]._buildMaster;
            }
            console.log(`[FFmpeg] Job ${jobId} fully done ✅`);
            jobs[jobId].status = 'done';
        } else {
            console.error(`[FFmpeg] Job ${jobId} failed with code ${code}`);
            if (jobs[jobId]?.status === 'pending') {
                jobs[jobId].status = 'error';
                jobs[jobId].error  = `FFmpeg exited with code ${code}`;
            }
        }
    });

    proc.on('error', err => {
        segWatcher.close();
        console.error(`[FFmpeg] Job ${jobId} spawn error:`, err);
        if (jobs[jobId].status === 'pending') {
            jobs[jobId].status = 'error';
            jobs[jobId].error  = err.message;
        }
    });
});

// --- STEP 2: Frontend polls this every 3s ---
// Returns 'pending' until 30s of video is ready, then 'Success' to start playback
// FFmpeg keeps writing segments in the background while the user watches
app.get('/api/convert/status/:jobId', (req, res) => {
    const job = jobs[req.params.jobId];
    if (!job) return res.status(404).json({ error: 'Job not found' });

    if (job.status === 'live' || job.status === 'done') {
        const thumb = job.thumbUrl
            ? (job.thumbUrl.startsWith('http') ? job.thumbUrl : `https://gameearn-backend.onrender.com${job.thumbUrl}`)
            : '';
        return res.json({
            status: 'Success',
            manifestUrl: job.manifestUrl,
            segments: job.segments,
            live: job.status === 'live',
            title: job.title || '',
            duration: job.duration || 0,
            audioLangs: job.audioLangs || [],
            subtitles: job.subtitles || [],
            thumbUrl: thumb
        });
    }
    if (job.status === 'error') {
        return res.json({ status: 'Error', error: job.error });
    }
    // Still buffering — return progress for frontend display
    res.json({
        status: 'pending',
        segments: job.segments || 0,
        progress: job.progress || null,
        title: job.title || '',
        duration: job.duration || 0
    });
});

// Health check — confirms FFmpeg binary is present
app.get('/api/health', (req, res) => {
    try {
        const ver = execSync(`"${ffmpegPath}" -version 2>&1`).toString().split('\n')[0];
        res.json({ status: 'ok', ffmpeg: ver, binary: ffmpegPath });
    } catch(e) {
        res.status(500).json({ status: 'error', ffmpeg: 'NOT FOUND', binary: ffmpegPath });
    }
});

// ---------------------------------------------------------------------------
// /api/convert-from-url  — Downloads the video first, then runs full
// ABR + multi-audio + subtitle conversion pipeline.
// Better than streaming from URL: avoids CDN blocks, works with Hubcloud/
// CFStorage signed links, and gives accurate ffprobe results.
// Use this when /api/convert fails due to 403 or header issues.
// ---------------------------------------------------------------------------
const UPLOAD_DIR = path.join(__dirname, 'uploads');
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

app.post('/api/convert-from-url', async (req, res) => {
    const { videoUrl } = req.body || {};
    if (!videoUrl) return res.status(400).json({ error: 'Missing videoUrl' });

    const jobId    = `job_${Date.now()}_${Math.random().toString(36).slice(2,7)}`;
    const streamId = `stream_${Date.now()}`;
    const streamDir = path.join(publicDir, streamId);
    fs.mkdirSync(streamDir, { recursive: true });

    jobs[jobId] = { status: 'downloading', streamId, streamDir, manifestUrl: `/public/${streamId}/playlist.m3u8`, startedAt: Date.now(), segments: 0 };
    res.json({ status: 'queued', jobId });

    // Guess file extension from URL
    const ext = (videoUrl.split('?')[0].split('.').pop() || 'mp4').slice(0, 5);
    const destPath = path.join(UPLOAD_DIR, `${jobId}.${ext}`);

    try {
        // Step 1: Download
        console.log(`[Download] Job ${jobId}: fetching ${videoUrl}`);
        await downloadToFile(videoUrl, destPath);
        jobs[jobId].status = 'probing';

        // Step 2: Probe with ffprobe for accurate metadata
        const probe = await ffprobeStreams(destPath);
        const audioStreams   = probe.streams.filter(s => s.codec_type === 'audio').map(s => ({ index: s.index, language: s.tags?.language, title: s.tags?.title }));
        const subtitleStreams = probe.streams.filter(s => s.codec_type === 'subtitle' && s.codec_name !== 'hdmv_pgs_subtitle');

        jobs[jobId].title    = probe.format?.tags?.title || '';
        jobs[jobId].duration = parseFloat(probe.format?.duration || 0);
        jobs[jobId].audioLangs = audioStreams.map((a, i) => {
            const langMap = { eng:'English',hin:'Hindi',jpn:'Japanese',tam:'Tamil',tel:'Telugu',fra:'French',spa:'Spanish',kor:'Korean',ara:'Arabic',por:'Portuguese',deu:'German',zho:'Chinese' };
            const label = a.title || langMap[a.language] || (a.language ? a.language.toUpperCase() : `Track ${i+1}`);
            return { index: i, lang: a.language || 'und', label };
        });

        // Step 3: Extract subtitles in parallel
        jobs[jobId].status = 'extracting_subtitles';
        jobs[jobId].subtitles = await extractSubtitles(destPath, streamDir, subtitleStreams);

        // Step 4: Thumbnail from frame 10s
        const thumbPath = path.join(streamDir, 'thumb.jpg');
        try {
            const tProc = spawn(ffmpegPath, ['-y','-ss','10','-i',destPath,'-frames:v','1','-q:v','2','-vf','scale=320:-1',thumbPath]);
            await new Promise(r => tProc.on('close', r));
            if (fs.existsSync(thumbPath)) jobs[jobId].thumbUrl = `/public/${streamId}/thumb.jpg`;
        } catch(_) {}

        // Step 5: ABR HLS encoding (1080p + 720p + 480p + all audio tracks)
        jobs[jobId].status = 'encoding';
        const args = buildABRArgs(destPath, streamDir, audioStreams.length ? audioStreams : [{ index: 0 }]);
        const proc = spawn(ffmpegPath, args);
        fs.mkdirSync(path.join(streamDir, 'stream_0'), { recursive: true });

        // Watch for segments to signal live start
        const segWatcher = fs.watch(streamDir, { recursive: true }, (event, filename) => {
            if (filename && filename.endsWith('.ts')) {
                const count = fs.readdirSync(path.join(streamDir, 'stream_0')).filter(f => f.endsWith('.ts')).length;
                jobs[jobId].segments = count;
                if (jobs[jobId].status === 'encoding' && count >= LIVE_START_SEGMENTS) {
                    jobs[jobId].status = 'live';
                    console.log(`[Job ${jobId}] Live — ${count} segments ready`);
                }
            }
        });

        let ffmpegBuf = '';
        proc.stderr.on('data', d => {
            ffmpegBuf += d.toString();
            const lines = ffmpegBuf.split('\r');
            ffmpegBuf = lines.pop();
            for (const line of lines) {
                const m = line.match(/time=([\d:]+\.?\d*)/);
                if (m) {
                    const parts = m[1].split(':').map(Number);
                    const secs = parts.length === 3 ? parts[0]*3600 + parts[1]*60 + parts[2] : parts[0]*60 + parts[1];
                    jobs[jobId].progress = { currentTime: secs, duration: jobs[jobId].duration || 0, pct: jobs[jobId].duration > 0 ? Math.min(99, Math.round((secs/jobs[jobId].duration)*100)) : null };
                }
            }
        });

        proc.on('close', async code => {
            segWatcher.close();
            fs.rm(destPath, { force: true }, () => {}); // delete downloaded source
            if (code === 0) {
                if (S3_ENABLED) {
                    jobs[jobId].status = 'uploading';
                    const baseUrl = await uploadDirToS3(streamId, streamDir);
                    jobs[jobId].manifestUrl = `${baseUrl}/playlist.m3u8`;
                    if (jobs[jobId].thumbUrl) jobs[jobId].thumbUrl = `${baseUrl}/thumb.jpg`;
                    fs.rm(streamDir, { recursive: true, force: true }, () => {});
                }
                jobs[jobId].status = 'done';
                console.log(`[Job ${jobId}] Fully done ✅`);
            } else {
                if (jobs[jobId].status !== 'live') {
                    jobs[jobId].status = 'error';
                    jobs[jobId].error  = `FFmpeg exited with code ${code}`;
                } else {
                    jobs[jobId].status = 'done'; // partial but playable
                }
            }
        });

        proc.on('error', err => {
            segWatcher.close();
            fs.rm(destPath, { force: true }, () => {});
            jobs[jobId].status = 'error';
            jobs[jobId].error  = err.message;
        });

    } catch(err) {
        fs.rm(destPath, { force: true }, () => {});
        jobs[jobId].status = 'error';
        jobs[jobId].error  = err.message;
        console.error(`[Job ${jobId}] Failed:`, err.message);
    }
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

        // FIXED GHOST CLONE LOGIC: Scan the entire user array, including disconnected entries waiting out their timer
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

            // Purge the old socket profile placeholder
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

    // --- HOST DELEGATION LOGIC ---
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

    // --- MEDIA SYNC LOGIC ---
    socket.on('change_video', (data) => {
        const room = rooms[data.roomId];
        const user = room?.users.find(u => u.socketId === socket.id);
        if (room && user && (user.isHost || user.isCoHost)) {
            room.currentVideo = { src: data.src, name: data.name, index: data.index, time: 0, state: 1 };
            io.to(data.roomId).emit('load_video', room.currentVideo);
        }
    });

    socket.on('update_playlist', (data) => {
        const room = rooms[data.roomId];
        const user = room?.users.find(u => u.socketId === socket.id);
        if (room && user && (user.isHost || user.isCoHost)) {
            room.playlist = data.playlist;
            socket.to(data.roomId).emit('sync_playlist', room.playlist);
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

    // HEARTBEAT SYNC
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

    // --- CHAT & VOICE ---
    socket.on('chat_message', (data) => { if (rooms[data.roomId]) io.to(data.roomId).emit('chat_message', data); });
    socket.on('voice_join', (data) => { socket.to(data.roomId).emit('voice_user_joined', { socketId: socket.id }); });
    socket.on('webrtc_offer', (data) => { io.to(data.target).emit('webrtc_offer', { sender: socket.id, sdp: data.sdp }); });
    socket.on('webrtc_answer', (data) => { io.to(data.target).emit('webrtc_answer', { sender: socket.id, sdp: data.sdp }); });
    socket.on('webrtc_ice', (data) => { io.to(data.target).emit('webrtc_ice', { sender: socket.id, candidate: data.candidate }); });

    // --- NON-BLOCKING DISCONNECT LOGIC ---
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
server.listen(PORT, () => { console.log(`✅ SyncTube Server v33 running on port ${PORT}`); });
