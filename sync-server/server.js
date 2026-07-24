const { exec } = require('child_process');
const util = require('util');
const execAsync = util.promisify(exec);

// --- UPDATED ASYNC CONVERSION ROUTE ---
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

    // Reply instantly so the UI doesn't hang
    res.json({ status: 'queued', jobId });

    // Run probe and thumbnail generation asynchronously so it doesn't freeze the server
    (async () => {
        let numAudio = 1;
        let videoTitle = '';
        let videoDuration = 0;
        let audioLangs = [];

        try {
            const safeUrl = videoUrl.replace(/"/g, '\"');
            
            // 1. Async Probe
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
        } catch (e) {
            console.log('[Probe] ffprobe failed:', e.message);
        }

        jobs[jobId].title = videoTitle;
        jobs[jobId].duration = videoDuration;
        jobs[jobId].audioLangs = audioLangs;

        // 2. Async Thumbnail Generation
        const thumbPath = path.join(streamDir, 'thumb.jpg');
        try {
            await execAsync(
                `ffmpeg -y -ss 10 -i "${videoUrl.replace(/"/g,'\"')}" -frames:v 1 -q:v 2 -vf scale=320:-1 "${thumbPath}"`,
                { timeout: 25000 }
            );
            jobs[jobId].thumbUrl = `/public/${streamId}/thumb.jpg`;
        } catch (e) {
            console.log('[Thumb] Thumbnail generation failed:', e.message);
        }

        // 3. Build FFmpeg Arguments based on track count
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

            // Spawn parallel extra audio tracks
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
