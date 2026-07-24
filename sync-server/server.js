const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Serve static frontend files
app.use(express.static('public'));

// In-memory state for rooms
const rooms = {};

function getPublicRooms() {
    return Object.keys(rooms)
        .map(roomId => ({ roomId, count: rooms[roomId].users.length }))
        .filter(r => r.count > 0);
}

io.on('connection', (socket) => {
    let currentRoom = null;
    let currentUser = null;

    // Send active public rooms to connecting user
    socket.emit('active-rooms', getPublicRooms());

    socket.on('join-room', ({ roomId, username }) => {
        socket.join(roomId);
        currentRoom = roomId;
        currentUser = username;

        if (!rooms[roomId]) {
            rooms[roomId] = {
                users: [],
                queue: [],
                voice: [],
                host: socket.id,
                playback: { type: 'none', url: null, videoId: null, time: 0, state: 'paused' }
            };
        }

        const isHost = rooms[roomId].host === socket.id;
        rooms[roomId].users.push({ id: socket.id, username, isHost });

        // Notify everyone about updated members list
        io.to(roomId).emit('members-update', rooms[roomId].users);
        
        // Send room details to the joined user
        socket.emit('room-info', {
            count: rooms[roomId].users.length,
            isAdmin: isHost,
            queue: rooms[roomId].queue
        });

        // Sync current video state to the new user
        socket.emit('initial-sync', {
            sourceType: rooms[roomId].playback.type,
            rawUrl: rooms[roomId].playback.url,
            videoId: rooms[roomId].playback.videoId,
            time: rooms[roomId].playback.time,
            playbackState: rooms[roomId].playback.state
        });

        // Sync voice chat participants
        socket.emit('voice-participants', rooms[roomId].voice);

        // Announce join
        socket.to(roomId).emit('chat-message', {
            type: 'system',
            text: `${username} joined the party`
        });
        
        // Update public rooms list for everyone
        io.emit('active-rooms', getPublicRooms());
    });

    // --- NEW: Transfer Host Feature ---
    socket.on('transfer-host', ({ roomId, targetId }) => {
        if (rooms[roomId] && rooms[roomId].host === socket.id) {
            rooms[roomId].host = targetId;
            rooms[roomId].users.forEach(u => u.isHost = (u.id === targetId));
            
            io.to(roomId).emit('members-update', rooms[roomId].users);
            
            const newHost = rooms[roomId].users.find(u => u.id === targetId);
            if (newHost) {
                io.to(targetId).emit('room-info', { count: rooms[roomId].users.length, isAdmin: true, queue: rooms[roomId].queue });
                socket.emit('room-info', { count: rooms[roomId].users.length, isAdmin: false, queue: rooms[roomId].queue });
                io.to(roomId).emit('chat-message', { type: 'system', text: `${newHost.username} is now the Host 👑` });
            }
        }
    });

    // --- FIXED: Persistent Playlist ---
    socket.on('add-to-queue', ({ roomId, video }) => {
        if (rooms[roomId]) {
            // Check if it's already in queue to prevent duplicates
            const exists = rooms[roomId].queue.find(v => (v.id && v.id === video.id) || (v.url && v.url === video.url));
            if (!exists) {
                rooms[roomId].queue.push(video);
                io.to(roomId).emit('queue-update', rooms[roomId].queue);
            }
        }
    });

    socket.on('clear-queue', (roomId) => {
        if (rooms[roomId] && rooms[roomId].host === socket.id) {
            rooms[roomId].queue = [];
            io.to(roomId).emit('queue-update', rooms[roomId].queue);
        }
    });

    socket.on('pop-queue', (roomId) => {
        if (rooms[roomId] && rooms[roomId].host === socket.id) {
            rooms[roomId].queue.shift();
            io.to(roomId).emit('queue-update', rooms[roomId].queue);
        }
    });

    // --- Universal Video Sync ---
    socket.on('video-command', (data) => {
        const { roomId, type, time, url, videoId } = data;
        if (!rooms[roomId]) return;

        // Track state so late joiners see what's playing
        if (type === 'change-raw') {
            rooms[roomId].playback = { type: 'raw', url: url, time: 0, state: 'playing' };
        } else if (type === 'change') {
            rooms[roomId].playback = { type: 'youtube', videoId: videoId, time: 0, state: 'playing' };
        } else if (type === 'play') {
            rooms[roomId].playback.state = 'playing';
        } else if (type === 'pause') {
            rooms[roomId].playback.state = 'paused';
        } else if (type === 'seek') {
            rooms[roomId].playback.time = time;
            rooms[roomId].playback.state = 'playing';
        }

        socket.to(roomId).emit('sync-video', data);
    });

    socket.on('chat-message', (data) => {
        data.timestamp = Date.now();
        io.to(data.roomId).emit('chat-message', data);
    });

    socket.on('typing', (data) => socket.to(data.roomId).emit('typing', data));

    // --- Voice Chat Tracker ---
    socket.on('voice-join', ({ roomId, user }) => {
        if(rooms[roomId]) {
            if(!rooms[roomId].voice.includes(user)) rooms[roomId].voice.push(user);
            io.to(roomId).emit('voice-participants', rooms[roomId].voice);
        }
    });
    socket.on('voice-leave', ({ roomId, user }) => {
        if(rooms[roomId]) {
            rooms[roomId].voice = rooms[roomId].voice.filter(u => u !== user);
            io.to(roomId).emit('voice-participants', rooms[roomId].voice);
        }
    });
    socket.on('voice-chunk', (data) => socket.to(data.roomId).emit('voice-chunk', data));
    socket.on('voice-speaking', (data) => socket.to(data.roomId).emit('voice-speaking', data));

    socket.on('disconnect', () => {
        if (currentRoom && rooms[currentRoom]) {
            const room = rooms[currentRoom];
            room.users = room.users.filter(u => u.id !== socket.id);
            room.voice = room.voice.filter(u => u !== currentUser); // Remove from voice

            if (room.users.length === 0) {
                delete rooms[currentRoom];
            } else {
                if (room.host === socket.id) {
                    room.host = room.users[0].id;
                    room.users[0].isHost = true;
                    io.to(room.host).emit('room-info', { count: room.users.length, isAdmin: true, queue: room.queue });
                    io.to(currentRoom).emit('chat-message', { type: 'system', text: `${room.users[0].username} is now the Host 👑` });
                }
                io.to(currentRoom).emit('members-update', room.users);
                io.to(currentRoom).emit('voice-participants', room.voice);
                io.to(currentRoom).emit('room-info', { count: room.users.length, isAdmin: room.host === socket.id, queue: room.queue });
                io.to(currentRoom).emit('chat-message', { type: 'system', text: `${currentUser} left the party` });
            }
            io.emit('active-rooms', getPublicRooms());
        }
    });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
    console.log(`🚀 SyncTube Pro Backend running on port ${PORT}`);
});
