const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" }
});

io.on('connection', (socket) => {
  console.log('🟢 A user connected! ID:', socket.id);

  socket.on('video-command', (data) => {
    socket.broadcast.emit('sync-video', data);
  });

  socket.on('chat-message', (messageData) => {
    io.emit('chat-message', messageData);
  });

  socket.on('disconnect', () => {
    console.log('🔴 User disconnected:', socket.id);
  });
});

// Render assigns a dynamic PORT, or defaults to 3000 for local testing
const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Watch Party Server running on port ${PORT}`);
});
