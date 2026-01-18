const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  transports: ['websocket', 'polling']
});

// Middleware untuk logging
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.url}`);
  next();
});

// Serve static files
app.use(express.static(path.join(__dirname, 'public')));

// API Routes
app.get('/api/streams', (req, res) => {
  res.json({
    activeStreams: Array.from(activeStreams.values()),
    totalViewers: viewerCount
  });
});

app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    uptime: process.uptime(),
    timestamp: new Date().toISOString()
  });
});

// Data store
const activeStreams = new Map();
let viewerCount = 0;

// Socket.io connection handling
io.on('connection', (socket) => {
  console.log('New client connected:', socket.id);
  
  // Set user type (broadcaster or viewer)
  socket.on('set-user-type', (data) => {
    socket.userType = data.type;
    socket.username = data.username || `User_${socket.id.substring(0, 6)}`;
    
    if (data.type === 'broadcaster') {
      activeStreams.set(socket.id, {
        id: socket.id,
        username: socket.username,
        startedAt: new Date().toISOString(),
        viewers: 0
      });
      socket.broadcast.emit('new-stream', activeStreams.get(socket.id));
    }
    
    console.log(`${socket.username} connected as ${data.type}`);
  });

  // WebRTC Signaling
  socket.on('offer', (data) => {
    socket.to(data.target).emit('offer', {
      sdp: data.sdp,
      sender: socket.id,
      username: socket.username
    });
  });

  socket.on('answer', (data) => {
    socket.to(data.target).emit('answer', {
      sdp: data.sdp,
      sender: socket.id
    });
  });

  socket.on('ice-candidate', (data) => {
    socket.to(data.target).emit('ice-candidate', {
      candidate: data.candidate,
      sender: socket.id
    });
  });

  // Stream management
  socket.on('stream-started', (streamData) => {
    console.log(`Stream started by ${socket.username}`);
    io.emit('stream-update', {
      type: 'started',
      streamId: socket.id,
      username: socket.username,
      timestamp: new Date().toISOString()
    });
  });

  socket.on('stream-stopped', () => {
    console.log(`Stream stopped by ${socket.username}`);
    activeStreams.delete(socket.id);
    io.emit('stream-update', {
      type: 'stopped',
      streamId: socket.id,
      timestamp: new Date().toISOString()
    });
  });

  socket.on('viewer-joined', (streamId) => {
    viewerCount++;
    const stream = activeStreams.get(streamId);
    if (stream) {
      stream.viewers++;
      io.emit('viewer-count-update', {
        streamId,
        viewers: stream.viewers
      });
    }
  });

  socket.on('viewer-left', (streamId) => {
    viewerCount = Math.max(0, viewerCount - 1);
    const stream = activeStreams.get(streamId);
    if (stream) {
      stream.viewers = Math.max(0, stream.viewers - 1);
      io.emit('viewer-count-update', {
        streamId,
        viewers: stream.viewers
      });
    }
  });

  // Chat functionality
  socket.on('chat-message', (message) => {
    const chatMessage = {
      id: Date.now().toString(),
      username: socket.username,
      message: message.text,
      timestamp: new Date().toISOString(),
      type: message.type || 'message'
    };
    
    if (message.streamId) {
      socket.to(message.streamId).emit('chat-message', chatMessage);
    } else {
      socket.broadcast.emit('chat-message', chatMessage);
    }
  });

  // Get active streams
  socket.on('get-streams', () => {
    socket.emit('active-streams', Array.from(activeStreams.values()));
  });

  // Disconnect handling
  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
    
    if (socket.userType === 'broadcaster') {
      activeStreams.delete(socket.id);
      io.emit('stream-update', {
        type: 'stopped',
        streamId: socket.id,
        timestamp: new Date().toISOString()
      });
    }
    
    // Update viewer count
    io.emit('user-disconnected', {
      userId: socket.id,
      username: socket.username
    });
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Server error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// 404 handler
app.use((req, res) => {
  res.status(404).sendFile(path.join(__dirname, 'public', '404.html'));
});

const PORT = process.env.PORT || 6778;
server.listen(PORT, () => {
  console.log(`
  ========================================
  🚀 Live Streaming App Berhasil Dijalankan!
  ========================================
  
  📍 Local: http://localhost:${PORT}
  🌐 Network: http://${getLocalIP()}:${PORT}
  
  📊 Endpoints:
  - Home: http://localhost:${PORT}
  - Broadcaster: http://localhost:${PORT}/broadcaster.html
  - Viewer: http://localhost:${PORT}/viewer.html
  - API Health: http://localhost:${PORT}/api/health
  - API Streams: http://localhost:${PORT}/api/streams
  
  ========================================
  `);
});

// Helper function to get local IP
function getLocalIP() {
  const interfaces = require('os').networkInterfaces();
  for (const interfaceName in interfaces) {
    for (const iface of interfaces[interfaceName]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return 'localhost';
}
