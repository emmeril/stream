const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const path = require('path');

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
  console.log(`${new Date().toLocaleTimeString()} - ${req.method} ${req.url}`);
  next();
});

// Serve static files
app.use(express.static(path.join(__dirname, 'public')));

// API Routes
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    version: '1.0.0'
  });
});

app.get('/api/stats', (req, res) => {
  res.json({
    totalBroadcasters: broadcasters.size,
    totalViewers: viewers.size,
    activeConnections: io.engine.clientsCount
  });
});

// Data storage
const broadcasters = new Map(); // Map<socketId, broadcasterInfo>
const viewers = new Map(); // Map<socketId, viewerInfo>
const peerConnections = new Map(); // Map<viewerId, broadcasterId>

// Socket.io Connection Handling
io.on('connection', (socket) => {
  console.log('✅ Client connected:', socket.id);

  // Set user type
  socket.on('set-user-type', (data) => {
    socket.userType = data.type;
    socket.username = data.username || `${data.type}_${socket.id.substring(0, 6)}`;
    
    if (data.type === 'broadcaster') {
      broadcasters.set(socket.id, {
        id: socket.id,
        username: socket.username,
        streamId: data.streamId,
        startedAt: new Date().toISOString(),
        viewers: new Set()
      });
      
      console.log(`🎥 Broadcaster registered: ${socket.username} (${socket.id})`);
      
      // Broadcast to all viewers that new stream is available
      io.emit('stream-list-updated', Array.from(broadcasters.values()).map(b => ({
        id: b.id,
        username: b.username,
        streamId: b.streamId,
        viewers: b.viewers.size
      })));
    } 
    else if (data.type === 'viewer') {
      viewers.set(socket.id, {
        id: socket.id,
        username: socket.username,
        watching: null
      });
      
      console.log(`👁️ Viewer registered: ${socket.username} (${socket.id})`);
    }
  });

  // ==============================
  // WEBRTC SIGNALING - FIXED VERSION
  // ==============================
  
  // 1. Broadcaster announces itself
  socket.on('broadcaster-announce', (streamId) => {
    console.log(`📢 Broadcaster ${socket.id} announced stream: ${streamId}`);
    socket.streamId = streamId;
    socket.broadcast.emit('broadcaster-available', {
      broadcasterId: socket.id,
      streamId: streamId,
      username: socket.username
    });
  });

  // 2. Viewer requests to watch a broadcaster
  socket.on('watch-stream', (broadcasterId) => {
    console.log(`👁️ Viewer ${socket.id} wants to watch ${broadcasterId}`);
    
    const broadcaster = broadcasters.get(broadcasterId);
    if (broadcaster) {
      // Store the connection
      peerConnections.set(socket.id, broadcasterId);
      broadcaster.viewers.add(socket.id);
      
      // Notify broadcaster about new viewer
      socket.to(broadcasterId).emit('viewer-request', {
        viewerId: socket.id,
        viewerName: socket.username
      });
      
      // Send viewer count update
      io.to(broadcasterId).emit('viewer-count-update', {
        count: broadcaster.viewers.size
      });
      
      // Send stream list update to all
      io.emit('stream-list-updated', Array.from(broadcasters.values()).map(b => ({
        id: b.id,
        username: b.username,
        streamId: b.streamId,
        viewers: b.viewers.size
      })));
    }
  });

  // 3. WebRTC Offer (from broadcaster to viewer)
  socket.on('webrtc-offer', (data) => {
    console.log(`📤 Offer from ${socket.id} to ${data.target}`);
    socket.to(data.target).emit('webrtc-offer', {
      sdp: data.sdp,
      sender: socket.id,
      senderName: socket.username
    });
  });

  // 4. WebRTC Answer (from viewer to broadcaster)
  socket.on('webrtc-answer', (data) => {
    console.log(`📥 Answer from ${socket.id} to ${data.target}`);
    socket.to(data.target).emit('webrtc-answer', {
      sdp: data.sdp,
      sender: socket.id
    });
  });

  // 5. ICE Candidates exchange
  socket.on('ice-candidate', (data) => {
    socket.to(data.target).emit('ice-candidate', {
      candidate: data.candidate,
      sender: socket.id
    });
  });

  // 6. Viewer leaving stream
  socket.on('leave-stream', (broadcasterId) => {
    console.log(`🚪 Viewer ${socket.id} leaving ${broadcasterId}`);
    
    const broadcaster = broadcasters.get(broadcasterId);
    if (broadcaster) {
      broadcaster.viewers.delete(socket.id);
      peerConnections.delete(socket.id);
      
      // Update viewer count
      io.to(broadcasterId).emit('viewer-count-update', {
        count: broadcaster.viewers.size
      });
      
      // Update stream list
      io.emit('stream-list-updated', Array.from(broadcasters.values()).map(b => ({
        id: b.id,
        username: b.username,
        streamId: b.streamId,
        viewers: b.viewers.size
      })));
    }
  });

  // 7. Broadcaster stopping stream
  socket.on('stop-stream', () => {
    console.log(`⏹️ Broadcaster ${socket.id} stopping stream`);
    
    const broadcaster = broadcasters.get(socket.id);
    if (broadcaster) {
      // Notify all viewers
      broadcaster.viewers.forEach(viewerId => {
        io.to(viewerId).emit('stream-ended', {
          broadcasterId: socket.id,
          message: 'Stream telah berakhir'
        });
      });
      
      // Clean up
      broadcasters.delete(socket.id);
      io.emit('stream-list-updated', Array.from(broadcasters.values()).map(b => ({
        id: b.id,
        username: b.username,
        streamId: b.streamId,
        viewers: b.viewers.size
      })));
    }
  });

  // 8. Chat messages
  socket.on('chat-message', (data) => {
    const message = {
      id: Date.now(),
      username: socket.username,
      message: data.message,
      timestamp: new Date().toISOString(),
      type: data.type || 'message',
      streamId: data.streamId
    };
    
    if (data.streamId) {
      // Send to everyone in the stream (broadcaster + viewers)
      const broadcaster = broadcasters.get(data.streamId);
      if (broadcaster) {
        // Send to broadcaster
        io.to(data.streamId).emit('chat-message', message);
        
        // Send to all viewers of this stream
        broadcaster.viewers.forEach(viewerId => {
          io.to(viewerId).emit('chat-message', message);
        });
      }
    } else {
      // General chat (if any)
      socket.broadcast.emit('chat-message', message);
    }
  });

  // 9. Get active streams
  socket.on('get-streams', () => {
    const streams = Array.from(broadcasters.values()).map(b => ({
      id: b.id,
      username: b.username,
      streamId: b.streamId,
      viewers: b.viewers.size,
      startedAt: b.startedAt
    }));
    
    socket.emit('active-streams', streams);
  });

  // 10. Get stream info
  socket.on('get-stream-info', (broadcasterId) => {
    const broadcaster = broadcasters.get(broadcasterId);
    if (broadcaster) {
      socket.emit('stream-info', {
        id: broadcaster.id,
        username: broadcaster.username,
        streamId: broadcaster.streamId,
        viewers: broadcaster.viewers.size,
        startedAt: broadcaster.startedAt
      });
    }
  });

  // Disconnect handling
  socket.on('disconnect', () => {
    console.log(`❌ Client disconnected: ${socket.id} (${socket.userType})`);
    
    if (socket.userType === 'broadcaster') {
      const broadcaster = broadcasters.get(socket.id);
      if (broadcaster) {
        // Notify all viewers
        broadcaster.viewers.forEach(viewerId => {
          io.to(viewerId).emit('stream-ended', {
            broadcasterId: socket.id,
            message: 'Broadcaster terputus'
          });
        });
        
        // Remove from broadcasters
        broadcasters.delete(socket.id);
        
        // Update stream list
        io.emit('stream-list-updated', Array.from(broadcasters.values()).map(b => ({
          id: b.id,
          username: b.username,
          streamId: b.streamId,
          viewers: b.viewers.size
        })));
      }
    } 
    else if (socket.userType === 'viewer') {
      // Remove viewer from any broadcaster's viewer list
      broadcasters.forEach(broadcaster => {
        if (broadcaster.viewers.has(socket.id)) {
          broadcaster.viewers.delete(socket.id);
          
          // Update viewer count
          io.to(broadcaster.id).emit('viewer-count-update', {
            count: broadcaster.viewers.size
          });
          
          // Update stream list
          io.emit('stream-list-updated', Array.from(broadcasters.values()).map(b => ({
            id: b.id,
            username: b.username,
            streamId: b.streamId,
            viewers: b.viewers.size
          })));
        }
      });
      
      // Remove from viewers map
      viewers.delete(socket.id);
    }
    
    // Remove peer connections
    peerConnections.delete(socket.id);
  });

  // Error handling
  socket.on('error', (error) => {
    console.error('Socket error:', error);
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

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
  console.log(`
  ============================================
  🚀 LIVE STREAMING APP BERHASIL DIJALANKAN!
  ============================================
  
  📍 Local:    http://localhost:${PORT}
  🌐 Network:  http://${getLocalIP()}:${PORT}
  
  📊 Endpoints:
  - Home Page:      http://localhost:${PORT}
  - Broadcaster:    http://localhost:${PORT}/broadcaster.html
  - Viewer:         http://localhost:${PORT}/viewer.html
  - API Health:     http://localhost:${PORT}/api/health
  - API Stats:      http://localhost:${PORT}/api/stats
  
  ============================================
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
