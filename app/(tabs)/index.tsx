import React, { useState, useCallback, useEffect, useRef } from 'react';
import { StyleSheet, View, Text, Button, TextInput, FlatList, KeyboardAvoidingView, Platform } from 'react-native';
import YoutubePlayer from 'react-native-youtube-iframe';
import io from 'socket.io-client';

// Connected directly to your live Render cloud server
const socket = io('https://watch-party-backend-wzj6.onrender.com');

export default function HomeScreen() {
  const [playing, setPlaying] = useState(false);
  const [videoId, setVideoId] = useState("dQw4w9WgXcQ");
  const [videoInput, setVideoInput] = useState("");
  
  // Chat States
  const [messages, setMessages] = useState([]);
  const [chatInput, setChatInput] = useState("");
  
  // Ref to prevent infinite sync loops
  const isRemoteUpdate = useRef(false);

  useEffect(() => {
    // Listen for incoming video sync commands
    socket.on('sync-video', (data) => {
      isRemoteUpdate.current = true;
      if (data.type === 'playState') {
        setPlaying(data.playing);
      } else if (data.type === 'videoChange') {
        setVideoId(data.videoId);
      }
    });

    // Listen for incoming chat messages
    socket.on('chat-message', (msg) => {
      setMessages((prev) => [...prev, msg]);
    });

    return () => {
      socket.off('sync-video');
      socket.off('chat-message');
    };
  }, []);

  const onStateChange = useCallback((state) => {
    if (state === "ended") {
      setPlaying(false);
    }
  }, []);

  const togglePlaying = () => {
    const nextState = !playing;
    setPlaying(nextState);
    
    // Only broadcast if the user physically pressed the button
    if (!isRemoteUpdate.current) {
      socket.emit('video-command', { type: 'playState', playing: nextState });
    }
    isRemoteUpdate.current = false;
  };

  const handleLoadVideo = () => {
    if (videoInput.trim() !== "") {
      setVideoId(videoInput);
      socket.emit('video-command', { type: 'videoChange', videoId: videoInput });
      setVideoInput("");
    }
  };

  const sendMessage = () => {
    if (chatInput.trim() !== "") {
      // Create a unique ID for the message using the current timestamp
      const newMsg = { id: Date.now().toString(), text: chatInput };
      socket.emit('chat-message', newMsg);
      setChatInput("");
    }
  };

  return (
    <KeyboardAvoidingView 
      style={styles.container} 
      behavior={Platform.OS === 'ios' ? 'padding' : undefined}
    >
      <Text style={styles.title}>Watch Party Room</Text>
      
      <YoutubePlayer 
        height={250} 
        play={playing} 
        videoId={videoId} 
        onChangeState={onStateChange} 
      />

      <View style={styles.controlsContainer}>
        <Button 
          title={playing ? "⏸ Pause Video" : "▶️ Play Video"} 
          onPress={togglePlaying} 
          color="#d9534f" 
        />
      </View>

      <View style={styles.inputContainer}>
        <TextInput 
          style={styles.input} 
          placeholder="Paste Video ID..." 
          value={videoInput} 
          onChangeText={setVideoInput} 
        />
        <Button title="Load" onPress={handleLoadVideo} />
      </View>

      {/* Chat Interface */}
      <View style={styles.chatSection}>
        <Text style={styles.chatHeader}>Live Chat</Text>
        
        <FlatList
          data={messages}
          keyExtractor={(item) => item.id}
          renderItem={({ item }) => <Text style={styles.chatMessage}>💬 {item.text}</Text>}
          style={styles.chatList}
        />
        
        <View style={styles.chatInputContainer}>
          <TextInput 
            style={styles.chatInputText} 
            placeholder="Type a message..." 
            value={chatInput} 
            onChangeText={setChatInput} 
          />
          <Button title="Send" onPress={sendMessage} color="#5cb85c" />
        </View>
      </View>
    </KeyboardAvoidingView>
  );
}

const styles = StyleSheet.create({
  container: { 
    flex: 1, 
    backgroundColor: '#f5f5f5', 
    paddingTop: 40 
  },
  title: { 
    fontSize: 24, 
    fontWeight: 'bold', 
    textAlign: 'center', 
    marginBottom: 10 
  },
  controlsContainer: { 
    marginVertical: 5, 
    paddingHorizontal: 50 
  },
  inputContainer: { 
    flexDirection: 'row', 
    paddingHorizontal: 20, 
    marginBottom: 10 
  },
  input: { 
    flex: 1, 
    height: 40, 
    borderColor: 'gray', 
    borderWidth: 1, 
    marginRight: 10, 
    paddingHorizontal: 10, 
    backgroundColor: 'white' 
  },
  
  // Chat Styles
  chatSection: { 
    flex: 1, 
    borderTopWidth: 1, 
    borderColor: '#ddd', 
    backgroundColor: '#fff', 
    paddingTop: 10 
  },
  chatHeader: { 
    fontSize: 18, 
    fontWeight: 'bold', 
    paddingHorizontal: 20, 
    marginBottom: 5 
  },
  chatList: { 
    flex: 1, 
    paddingHorizontal: 20 
  },
  chatMessage: { 
    fontSize: 16, 
    marginVertical: 4, 
    padding: 8, 
    backgroundColor: '#f1f1f1', 
    borderRadius: 8 
  },
  chatInputContainer: { 
    flexDirection: 'row', 
    padding: 10, 
    borderTopWidth: 1, 
    borderColor: '#ddd' 
  },
  chatInputText: { 
    flex: 1, 
    height: 40, 
    borderColor: 'gray', 
    borderWidth: 1, 
    marginRight: 10, 
    paddingHorizontal: 10, 
    borderRadius: 20 
  }
});
