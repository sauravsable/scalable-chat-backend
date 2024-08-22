import express from 'express';
import dotenv from 'dotenv';
import cors from 'cors';
import { Server } from 'socket.io';
import Redis from 'ioredis';
import { Kafka, logLevel } from 'kafkajs';
import mongoose from 'mongoose';

dotenv.config();

const PORT = process.env.PORT || 8000;
const app = express();

app.use(cors({
    origin: true,
    credentials: true,
}));

const pub = new Redis({
    host: process.env.REDIS_HOSTNAME,
    port: process.env.REDIS_PORT,
    username: "default",
    password: process.env.REDIS_PASSWORD,
});

const MONGO_URI = process.env.MONGO_URI;

await mongoose.connect(MONGO_URI)
    .then((data) => { console.log(`Mongodb connected with Server: ${data.connection.host}`) })
    .catch((err) => { console.log(err) })

const MessageSchema = new mongoose.Schema({
    room: String,
    message: String,
    timestamp: { type: Date, default: Date.now },
});

const Message = mongoose.model('messages', MessageSchema);

// Kafka setup
const kafka = new Kafka({
    clientId: 'chat-app',
    brokers: ["10.22.4.126:9092"],
    logLevel: logLevel.INFO,
});

const admin = kafka.admin();
const producer = kafka.producer();

const server = app.listen(PORT, async () => {
    console.log(`Server is running on ${PORT}`);
    await admin.connect();
    await producer.connect();
});

const io = new Server(server, {
    cors: {
        origin: "http://localhost:3000",
        credentials: true,
    },
});

const channelSubscribers = new Map();
const kafkaConsumers = new Map();

io.on("connection", (socket) => {
    console.log("Socket connected:", socket.id);

    socket.on('joinRoom', async ({ username, room }) => {
        socket.join(room);
        socket.broadcast.to(room).emit('join-room-message', `${username} has joined the chat`);
        console.log(`${username} joined the room: ${room}`);

        const topics = await admin.listTopics();
        if (!topics.includes(room)) {
            await admin.createTopics({
                topics: [{ topic: room }],
            });
            console.log(`Created Kafka topic: ${room}`);
        }

        if (!channelSubscribers.has(room)) {
            const roomSub = new Redis({
                host: process.env.REDIS_HOSTNAME,
                port: process.env.REDIS_PORT,
                username: "default",
                password: process.env.REDIS_PASSWORD,
            });

            roomSub.subscribe(room, (err, count) => {
                if (err) {
                    console.error("Failed to subscribe:", err.message);
                } else {
                    console.log(`Subscribed to ${count} channel(s). Listening to updates on the ${room} channel.`);
                }
            });

            roomSub.on("message", (channel, message) => {
                if (channelSubscribers.has(channel)) {
                    console.log(`New message received in room ${channel}: ${message}`);
                    io.to(channel).emit("message", JSON.parse(message));
                }
            });

            channelSubscribers.set(room, roomSub);
        }

        if (!kafkaConsumers.has(room)) {
            const consumer = kafka.consumer({ groupId: `chat-group-${room}` });
            await consumer.connect();
            await consumer.subscribe({ topic: room });

            consumer.run({
                eachMessage: async ({ topic,message }) => {
                    const parsedMessage = JSON.parse(message.value.toString());
                    console.log(`Processing message from Kafka for room ${topic}: ${parsedMessage.message}`);

                    const newMessage = new Message({
                        room: parsedMessage.room,
                        message: parsedMessage.message,
                    });
                    await newMessage.save();
                    console.log("Message saved to MongoDB");
                },
            });

            kafkaConsumers.set(room, consumer);
        }
    });

    socket.on("event:message", async ({ room, message }) => {
        console.log("room", room);
        console.log("New Message Received:", message);

        await pub.publish(room, JSON.stringify({ message }));

        await producer.send({
            topic: room,
            messages: [
                { value: JSON.stringify({ room, message }) },
            ],
        });
    });

    socket.on("disconnect", () => {
        console.log("Socket disconnected:", socket.id);
    });
});
