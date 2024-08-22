import mongoose from 'mongoose';
import dotenv from 'dotenv'
dotenv.config();

const MONGO_URI = process.env.MONGO_URI;

const connectDataBase = async ()=>{
    await mongoose.connect(MONGO_URI)
    .then((data)=>{console.log(`Mongodb connected with Server: ${data.connection.host}`)})
    .catch((err)=>{console.log(err)})
}

export default connectDataBase;