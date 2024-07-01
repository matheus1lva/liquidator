import bs58 from "bs58";
import {ParclV3Sdk, translateAddress} from "@parcl-oss/v3-sdk";
import {Kafka} from "kafkajs";
import {runConsumer} from "../liquidator";
import {Commitment, Connection, Keypair} from "@solana/web3.js";
import * as dotenv from "dotenv";
import {KafkaTopics} from "../constants";

dotenv.config();

(async function main() {
    console.log("Starting consumer");
    if (process.env.RPC_URL === undefined) {
        throw new Error("Missing rpc url");
    }
    if (process.env.LIQUIDATOR_MARGIN_ACCOUNT === undefined) {
        throw new Error("Missing liquidator margin account");
    }
    if (process.env.PRIVATE_KEY === undefined) {
        throw new Error("Missing liquidator signer");
    }
    if (process.env.KAFKA_BROKER === undefined) {
        throw new Error("Missing Kafka broker");
    }

    const liquidatorSigner = Keypair.fromSecretKey(bs58.decode(process.env.PRIVATE_KEY));
    const commitment = process.env.COMMITMENT as Commitment | undefined;
    const sdk = new ParclV3Sdk({ rpcUrl: process.env.RPC_URL, commitment });
    const connection = new Connection(process.env.RPC_URL, commitment);

    const kafka = new Kafka({
        clientId: 'liquidator-consumer',
        brokers: [process.env.KAFKA_BROKER]
    });
    const consumer = kafka.consumer({ groupId: 'liquidators' });
    await consumer.connect();
    await consumer.subscribe({ topic: KafkaTopics.ACCOUNT_LIQUIDATION, fromBeginning: true });
    await runConsumer({
        sdk,
        connection,
        consumer,
        liquidatorSigner,
        liquidatorMarginAccount: translateAddress(process.env.LIQUIDATOR_MARGIN_ACCOUNT),
    });
})();