import {getExchangePda, ParclV3Sdk} from "@parcl-oss/v3-sdk";
import bs58 from "bs58";
import {Kafka} from "kafkajs";
import {runProducer} from "../account-scanner.producer";
import {Commitment, Connection, Keypair} from "@solana/web3.js";
import * as dotenv from "dotenv";

dotenv.config();

(async function main() {
    console.log("Starting producer");
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

    const [exchangeAddress] = getExchangePda(0);
    const liquidatorSigner = Keypair.fromSecretKey(bs58.decode(process.env.PRIVATE_KEY));
    const interval = parseInt(process.env.INTERVAL ?? "300");
    const commitment = process.env.COMMITMENT as Commitment | undefined;
    const sdk = new ParclV3Sdk({ rpcUrl: process.env.RPC_URL, commitment });
    const connection = new Connection(process.env.RPC_URL, commitment);

    const kafka = new Kafka({
        clientId: 'liquidator-producer',
        brokers: [process.env.KAFKA_BROKER]
    });
    const producer = kafka.producer();
    await producer.connect();

    await runProducer({
        sdk,
        connection,
        interval,
        exchangeAddress,
        liquidatorSigner,
        producer
    });
})();
