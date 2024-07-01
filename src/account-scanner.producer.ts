import {
    Address,
    ExchangeWrapper,
    getMarketPda,
    MarginAccountWrapper,
    Market,
    MarketMap,
    MarketWrapper,
    ParclV3Sdk,
    PriceFeedMap,
    ProgramAccount,
} from "@parcl-oss/v3-sdk";
import {Connection, Keypair, PublicKey} from "@solana/web3.js";
import {Producer} from 'kafkajs';
import {KafkaTopics} from "./constants";

type RunProducerParams = {
    sdk: ParclV3Sdk;
    connection: Connection;
    interval: number;
    exchangeAddress: Address;
    liquidatorSigner: Keypair;
    producer: Producer;
};

export async function runProducer({
                               sdk,
                               interval,
                               exchangeAddress,
                               producer
                           }: RunProducerParams): Promise<void> {
    let firstRun = true;
    // eslint-disable-next-line no-constant-condition
    while (true) {
        if (firstRun) {
            firstRun = false;
        } else {
            await new Promise((resolve) => setTimeout(resolve, interval * 1000));
        }
        const exchange = await sdk.accountFetcher.getExchange(exchangeAddress);
        if (exchange === undefined) {
            throw new Error("Invalid exchange address");
        }
        const allMarketAddresses: PublicKey[] = [];
        for (const marketId of exchange.marketIds) {
            if (marketId === 0) {
                continue;
            }
            const [market] = getMarketPda(exchangeAddress, marketId);
            allMarketAddresses.push(market);
        }
        const allMarkets = await sdk.accountFetcher.getMarkets(allMarketAddresses);
        const [[markets, priceFeeds], allMarginAccounts] = await Promise.all([
            getMarketMapAndPriceFeedMap(sdk, allMarkets),
            sdk.accountFetcher.getAllMarginAccounts(),
        ]);
        console.log(`Fetched ${allMarginAccounts.length} margin accounts`);
        for (const rawMarginAccount of allMarginAccounts) {
            const marginAccount = new MarginAccountWrapper(
                rawMarginAccount.account,
                rawMarginAccount.address
            );
            const margins = marginAccount.getAccountMargins(
                new ExchangeWrapper(exchange),
                markets,
                priceFeeds,
                Math.floor(Date.now() / 1000)
            );
            if (margins.canLiquidate()) {
                await producer.send({
                    topic: KafkaTopics.ACCOUNT_LIQUIDATION,
                    messages: [
                        { value: JSON.stringify({ marginAccount: rawMarginAccount.address, exchange: rawMarginAccount.account.exchange, owner: rawMarginAccount.account.owner }) }
                    ]
                });
            }
        }
    }
}

async function getMarketMapAndPriceFeedMap(
    sdk: ParclV3Sdk,
    allMarkets: (ProgramAccount<Market> | undefined)[]
): Promise<[MarketMap, PriceFeedMap]> {
    const markets: MarketMap = {};
    for (const market of allMarkets) {
        if (market === undefined) {
            continue;
        }
        markets[market.account.id] = new MarketWrapper(market.account, market.address);
    }
    const allPriceFeedAddresses = (allMarkets as ProgramAccount<Market>[]).map(
        (market) => market.account.priceFeed
    );
    const allPriceFeeds = await sdk.accountFetcher.getPythPriceFeeds(allPriceFeedAddresses);
    const priceFeeds: PriceFeedMap = {};
    for (let i = 0; i < allPriceFeeds.length; i++) {
        const priceFeed = allPriceFeeds[i];
        if (priceFeed === undefined) {
            continue;
        }
        priceFeeds[allPriceFeedAddresses[i]] = priceFeed;
    }
    return [markets, priceFeeds];
}
