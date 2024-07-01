import {
  Address,
  ExchangeWrapper,
  getMarketPda,
  LiquidateAccounts,
  LiquidateParams,
  MarginAccount,
  MarginAccountWrapper,
  Market,
  MarketMap,
  MarketWrapper,
  ParclV3Sdk,
  PriceFeedMap,
  ProgramAccount,
} from "@parcl-oss/v3-sdk";
import {Connection, Keypair, PublicKey, sendAndConfirmTransaction, Signer} from "@solana/web3.js";
import * as dotenv from "dotenv";
import {EachMessagePayload, Kafka} from 'kafkajs';

dotenv.config();


type RunConsumerParams = {
  sdk: ParclV3Sdk;
  connection: Connection;
  consumer: ReturnType<Kafka['consumer']>;
  liquidatorSigner: Keypair;
  liquidatorMarginAccount: Address;
};


export async function runConsumer({
                             sdk,
                             connection,
                             consumer,
                             liquidatorSigner,
                             liquidatorMarginAccount,
                           }: RunConsumerParams): Promise<void> {
  await consumer.run({
    eachMessage: async ({  message }: EachMessagePayload) => {
      const { marginAccount, exchange, owner }: { marginAccount: string; exchange: string; owner: string } = JSON.parse(message?.value?.toString() ?? '{}');

      const rawMarginAccount = await sdk.accountFetcher.getMarginAccount(new PublicKey(marginAccount)) as ProgramAccount<MarginAccount> | undefined;
      if (!rawMarginAccount) {
        console.log(`Margin account ${marginAccount} not found`);
        return;
      }

      const marginAccountWrapper = new MarginAccountWrapper(rawMarginAccount.account, new PublicKey(marginAccount));

      if (marginAccountWrapper.inLiquidation()) {
        console.log(`Liquidating account already in liquidation (${marginAccountWrapper.address})`);
        await liquidate(
            sdk,
            connection,
            marginAccountWrapper,
            {
              marginAccount: new PublicKey(marginAccount),
              exchange: new PublicKey(exchange),
              owner: new PublicKey(owner),
              liquidator: liquidatorSigner.publicKey,
              liquidatorMarginAccount,
            },
            [liquidatorSigner],
            liquidatorSigner.publicKey
        );
      }

      const exchangeAccount = await sdk.accountFetcher.getExchange(new PublicKey(exchange));
      if (exchangeAccount === undefined) {
        throw new Error("Invalid exchange address");
      }

      const allMarketAddresses: PublicKey[] = [];
      for (const marketId of exchangeAccount.marketIds) {
        if (marketId === 0) continue;
        const [market] = getMarketPda(new PublicKey(exchange), marketId);
        allMarketAddresses.push(market);
      }

      const allMarkets = await sdk.accountFetcher.getMarkets(allMarketAddresses);
      const [markets, priceFeeds] = await getMarketMapAndPriceFeedMap(sdk, allMarkets);
      const margins = marginAccountWrapper.getAccountMargins(
          new ExchangeWrapper(exchangeAccount),
          markets,
          priceFeeds,
          Math.floor(Date.now() / 1000)
      );

      if (margins.canLiquidate()) {
        console.log(`Starting liquidation for ${marginAccountWrapper.address}`);
        const signature = await liquidate(
            sdk,
            connection,
            marginAccountWrapper,
            {
              marginAccount: new PublicKey(marginAccount),
              exchange: new PublicKey(exchange),
              owner: new PublicKey(owner),
              liquidator: liquidatorSigner.publicKey,
              liquidatorMarginAccount,
            },
            [liquidatorSigner],
            liquidatorSigner.publicKey
        );
        console.log("Signature: ", signature);
      }
    },
  });
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

async function liquidate(
    sdk: ParclV3Sdk,
    connection: Connection,
    marginAccount: MarginAccountWrapper,
    accounts: LiquidateAccounts,
    signers: Signer[],
    feePayer: Address,
    params?: LiquidateParams
): Promise<string> {
  const [marketAddresses, priceFeedAddresses] = getMarketsAndPriceFeeds(marginAccount, accounts);
  const { blockhash: recentBlockhash } = await connection.getLatestBlockhash();
  const tx = sdk
      .transactionBuilder()
      .liquidate(accounts, marketAddresses, priceFeedAddresses, params)
      .feePayer(feePayer)
      .buildSigned(signers, recentBlockhash);
  return await sendAndConfirmTransaction(connection, tx, signers);
}

function getMarketsAndPriceFeeds(
    marginAccount: MarginAccountWrapper,
    markets: MarketMap
): [Address[], Address[]] {
  const marketAddresses: Address[] = [];
  const priceFeedAddresses: Address[] = [];
  for (const position of marginAccount.positions()) {
    const market = markets[position.marketId()];
    if (market.address === undefined) {
      throw new Error(`Market is missing from markets map (id=${position.marketId()})`);
    }
    marketAddresses.push(market.address);
    priceFeedAddresses.push(market.priceFeed());
  }
  return [marketAddresses, priceFeedAddresses];
}
