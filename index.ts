import Client, {
  CommitmentLevel,
  SubscribeRequestAccountsDataSlice,
  SubscribeRequestFilterAccounts,
  SubscribeRequestFilterBlocks,
  SubscribeRequestFilterBlocksMeta,
  SubscribeRequestFilterEntry,
  SubscribeRequestFilterSlots,
  SubscribeRequestFilterTransactions,
} from "@triton-one/yellowstone-grpc";
import { SubscribeRequestPing } from "@triton-one/yellowstone-grpc/dist/grpc/geyser";
import { PublicKey, VersionedTransactionResponse } from "@solana/web3.js";
import { Idl } from "@project-serum/anchor";
import { SolanaParser } from "@shyft-to/solana-transaction-parser";
import { TransactionFormatter } from "./utils/transaction-formatter";
import pumpFunIdl from "./idls/pump_0.1.0.json";
import { SolanaEventParser } from "./utils/event-parser";
import { bnLayoutFormatter } from "./utils/bn-layout-formatter";
import { transactionOutput } from "./utils/transactionOutput";
import { getBondingCurveAddress } from "./utils/getBonding";

const TelegramBot = require('node-telegram-bot-api');
const a = 0.00022500443612959005
const b = -0.04465309899499017
const c = 3.3439469804363813
const d = 1.7232697904532974
  const token = '7628488015:AAFXiqCkoaVxLKmnVxscyzV4N5M31Azd0xE';
  var value = 0
  // Create a bot instance
  const bot = new TelegramBot(token);
  
  // Replace with your channel ID or username (e.g., '@your_channel_username')
  const chatId = '@Pumpfun_bondingCurve_alert_ch';
  
  // Create a Set to track sent addresses
  const sentAddresses = new Set();
  
  // Function to send message
  function sendMessage(message) {
      bot.sendMessage(chatId, message, { parse_mode: 'HTML' })
          .then(() => {
              console.log('Message sent successfully');
          })
          .catch((error) => {
              console.error('Error sending message:', error);
          });
  }
interface SubscribeRequest {
  accounts: { [key: string]: SubscribeRequestFilterAccounts };
  slots: { [key: string]: SubscribeRequestFilterSlots };
  transactions: { [key: string]: SubscribeRequestFilterTransactions };
  transactionsStatus: { [key: string]: SubscribeRequestFilterTransactions };
  blocks: { [key: string]: SubscribeRequestFilterBlocks };
  blocksMeta: { [key: string]: SubscribeRequestFilterBlocksMeta };
  entry: { [key: string]: SubscribeRequestFilterEntry };
  commitment?: CommitmentLevel | undefined;
  accountsDataSlice: SubscribeRequestAccountsDataSlice[];
  ping?: SubscribeRequestPing | undefined;
}

const TXN_FORMATTER = new TransactionFormatter();
const PUMP_FUN_PROGRAM_ID = new PublicKey(
  "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
);
const PUMP_FUN_IX_PARSER = new SolanaParser([]);
PUMP_FUN_IX_PARSER.addParserFromIdl(
  PUMP_FUN_PROGRAM_ID.toBase58(),
  pumpFunIdl as Idl,
);
const PUMP_FUN_EVENT_PARSER = new SolanaEventParser([], console);
PUMP_FUN_EVENT_PARSER.addParserFromIdl(
  PUMP_FUN_PROGRAM_ID.toBase58(),
  pumpFunIdl as Idl,
);

async function handleStream(client: Client, args: SubscribeRequest) {
  // Subscribe for events
  const stream = await client.subscribe();

  // Create `error` / `end` handler
  const streamClosed = new Promise<void>((resolve, reject) => {
    stream.on("error", (error) => {
      console.log("ERROR", error);
      reject(error);
      stream.end();
    });
    stream.on("end", () => {
      resolve();
    });
    stream.on("close", () => {
      resolve();
    });
  });

  // Handle updates
  stream.on("data", async (data) => {
    try{
      if (data?.transaction) {
        const txn = TXN_FORMATTER.formTransactionFromJson(
          data.transaction,
          Date.now(),
        );
        const parsedTxn = decodePumpFunTxn(txn);
        if (!parsedTxn) return;
        const tOutput = transactionOutput(parsedTxn)
        const balance = await getBondingCurveAddress(tOutput.bondingCurve)
        const progress = a * Number(balance)**3 + b * Number(balance)**2 + c * Number(balance) + d;
        console.log(
          `
          TYPE : ${tOutput.type}
          MINT : ${tOutput.mint}
          SIGNER : ${tOutput.user}
          BONDING CURVE : ${tOutput.bondingCurve}
          TOKEN AMOUNT : ${tOutput.tokenAmount}
          SOL AMOUNT : ${tOutput.solAmount} SOL
          POOL DETAILS : ${balance} SOL
                        ${Number(progress).toFixed(2)}% to completion
          SIGNATURE : ${txn.transaction.signatures[0]}
          `
        );

        if (Number(progress) >= 97.7 && Number(progress) <= 100 && !sentAddresses98.has(tOutput.mint)) {
          sendMessage(`Token: <code>${tOutput.mint}</code> \nCurve Progress: <b>${Number(progress).toFixed(1)} %</b>    Pool Value : <b>${Number(balance).toFixed(2)} SOL</b>`);
          // Add the address to the set of sent addresses
          sentAddresses.add(tOutput.mint);
        }
      }
    }catch(err){
      console.log(err);
    }
  });

  // Send subscribe request
  await new Promise<void>((resolve, reject) => {
    stream.write(args, (err: any) => {
      if (err === null || err === undefined) {
        resolve();
      } else {
        reject(err);
      }
    });
  }).catch((reason) => {
    console.error(reason);
    throw reason;
  });

  await streamClosed;
}

async function subscribeCommand(client: Client, args: SubscribeRequest) {
  while (true) {
    try {
      await handleStream(client, args);
    } catch (error) {
      console.error("Stream error, restarting in 1 second...", error);
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

const client = new Client(
  'SHYFT gRPC',
  '23cf6dd3-e616-4d08-9740-708f708f913e',
  undefined,
);
const req: SubscribeRequest = {
  accounts: {},
  slots: {},
  transactions: {
    pumpFun: {
      vote: false,
      failed: false,
      signature: undefined,
      accountInclude: [PUMP_FUN_PROGRAM_ID.toBase58()],//["Hb9uyfUg8RbLsfSdud3LSW3yXx5PodM3XZmMS2ajpump",'DT1WapMVRafeBbJ2RcA7Rf2dF3g6pEa7vz7rxYLXpump]
      accountExclude: [],
      accountRequired: [],
    },
  },
  transactionsStatus: {},
  entry: {},
  blocks: {},
  blocksMeta: {},
  accountsDataSlice: [],
  ping: undefined,
  commitment: CommitmentLevel.CONFIRMED,
};

subscribeCommand(client, req);

function decodePumpFunTxn(tx: VersionedTransactionResponse) {
  if (tx.meta?.err) return;

  const paredIxs = PUMP_FUN_IX_PARSER.parseTransactionData(
    tx.transaction.message,
    tx.meta.loadedAddresses,
  );

  const pumpFunIxs = paredIxs.filter((ix) =>
    ix.programId.equals(PUMP_FUN_PROGRAM_ID),
  );

  if (pumpFunIxs.length === 0) return;
  const events = PUMP_FUN_EVENT_PARSER.parseEvent(tx);
  const result = { instructions: pumpFunIxs, events };
  bnLayoutFormatter(result);
  return result;
}