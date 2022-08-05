use crate::sdk::rule::Verb;
use crate::sdk::value::Value;
use crate::sdk::{Account, Role, RoleBinding, Rule};
use clap::Parser;
use futures_util::StreamExt;
use m10_sdk::account::AccountId;
use m10_sdk::client::Channel;
use m10_sdk::client::M10Client;
use m10_sdk::error::M10Error;
use m10_sdk::prost::bytes::Bytes;
use m10_sdk::{
    sdk, AccountBuilder, AccountFilter, ActionBuilder, ActionsFilter, Collection, DocumentBuilder,
    Ed25519, Signer, StepBuilder, TransferBuilder, TxnFilter, WithContext,
};
use rust_decimal::prelude::One;
use rust_decimal::Decimal;
use service::config::{Config, LiquidityConfig};
use service::event::{Event, Execute, Quote, Request};
use service::{FX_SWAP_ACTION, FX_SWAP_METADATA};
use std::collections::HashMap;
use std::iter::once;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, info, info_span, Instrument};
use uuid::Uuid;

const DEFAULT_LEDGER_URL: &str = "https://develop.m10.net";

const TEST_ROOT_KEY: &str = "3053020101300506032b6570042204207cabfa6e59e20cbd271a0c7\
    60aeb5ff104f2e873923df3b56c76f33271a52b6aa12303210000230a56\
    4788b7e2f9cf7f71939607ba38dc37b6cb867abe50890b065ca634ce";

#[derive(Parser)]
#[clap(name = "command")]
#[clap(bin_name = "command")]
struct Command {
    #[clap(short, long, default_value = DEFAULT_LEDGER_URL)]
    url: String,
    #[clap(subcommand)]
    command: RPC,
}

#[derive(clap::Subcommand, Debug)]
enum RPC {
    Setup(Setup),
    Initiate(Initiate),
    Execute(ExecuteQuote),
}

#[derive(clap::Args, Debug)]
#[clap(author, version, about, long_about = None)]
struct Setup {
    #[clap(short, long)]
    key_pair: Option<String>,
    #[clap(short, long, multiple = true)]
    currencies: Vec<String>,
}

#[derive(clap::Args, Debug)]
#[clap(author, version, about, long_about = None)]
struct Initiate {
    #[clap(short, long)]
    key_pair: String,
    #[clap(short, long)]
    from: AccountId,
    #[clap(short, long, value_parser)]
    to: AccountId,
    #[clap(short, long, value_parser)]
    amount: u64,
}

#[derive(clap::Args, Debug)]
#[clap(author, version, about, long_about = None)]
struct ExecuteQuote {
    #[clap(short, long)]
    key_pair: String,
    #[clap(short, long, value_parser)]
    context_id: String,
    #[clap(
        long,
        value_parser,
        help = "Percentage margin on the current exchange rate"
    )]
    margin: Decimal,
    #[clap(short, long, value_parser, help = "Duration in seconds")]
    valid_for: Option<u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Init logging
    tracing_subscriber::fmt().with_target(false).init();

    let Command { url, command } = Command::parse();

    let channel = Channel::from_shared(url)?
        .timeout(Duration::from_secs(15))
        .connect_lazy()?;

    match command {
        RPC::Setup(setup) => {
            info!("{:?}", setup);
            let key_pair = if let Some(key_pair) = setup.key_pair.as_ref() {
                Ed25519::load_key_pair(key_pair)?
            } else {
                root_key()
            };
            let client = M10Client::new(key_pair, channel);
            try_setup(client, setup)
                .instrument(info_span!("setup"))
                .await
        }
        RPC::Initiate(initiate) => {
            info!("{:?}", initiate);
            let key_pair = Ed25519::load_key_pair(&initiate.key_pair)?;
            let client = M10Client::new(key_pair, channel);
            try_initiate(client, initiate)
                .instrument(info_span!("initiate"))
                .await
        }
        RPC::Execute(execute) => {
            info!("{:?}", execute);
            let key_pair = Ed25519::load_key_pair(&execute.key_pair)?;
            let client = M10Client::new(key_pair, channel);
            let context_id = hex::decode(&execute.context_id)?;

            // Find the last transaction in the context
            let actions = client
                .list_actions(TxnFilter::<ActionsFilter>::by_context_id(
                    FX_SWAP_ACTION.to_string(),
                    context_id.clone(),
                ))
                .await?;
            let action = actions
                .first()
                .ok_or_else(|| anyhow::anyhow!("No quote found for context"))?;
            let quote = match serde_json::from_slice(&action.payload)? {
                Event::Quote(quote) => quote,
                Event::Request(_) => panic!("Request hasn't been quoted"),
                Event::Execute(_) | Event::Completed => {
                    panic!("Already executed");
                }
            };

            let mut stream = client
                .observe_actions(
                    AccountFilter::name(FX_SWAP_ACTION.to_string())
                        .involves(quote.request.from)
                        .starting_from(action.tx_id + 1),
                )
                .await?;

            try_execute(&client, execute, quote, context_id.clone())
                .instrument(info_span!("execute"))
                .await?;

            // Wait for confirmation
            info!("Waiting for swap confirmation");
            while let Some(Ok(actions)) = stream.next().await {
                for action in actions {
                    if action.context_id != context_id {
                        continue;
                    }

                    let event = serde_json::from_slice(&action.payload);
                    if let Ok(Event::Completed) = event {
                        info!("Swap completed");
                        return Ok(());
                    } else {
                        error!("Invalid event: {:?}", event);
                    }
                }
            }

            Ok(())
        }
    }
}

async fn try_setup(client: M10Client<Ed25519>, setup: Setup) -> anyhow::Result<()> {
    let liquidity_key = Ed25519::new_key_pair(Some("./liquidity.pkcs8"))?;
    let alice_key = Ed25519::new_key_pair(Some("./alice.pkcs8"))?;
    let bob_key = Ed25519::new_key_pair(Some("./bob.pkcs8"))?;

    let mut accounts = vec![];
    // Scan for all currencies
    for i in 0..256 {
        let root_id = AccountId::from_root_account_index(i)?;
        match client.get_account(root_id).await {
            Ok(account) => {
                let currency = account.code.to_lowercase();
                if !setup.currencies.contains(&currency) {
                    continue;
                }
                info!(%account.id, %currency, "Found account");
                if setup.currencies.contains(&currency) {
                    accounts.push(account);
                }
            }
            Err(M10Error::Status(status)) if status.code() as usize == 5 => {
                // NOT FOUND
                break;
            }
            Err(err) => {
                panic!("Could not retrieve account: {:?}", err);
            }
        }
    }

    // Create accounts & account docs for all currencies
    let mut liquidity_accounts = HashMap::new();
    for account in accounts {
        let currency = account.code;
        async {
            let account_id = create_account(
                &client,
                account.id,
                liquidity_key.public_key(),
                "fx-liquidity".to_string(),
                10_000_000,
            )
            .instrument(info_span!("liquidity"))
            .await?;

            liquidity_accounts.insert(currency.clone(), account_id);

            if currency.to_lowercase() == setup.currencies[0].to_lowercase() {
                // Create an account for alice
                let account_id = create_account(
                    &client,
                    account.id,
                    alice_key.public_key(),
                    "alice".to_string(),
                    10_000_000,
                )
                .instrument(info_span!("Alice"))
                .await?;
                info!(account_id = %hex::encode(account_id.to_be_bytes()), "Created Alice {} account", currency);
            }

            if currency.to_lowercase() == setup.currencies[1].to_lowercase() {
                let account_id = create_account(
                    &client,
                    account.id,
                    bob_key.public_key(),
                    "bob".to_string(),
                    0,
                )
                .instrument(info_span!("Bob"))
                .await?;
                info!(account_id = %hex::encode(account_id.to_be_bytes()), "Created Bob {} account", currency);
            }

            Result::<(), anyhow::Error>::Ok(())
        }
        .instrument(info_span!("account", %currency))
        .await?;
    }

    if liquidity_accounts.is_empty() {
        return Ok(());
    }

    // Setup role & role-binding
    let role_id = Uuid::new_v4();
    client
        .documents(
            DocumentBuilder::default()
                .insert(Role {
                    id: Bytes::copy_from_slice(&role_id.into_bytes()),
                    owner: Bytes::copy_from_slice(liquidity_key.public_key()),
                    name: "m10.fx.liquidity".to_string(),
                    rules: vec![
                        can_read_and_transact_ledger_accounts(liquidity_accounts.values().copied()),
                        can_read_and_transact_accounts(liquidity_accounts.values().copied()),
                        can_read_all_ledger_accounts(),
                        can_read_all_accounts(),
                    ],
                })
                .insert(RoleBinding {
                    id: Bytes::copy_from_slice(role_id.as_bytes()),
                    owner: Bytes::copy_from_slice(liquidity_key.public_key()),
                    name: "m10.fx.liquidity".to_string(),
                    role: Bytes::copy_from_slice(role_id.as_bytes()),
                    subjects: vec![Bytes::copy_from_slice(liquidity_key.public_key())],
                    expressions: vec![],
                    is_universal: false,
                }),
        )
        .await?;
    info!(%role_id, "Created role & role-binding");

    // Write config
    let toml_string = toml::to_string(&Config {
        address: DEFAULT_LEDGER_URL.to_string(),
        liquidity: liquidity_accounts
            .into_iter()
            .map(|(currency, account)| {
                let base_rate = rate_for(&currency);
                (
                    currency,
                    LiquidityConfig {
                        account: hex::encode(&account.to_be_bytes()),
                        base_rate,
                        key_pair: PathBuf::from("./liquidity.pkcs8"),
                    },
                )
            })
            .collect(),
    })?;
    let path = "config.toml";
    std::fs::write(path, toml_string)?;
    info!(%path, "Wrote config to");

    Ok(())
}

async fn try_initiate(client: M10Client<Ed25519>, initiate: Initiate) -> anyhow::Result<()> {
    let from_account = client.get_account(initiate.from).await?;
    let context_id = fastrand::u64(..).to_be_bytes().to_vec();
    let context_hex = hex::encode(&context_id);
    let event = Event::Request(Request {
        from: from_account.id,
        to: initiate.to,
        amount: Decimal::new(initiate.amount as i64, from_account.decimals),
    });

    // Submit request
    let tx_id = client
        .action(
            ActionBuilder::for_all(FX_SWAP_ACTION.to_string(), from_account.id)
                .payload(serde_json::to_vec(&event)?),
            context_id.clone(),
        )
        .await?;
    info!(%tx_id, context_id=%context_hex, "Submitted transaction");

    // Wait for the quote
    let mut actions = client
        .observe_actions(
            AccountFilter::name(FX_SWAP_ACTION.to_string())
                .starting_from(tx_id + 1)
                .involves(from_account.id),
        )
        .await?;

    info!("Waiting for the proposed quote");
    while let Some(Ok(actions)) = actions.next().await {
        for action in actions {
            if action.context_id != context_id {
                continue;
            }

            let event =
                serde_json::from_slice::<Event>(&action.payload).expect("invalid Event data");

            if let Event::Quote(quote) = event {
                info!(
                    "Received quote {} context_id={}",
                    quote,
                    hex::encode(context_id)
                );
                return Ok(());
            } else {
                panic!("Invalid Event type");
            }
        }
    }

    info!(context_id = %context_hex);
    Ok(())
}

async fn try_execute(
    client: &M10Client<Ed25519>,
    execute: ExecuteQuote,
    quote: Quote,
    context_id: Vec<u8>,
) -> anyhow::Result<()> {
    info!(
        "Transferring from {} -> {}",
        quote.request.from, quote.intermediary
    );
    let amount = quote.rate * quote.request.amount;
    let tx_id = client
        .transfer(
            TransferBuilder::new()
                .step(
                    StepBuilder::new(quote.request.from, quote.intermediary, amount.try_into()?)
                        .custom_metadata(
                            FX_SWAP_METADATA,
                            serde_json::to_vec(&Event::Execute(Execute {
                                request: quote.request,
                                valid_until: (SystemTime::now()
                                    + Duration::from_secs(execute.valid_for.unwrap_or(300)))
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                                upper_limit: (Decimal::one() + execute.margin) * quote.rate,
                                lower_limits: (Decimal::one() - execute.margin) * quote.rate,
                            }))?,
                        ),
                )
                .context_id(context_id.clone()),
        )
        .await?;
    info!(%tx_id, "Transfer success");
    Ok(())
}

fn root_key() -> Ed25519 {
    Ed25519::from_pkcs8(&hex::decode(TEST_ROOT_KEY).unwrap()).unwrap()
}

async fn create_account(
    client: &M10Client<Ed25519>,
    parent_account: AccountId,
    owner: &[u8],
    name: String,
    funding: u64,
) -> anyhow::Result<AccountId> {
    // Create ledger account
    let (_tx_id, account_id) = client
        .create_account(AccountBuilder::parent(parent_account))
        .await?;
    info!(%account_id, "Created account");

    // Register RBAC resource
    info!("Registering RBAC document");
    let role_id = Uuid::new_v4();
    client
        .documents(
            DocumentBuilder::default()
                // Add Account RBAC resource
                .insert(Account {
                    owner: owner.to_vec(),
                    profile_image_url: String::new(),
                    name: name.clone(),
                    public_name: name,
                    id: account_id.to_vec(),
                })
                // Create role & role-binding for basic accounts
                .insert(Role {
                    id: Bytes::copy_from_slice(&role_id.into_bytes()),
                    owner: Bytes::copy_from_slice(owner),
                    name: "m10.fx.account".to_string(),
                    rules: vec![
                        can_read_and_transact_accounts(once(account_id)),
                        can_read_and_transact_ledger_accounts(once(account_id)),
                    ],
                })
                .insert(RoleBinding {
                    id: Bytes::copy_from_slice(&role_id.into_bytes()),
                    owner: Bytes::copy_from_slice(owner),
                    name: "m10.fx.account".to_string(),
                    role: Bytes::copy_from_slice(role_id.as_bytes()),
                    subjects: vec![Bytes::copy_from_slice(owner)],
                    expressions: vec![],
                    is_universal: false,
                }),
        )
        .await?;
    info!(%role_id, "Created role & role-binding");

    // ledger-accounts
    if funding > 0 {
        // Fund account
        info!(%funding, "Funding account");
        client
            .transfer(TransferBuilder::new().step(StepBuilder::new(
                parent_account,
                account_id,
                funding,
            )))
            .await?;
    }

    Ok(account_id)
}

fn can_read_and_transact_accounts(accounts: impl Iterator<Item = AccountId>) -> Rule {
    Rule {
        collection: Collection::Accounts.to_string(),
        instance_keys: accounts
            .map(AccountId::to_be_bytes)
            .map(|b| Bytes::copy_from_slice(&b))
            .map(Value::BytesValue)
            .map(Some)
            .map(|value| sdk::Value { value })
            .collect(),
        verbs: vec![Verb::Read as i32, Verb::Transact as i32],
    }
}

fn can_read_and_transact_ledger_accounts(accounts: impl Iterator<Item = AccountId>) -> Rule {
    Rule {
        collection: "ledger-accounts".to_string(),
        instance_keys: accounts
            .map(AccountId::to_be_bytes)
            .map(|b| Bytes::copy_from_slice(&b))
            .map(Value::BytesValue)
            .map(Some)
            .map(|value| sdk::Value { value })
            .collect(),
        verbs: vec![Verb::Read as i32, Verb::Transact as i32],
    }
}

fn can_read_all_ledger_accounts() -> Rule {
    Rule {
        collection: "ledger-accounts".to_string(),
        instance_keys: vec![],
        verbs: vec![Verb::Read as i32],
    }
}

fn can_read_all_accounts() -> Rule {
    Rule {
        collection: Collection::Accounts.to_string(),
        instance_keys: vec![],
        verbs: vec![Verb::Read as i32],
    }
}

fn rate_for(currency: &str) -> Decimal {
    match currency.to_lowercase().as_str() {
        "usd" => Decimal::one(),
        "eur" => Decimal::new(9, 1),
        "btc" => Decimal::new(43, 6),
        _ => Decimal::new(5, 1),
    }
}
