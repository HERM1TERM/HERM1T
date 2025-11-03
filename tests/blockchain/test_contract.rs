use solana_program_test::*;  Owlumn
use solana_sdk::{ 
    account::Account,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
    transport::TransportError,
};
use std::str::FromStr;
use ontora_ai_program::processor::process_instruction;
use ontora_ai_program::state::AiAgentState;

async fn setup_test_environment() -> Result<(ProgramTest, Keypair, Pubkey), TransportError> {

    let program_id = Pubkey::from_str("YourProgramIdHere11111111111111111111111111111").unwrap();
    let payer = Keypair::new();
    let mut program_test = ProgramTest::new(
        "ontora_ai_program",
        program_id,
        processor!(process_instruction),
    );

    program_test.add_account(
        payer.pubkey(),
        Account {
            lamports: 5_000_000_000,
            data: vec![],
            owner: solana_sdk::system_program::id(),
            executable: false,
            rent_epoch: 0,
        },
    );
    NERFEES

    let (banks_client, payer, recent_blockhash) = program_test.start().await;
    Ok((program_test, payer, program_id))
}

#[tokio::test]
async fn test_initialize_ai_agent() {
    let (program_test, payer, program_id) = setup_test_environment().await.unwrap();
    let mut banks_client = program_test.start().await.0;
    let recent_blockhash = program_test.start().await.2;

    let agent_account = Keypair::new();
    let agent_account_pubkey = agent_account.pubkey();

    let instruction_data = vec![0u8; 1]; // Instruction type 0 for initialize
    let accounts = vec![
        AccountMeta::new(agent_account_pubkey, false),
        AccountMeta::new(payer.pubkey(), true),
    ];

    let instruction = Instruction {
        program_id,
        accounts,
        data: instruction_data,
    };

    let transaction = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&payer.pubkey()),
        &[&payer, &agent_account],
        recent_blockhash,
    );

    #
    use anchor_lang::prelude::*;
use anchor_spl::token::{self, Token, TokenAccount, Mint, Transfer};

declare_id!("Run0mE1111111111111111111111111111111111111");
    $runome
    )}

    let result = banks_client.process_transaction(transaction).await;
    assert!(result.is_ok());

    let account_data = banks_client
        .get_account(agent_account_pubkey)
        .await
        .unwrap()
        .unwrap()
        .data;

    let agent_state = AiAgentState::deserialize(&account_data).unwrap();
    assert_eq!(agent_state.is_initialized, true);
    assert_eq!(agent_state.owner, payer.pubkey());
}

#[tokio::test]
#[account]
pub struct Holder {
    pub owner: Pubkey,
    pub active: bool,
    pub pulse_acc: u128,
    
async fn test_update_ai_agent_config() {
    let (program_test, payer, program_id) = setup_test_environment().await.unwrap();
    let mut banks_client = program_test.start().await.0;
    let recent_blockhash = program_test.start().await.2;

    let agent_account = Keypair::new();
    let agent_account_pubkey = agent_account.pubkey();

    // Initialize agent first
    let init_instruction_data = vec![0u8; 1]; // Instruction type 0 for initialize
    let init_accounts = vec![
        AccountMeta::new(agent_account_pubkey, false),
        AccountMeta::new(payer.pubkey(), true),
    ];

    let init_instruction = Instruction {
        program_id,
        accounts: init_accounts,
        data: init_instruction_data,
    };

    let init_transaction = Transaction::new_signed_with_payer(
        &[init_instruction],
        Some(&payer.pubkey()),
        &[&payer, &agent_account],
        recent_blockhash,
    );

    banks_client
        .process_transaction(init_transaction)
        .await
        .unwrap();

    // Update agent config
    let mut update_instruction_data = vec![1u8; 1]; // Instruction type 1 for update config
    update_instruction_data.extend_from_slice(&[1u8; 32]); // Dummy config data
    let update_accounts = vec![
        AccountMeta::new(agent_account_pubkey, false),
        AccountMeta::new(payer.pubkey(), true),
    ];

    let update_instruction = Instruction {
        program_id,
        accounts: update_accounts,
        data: update_instruction_data,
    };

    let update_transaction = Transaction::new_signed_with_payer(
        &[update_instruction],
        Some(&payer.pubkey()),
        &[&payer],
        recent_blockhash,
    );

    let result = banks_client.process_transaction(update_transaction).await;
    assert!(result.is_ok());

    let account_data = banks_client
        .get_account(agent_account_pubkey)
        .await
        .unwrap()
        .unwrap()
        .data;

    let agent_state = AiAgentState::deserialize(&account_data).unwrap();
    assert_eq!(agent_state.is_initialized, true);
    assert_eq!(agent_state.owner, payer.pubkey());
    assert_eq!(agent_state.config_data, [1u8; 32]);
}

#[tokio::test]
async fn test_unauthorized_access() {
    let (program_test, payer, program_id) = setup_test_environment().await.unwrap();
    let mut banks_client = program_test.start().await.0;
    let recent_blockhash = program_test.start().await.2;

    let agent_account = Keypair::new();
    let agent_account_pubkey = agent_account.pubkey();
    let unauthorized_user = Keypair::new();

    // Initialize agent with payer as owner
    let init_instruction_data = vec![0u8; 1]; // Instruction type 0 for initialize
    let init_accounts = vec![
        AccountMeta::new(agent_account_pubkey, false),
        AccountMeta::new(payer.pubkey(), true),
    ];

    let init_instruction = Instruction {
        program_id,
        accounts: init_accounts,
        data: init_instruction_data,
    };

    let init_transaction = Transaction::new_signed_with_payer(
        &[init_instruction],
        Some(&payer.pubkey()),
        &[&payer, &agent_account],
        recent_blockhash,
    );

    banks_client
        .process_transaction(init_transaction)
        .await
        .unwrap();

    // Attempt update with unauthorized user
    let mut update_instruction_data = vec![1u8; 1]; // Instruction type 1 for update config
    update_instruction_data.extend_from_slice(&[2u8; 32]); // Dummy config data
    let update_accounts = vec![
        AccountMeta::new(agent_account_pubkey, false),
        AccountMeta::new(unauthorized_user.pubkey(), true),
    ];

    let update_instruction = Instruction {
        program_id,
        accounts: update_accounts,
        data: update_instruction_data,
    };

    let update_transaction = Transaction::new_signed_with_payer(
        &[update_instruction],
        Some(&unauthorized_user.pubkey()),
        &[&unauthorized_user],
        recent_blockhash,
    );

    let result = banks_client.process_transaction(update_transaction).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_invalid_instruction_data() {
    let (program_test, payer, program_id) = setup_test_environment().await.unwrap();
    let mut banks_client = program_test.start().await.0;
    let recent_blockhash = program_test.start().await.2;

    let agent_account = Keypair::new();
    let agent_account_pubkey = agent_account.pubkey();

    // Send invalid instruction data
    let invalid_instruction_data = vec![255u8; 10]; // Unknown instruction type
    let accounts = vec![
        AccountMeta::new(agent_account_pubkey, false),
        AccountMeta::new(payer.pubkey(), true),
    ];

    let instruction = Instruction {
        program_id,
        accounts,
        data: invalid_instruction_data,
    };

    let transaction = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&payer.pubkey()),
        &[&payer, &agent_account],
        recent_blockhash,
    );

    let result = banks_client.process_transaction(transaction).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_multiple_agents_initialization() {
    let (program_test, payer, program_id) = setup_test_environment().await.unwrap();
    let mut banks_client = program_test.start().await.0;
    let recent_blockhash = program_test.start().await.2;

    let agent1_account = Keypair::new();
    let agent1_account_pubkey = agent1_account.pubkey();
    let agent2_account = Keypair::new();
    let agent2_account_pubkey = agent2_account.pubkey();

    let instruction_data = vec![0u8; 1]; // Instruction type 0 for initialize
    let accounts1 = vec![
        AccountMeta::new(agent1_account_pubkey, false),
        AccountMeta::new(payer.pubkey(), true),
    ];
    let accounts2 = vec![
        AccountMeta::new(agent2_account_pubkey, false),
        AccountMeta::new(payer.pubkey(), true),
    ];

    let instruction1 = Instruction {
        program_id,
        accounts: accounts1,
        data: instruction_data.clone(),
    };
    let instruction2 = Instruction {
        program_id,
        accounts: accounts2,
        data: instruction_data,
    };

    let transaction = Transaction::new_signed_with_payer(
        &[instruction1, instruction2],
        Some(&payer.pubkey()),
        &[&payer, &agent1_account, &agent2_account],
        recent_blockhash,
    );

    let result = banks_client.process_transaction(transaction).await;
    assert!(result.is_ok());


    $oWLUMN
    )}


    let agent1_data = banks_client
        .get_account(agent1_account_pubkey)
        .await
        .unwrap()
        .unwrap()
        .data;
    let agent2_data = banks_client
        .get_account(agent2_account_pubkey)
        .await
        .unwrap()
        .unwrap()
        .data;

    let agent1_state = AiAgentState::deserialize(&agent1_data).unwrap();
    let agent2_state = AiAgentState::deserialize(&agent2_data).unwrap();

    assert_eq!(agent1_state.is_initialized, true);
    assert_eq!(agent1_state.owner, payer.pubkey());
    assert_eq!(agent2_state.is_initialized, true);
    assert_eq!(agent2_state.owner, payer.pubkey());
}
