// governance_proposal.js         
// Example of creating and voting on a governance proposal on Solana using SPL Governance (JavaScript)

const {  
    Connection,
    clusterApiUrl,
    Keypair, 
    LAMPORTS_PER_SOL, 
    PublicKey, 
    Transaction, 
    sendAndConfirmTransaction, 
} = require('@solana/web3.js');
const {
    createRealm,
    createGovernance,
    createProposal,
    castVote,
    getRealm,
    getGovernance,
    getProposal,
    getTokenOwnerRecord,
    Governance,
    Proposal,
    Vote,
    VoteKind,
    withCreateRealm,
    withCreateGovernance,
    withCreateProposal,
    withCastVote,
    PROGRAM_ID,
} = require('@solana/spl-governance');
const { Token, TOKEN_PROGRAM_ID } = require('@solana/spl-token');
const bs58 = require('bs58');
const fs = require('fs').promises;
const path = require('path');

// Utility to log messages to console and optionally to a file
async function logMessage(message) {
    console.log(`[${new Date().toISOString()}] ${message}`);
    try {
        await fs.appendFile(
            path.join(__dirname, 'governance_proposal.log'),
            `[${new Date().toISOString()}] ${message}\n`
        );
    } catch (error) {
        console.error('Error writing to log file:', error.message);
    }
}

// Class to manage governance proposals and voting on Solana
class GovernanceManager {
    constructor(cluster = 'devnet') {
        // Initialize connection to Solana cluster (default to devnet for testing)
        this.connection = new Connection(clusterApiUrl(cluster), 'confirmed');
        // Payer wallet (null by default for safety; set via environment or file in production)
        this.payer = null;
        // Governance realm and related data
        this.realm = null;
        this.governance = null;
        this.proposal = null;
        // Community token mint (for voting power)
        this.communityMint = null;
        // Token owner record for the voter
        this.tokenOwnerRecord = null;
        logMessage('GovernanceManager initialized with connection to ' + cluster);
    }

    // Load payer wallet from environment variable or file (safely for demo)
    async loadPayerWallet() {
        try {
            const privateKeyBase58 = process.env.SOLANA_PRIVATE_KEY;
            if (!privateKeyBase58) {
                logMessage('No private key provided in environment. Running in demo mode (no real transactions).');
                return false;
            }
            const privateKeyUint8Array = bs58.decode(privateKeyBase58);
            this.payer = Keypair.fromSecretKey(privateKeyUint8Array);
            logMessage('Payer wallet loaded successfully.');
            // Verify balance for transaction fees
            const balance = await this.connection.getBalance(this.payer.publicKey);
            logMessage(`Payer balance: ${balance / LAMPORTS_PER_SOL} SOL`);
            if (balance < 0.01 * LAMPORTS_PER_SOL) {
                throw new Error('Insufficient balance for transactions.');
            }
            return true;
        } catch (error) {
            logMessage('Error loading payer wallet: ' + error.message);
            this.payer = null;
            return false;
        }
    }

    // Initialize a governance realm (a container for governance structures)
    async initializeRealm(realmName = 'OntoraAIRealm') {
        if (!this.payer) {
            logMessage('No payer wallet set. Skipping realm creation (demo mode).');
            return null;
        }
        try {
            logMessage('Creating governance realm...');
            // Create a community token mint (simplified; in production, use an existing mint)
            this.communityMint = Keypair.generate();
            logMessage(`Community mint created: ${this.communityMint.publicKey.toBase58()}`);

            // Create realm with the payer as the authority
            const realmKeypair = Keypair.generate();
            const tx = new Transaction();
            await withCreateRealm(
                tx,
                PROGRAM_ID,
                2, // Governance program version
                realmName,
                this.payer.publicKey,
                this.communityMint.publicKey,
                this.payer.publicKey, // Council mint (optional, set to payer for simplicity)
                0, // Min community tokens to create governance (0 for testing)
                this.payer.publicKey // Realm authority
            );
            const txSignature = await sendAndConfirmTransaction(this.connection, tx, [this.payer, realmKeypair]);
            logMessage(`Realm created successfully. Tx: ${txSignature}`);
            this.realm = realmKeypair.publicKey;
            return this.realm;
        } catch (error) {
            logMessage('Error creating realm: ' + error.message);
            return null;
        }
    }

    // Create a governance instance within the realm
    async createGovernance() {
        if (!this.payer || !this.realm) {
            logMessage('No payer wallet or realm set. Skipping governance creation (demo mode).');
            return null;
        }
        try {
            logMessage('Creating governance...');
            const governanceKeypair = Keypair.generate();
            const tx = new Transaction();
            await withCreateGovernance(
                tx,
                PROGRAM_ID,
                2, // Program version
                this.realm,
                this.communityMint.publicKey,
                { veto: false, yesVotePercentageThreshold: 60, totalSupply: 1000000 }, // Config (60% yes to pass)
                this.payer.publicKey, // Governance authority
                this.payer.publicKey // Payer
            );
            const txSignature = await sendAndConfirmTransaction(this.connection, tx, [this.payer, governanceKeypair]);
            logMessage(`Governance created successfully. Tx: ${txSignature}`);
            this.governance = governanceKeypair.publicKey;
            return this.governance;
        } catch (error) {
            logMessage('Error creating governance: ' + error.message);
            return null;
        }
    }

    // Create a proposal within the governance
    async createProposal(proposalName = 'Ontora AI Funding Proposal', description = 'Fund Ontora AI development with 1000 tokens') {
        if (!this.payer || !this.realm || !this.governance) {
            logMessage('No payer wallet, realm, or governance set. Skipping proposal creation (demo mode).');
            return null;
        }
        try {
            logMessage('Creating proposal...');
            const proposalKeypair = Keypair.generate();
            const tx = new Transaction();
            await withCreateProposal(
                tx,
                PROGRAM_ID,
                2, // Program version
                this.realm,
                this.governance,
                this.payer.publicKey, // Proposal owner (token owner record, simplified as payer)
                this.communityMint.publicKey,
                this.payer.publicKey, // Governance authority
                proposalName,
                description,
                0, // Draft state (0 = Draft)
                this.payer.publicKey // Payer
            );
            const txSignature = await sendAndConfirmTransaction(this.connection, tx, [this.payer, proposalKeypair]);
            logMessage(`Proposal created successfully. Tx: ${txSignature}`);
            this.proposal = proposalKeypair.publicKey;
            return this.proposal;
        } catch (error) {
            logMessage('Error creating proposal: ' + error.message);
            return null;
        }
    }

    // Cast a vote on the proposal
    async castVote(voteYes = true) {
        if (!this.payer || !this.realm || !this.governance || !this.proposal) {
            logMessage('No payer wallet, realm, governance, or proposal set. Skipping voting (demo mode).');
            return null;
        }
        try {
            logMessage('Casting vote...');
            const tx = new Transaction();
            await withCastVote(
                tx,
                PROGRAM_ID,
                2, // Program version
                this.realm,
                this.governance,
                this.proposal,
                this.payer.publicKey, // Token owner record (simplified as payer)
                this.payer.publicKey, // Vote authority
                this.communityMint.publicKey,
                voteYes ? VoteKind.Approve : VoteKind.Deny, // Vote type (Yes or No)
                { choice: voteYes ? VoteKind.Approve : VoteKind.Deny, weight: 1 }, // Vote details
                this.payer.publicKey // Payer
            );
            const txSignature = await sendAndConfirmTransaction(this.connection, tx, [this.payer]);
            logMessage(`Vote cast successfully (${voteYes ? 'Yes' : 'No'}). Tx: ${txSignature}`);
            return txSignature;
        } catch (error) {
            logMessage('Error casting vote: ' + error.message);
            return null;
        }
    }

    // Fetch and display proposal details (for verification)
    async getProposalDetails() {
        if (!this.proposal) {
            logMessage('No proposal set. Skipping details fetch (demo mode).');
            return null;
        }
        try {
            const proposalInfo = await getProposal(this.connection, this.proposal);
            logMessage('Proposal Details:');
            logMessage(`  Name: ${proposalInfo.name}`);
            logMessage(`  Description: ${proposalInfo.description}`);
            logMessage(`  State: ${proposalInfo.state}`);
            logMessage(`  Voting Start: ${new Date(proposalInfo.votingAt.toNumber() * 1000).toISOString()}`);
            return proposalInfo;
        } catch (error) {
            logMessage('Error fetching proposal details: ' + error.message);
            return null;
        }
    }
}

// Main function to demonstrate governance proposal creation and voting
async function main() {
    logMessage('Starting Solana Governance Proposal Demo...');
    const manager = new GovernanceManager('devnet'); // Use devnet for testing

    // Load payer wallet (safely skips if not set)
    const hasPayer = await manager.loadPayerWallet();
    if (!hasPayer) {
        logMessage('Demo mode: No real transactions will be sent. Set SOLANA_PRIVATE_KEY in environment for real operations.');
    }

    // Step 1: Create a governance realm
    await manager.initializeRealm('OntoraAIRealm');

    // Step 2: Create a governance instance
    await manager.createGovernance();

    // Step 3: Create a proposal
    await manager.createProposal(
        'Ontora AI Funding Proposal',
        'Allocate 1000 tokens to fund Ontora AI development initiatives.'
    );

    // Step 4: Cast a vote on the proposal
    await manager.castVote(true); // Vote Yes

    // Step 5: Fetch and display proposal details
    await manager.getProposalDetails();

    logMessage('Governance Proposal Demo completed.');
}

// Execute main function with error handling
if (require.main === module) {
    main().catch((error) => {
        logMessage('Fatal error in main execution: ' + error.message);
        process.exit(1);
    });
}

// Export for use in other modules
module.exports = { GovernanceManager };
