const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const { v4: uuidv4 } = require('uuid');

exports.handler = async (event) => {
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    const tableName = process.env.DYNAMODB_TABLE_NAME;
    const historyTableName = process.env.DYNAMODB_HISTORY_TABLE_NAME;

    const params = {
        Bucket: bucket,
        Key: key,
    };

    try {
        const data = await s3.getObject(params).promise();
        const logContent = data.Body.toString('utf-8');
        const category = getCategoryFromKey(key);
        const transactions = parseData(logContent, category.type);

        for (const transaction of transactions) {
            const recordID = uuidv4(); // Generate a unique RecordID
            const timestamp = new Date().toISOString();
            const commonAttributes = {
                RecordId : recordID,
                Date: transaction.Date,
                Category: category.name,
                CategoryType: category.type,
                AccountID: transaction.AccountID,
                LastModified: timestamp
            };
    
            let transactionItem = { ...commonAttributes };
            let historyItem = { ...commonAttributes, VersionTimestamp: timestamp, Operation: 'CREATE' };

            switch (transactionItem.CategoryType) {
                case 'LW': // Large Withdrawal
                    transactionItem = {
                        ...transactionItem,
                        TransactionID: transaction.TransactionID,
                        WithdrawalAmount: transaction.WithdrawalAmount,
                        Currency: transaction.Currency,
                        Description: transaction.Description
                    };
                    historyItem = {
                        ...historyItem,
                        TransactionID: transaction.TransactionID,
                        WithdrawalAmount: transaction.WithdrawalAmount,
                        Currency: transaction.Currency,
                        Description: transaction.Description
                    };
                break;
                case 'PS': // Potential Scam/Phishing attempt
                transactionItem = {
                    ...transactionItem,
                    ScamDetails: transaction.Description
                };
                historyItem = {
                    ...historyItem,
                    ScamDetails: transaction.Description
                };
                break;    
                case 'USP': // Change in User's Usage Pattern
                transactionItem = {
                    ...transactionItem,
                    UsagePatternDetails: transaction.Description
                };
                historyItem = {
                    ...historyItem,
                    UsagePatternDetails: transaction.Description
                };
                break;
        }
        console.log(transactionItem)
            await dynamoDB.put({
                TableName: tableName,
                Item: {
                    LogContent: logContent,
                    FileName: key,
                    ...transactionItem
                },
            }).promise();

             // Put operation for the history table
        await dynamoDB.put({
            TableName: historyTableName,
            Item: historyItem
        }).promise();
        }

        console.log(`Successfully processed ${key}`);
    } catch (error) {
        console.error(`Error processing ${key}:`, error);
        throw error;
    }
};

// Function to parse log content
function parseData(fileContent, categoryType) {
    const transactions = [];
    let currentTransaction = {};
    let currentDate = null;

    const lines = fileContent.split('\n');
    for (const line of lines) {
        const trimmedLine = line.trim();
        if (!trimmedLine) {
            continue;
        }

        if (trimmedLine.startsWith('Date:')) {
            currentDate = trimmedLine.split(': ')[1];
            continue;
        }

        const [key, value] = trimmedLine.split(': ', 2);
        if ((key === 'TransactionID' || (categoryType!== 'LW' && key === 'AccountID')) && Object.keys(currentTransaction).length > 0) {
            if (currentDate) {
                currentTransaction.Date = currentDate;
            }
            transactions.push(currentTransaction);
            currentTransaction = {};
        }
        currentTransaction[key] = value;
    }

    if (Object.keys(currentTransaction).length > 0) {
        if (currentDate) {
            currentTransaction.Date = currentDate;
        }
        transactions.push(currentTransaction);
    }

    return transactions;
}

function getCategoryFromKey(key) {
    key = key.substring(key.lastIndexOf('/')+1) 
    if (key.startsWith('LW_')) return {type: 'LW', name: 'LargeWithdrawal'};
    if (key.startsWith('PS_')) return {type: 'PS',name: 'PotentialScam'};
    if (key.startsWith('USP_')) return {type: 'USP',name: 'UserUsagePattern'};
    return 'Unknown';
}