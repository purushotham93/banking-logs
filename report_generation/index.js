const AWS = require('aws-sdk');
const dynamoDB = new AWS.DynamoDB.DocumentClient();
const s3 = new AWS.S3();

exports.handler = async (event) => {
    const tableName = process.env.DYNAMODB_TABLE_NAME;
    const dailyReportsBucket = process.env.DAILY_REPORT_BUCKET;
    const currentDate = new Date().toISOString().split('T')[0];

    const categories = ['LargeWithdrawal', 'PotentialScam', 'UserUsagePattern'];
    let report = {};

    for (const category of categories) {
        const params = {
            TableName: tableName,
            IndexName: 'Category-Date-index',
            KeyConditionExpression: '#category = :category and begins_with(#date, :date)',
            ExpressionAttributeNames: {
                '#category': 'Category',
                '#date': 'Date'
            },
            ExpressionAttributeValues: {
                ':category': category,
                ':date': currentDate
            }
        };

        try {
            const data = await dynamoDB.query(params).promise();
            console.log('data^^^^', data)
            report[category] = data.Items;
        } catch (error) {
            console.error(`Error querying ${category} data:`, error);
        }
    }

    const reportKey = `daily_report_${currentDate}.json`;
    const s3Params = {
        Bucket: dailyReportsBucket,
        Key: reportKey,
        Body: JSON.stringify(report),
        ContentType: 'application/json',
    };

    try {
        await s3.putObject(s3Params).promise();
        console.log(`Daily report generated: ${reportKey}`);
    } catch (error) {
        console.error('Error saving report to S3:', error);
    }
};
