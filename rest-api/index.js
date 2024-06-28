const AWS = require('aws-sdk');
const dynamodb = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
    const queryParams = event.queryStringParameters;
    const tableName = process.env.DYNAMODB_TABLE_NAME;

    if (!queryParams || !queryParams.category || !queryParams.date) {
        return {
            statusCode: 400,
            body: JSON.stringify({ error: 'Missing required query parameters' }),
        };
    }

    const category = queryParams.category;
    const date = queryParams.date;
    const fields = queryParams.fields;

    const expressionAttributeNames = {
        '#category': 'Category',
        '#date': 'Date'
    };
    const expressionAttributeValues = {
        ':category': category,
        ':date': date
    };

    let params = {};
    let result = {};
    let ProjectionExpression = null;

    if (fields) {
        const projectionFields = [];
        const fieldsArray = fields.split(',');
        console.log(fieldsArray)

        fieldsArray.forEach(field => {
            projectionFields.push(`#${field}`);
            expressionAttributeNames[`#${field}`] = field;
        });

        ProjectionExpression = projectionFields.join(', ');
    }

    

    if (queryParams.category && queryParams.date) {
        params = {
            TableName: tableName,
            IndexName: 'Category-Date-index',
            ExpressionAttributeNames: expressionAttributeNames,
            ExpressionAttributeValues: expressionAttributeValues,
            KeyConditionExpression: '#category = :category and begins_with(#date, :date)',
        };
        if (ProjectionExpression) {
            params.ProjectionExpression = ProjectionExpression;
        }
        result = await dynamodb.query(params).promise();
    } else {
        return {
            statusCode: 400,
            body: JSON.stringify({ error: 'Invalid query parameter' }),
        };
    }

    return {
        statusCode: 200,
        body: JSON.stringify(result.Items),
    };
};
