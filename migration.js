const { ObjectId } = require('mongoose').Types;
const _ = require('lodash');
const { Pool, Client } = require("pg");
const mongo = require("mongodb");
const cl = new mongo.MongoClient(                       //string for mongo connection
    "mongodb+srv://win:swag%400529@prod.znonx.mongodb.net/connect",
    { useUnifiedTopology: true }
);

const credentials = {                //credentials for postgres
    user: "financeadmin",
    host: "winuall-finance.postgres.database.azure.com",
    database: "Abhishek",
    password: "AbhishekDevs",
    port: 5432,
    ssl: true
}
const client = new Client(credentials);     //connection to postgres
client.connect();
/** Store */

const Loop = async (transferObj) => {
    for (var k = 0; k < transferObj.length; k++) {
        var col = "";
        var rows = [];
        var obj = transferObj[k];
        var objVal = Object.values(obj)
        for (var i = 0; i < objVal.length; i++) {
            if (typeof (objVal[i]) == "string" || typeof (objVal[i]) == "object") {
                if (objVal[i]) {
                    var parsed = `'${objVal[i]}'`
                    if (_.includes(parsed, 'T') && _.includes(parsed, 'G') && _.includes(parsed, '+')) {
                        parsed = moment(parsed).format('YYYY-MM-DD')
                        parsed = `'${parsed}'`
                        rows.push(parsed);
                    }
                    else {
                        var parsed = objVal[i];
                        parsed = parsed.toString().replace(/'/g, '');
                        parsed = `'${parsed}'`
                        rows.push(parsed);
                    }
                }
                else { rows.push("null"); }
            }
            else if (objVal[i] == null || (objVal[i]) == undefined) { rows.push("null") }
            else { rows.push(objVal[i]) }
        }
        var values = rows.toString(); var col = Object.keys(obj); var columN = col.toString()
        console.log(columN)
        console.log(values)

        //*******query to insert data in postgress ********//
        const result = await client.query(`INSERT into master_sheet (${columN}) VALUES(${values})`);
        console.log(result)
    }
    return ("done insertion")
}

async function run() {
    await cl.connect();
    const db = cl.db("connect");     //creating connection to mongo db
    console.log("mongodB connected")

    //*************latest id from postgres*************//
    const latestDateStore = await client.query(`SELECT reference_id FROM master_sheet where product_category = 'store' AND reference_id IS NOT NULL ORDER BY id DESC LIMIT 1;`)
    var referenceIdStore;
    if (!_.isEmpty(latestDateStore.rows)) { referenceIdStore = latestDateStore.rows[0].reference_id; }
    else { referenceIdStore = 0; }


    ////*********query to return data from mongo***********////
    let store = db.collection("mkt_carts")
    const transferObjStore = await store.aggregate([{
        "$match": {
            "creatorAmount": {
                "$exists": false
            },
            "created_at": {
                "$ne": "some"
            },
            "state": "complete",
            "razorpay_order_id": {
                "$exists": true
            },
            "_id": {
                "$gt": ObjectId(referenceIdStore)
            },
        }
    },
    {
        "$unwind": "$items"
    },
    {
        "$project": {
            "_id": 1,
            "amount": {
                "$sum": [
                    "$payable",
                    "$handling_fee"
                ]
            },
            "commission": {
                "$divide": [
                    "$commission",
                    100
                ]
            },
            "rp_fee": {
                "$divide": [
                    "$fee",
                    100
                ]
            },
            "rp_tax": {
                "$divide": [
                    "$tax",
                    100
                ]
            },
            "items": 1,
            "razorpay_payment_id": "$razorpay_payment_id",
            "orderId": "$razorpay_order_id",
            "created_at": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$created_at",
                    "timezone": "Asia/Kolkata"
                }
            },
            "defaultCreatedAT": "$created_at",
            "orgId": 1,
            "userId": 1,
            "paymentMode": "$paymentDetailsObject.paymenttype"
        }
    },
    {
        "$lookup": {
            "from": "users",
            "localField": "userId",
            "foreignField": "_id",
            "as": "user"
        }
    },
    {
        "$unwind": {
            "path": "$user",
            "preserveNullAndEmptyArrays": true
        }
    },
    {
        "$match": {
            "amount": {
                "$ne": 0
            },
            //        "created_at": {
            //            "$gte": "2022-03-01",
            //            "$lte": "2022-03-31"
            //        }
        }
    },
    {
        "$lookup": {
            "from": "organisations",
            "localField": "orgId",
            "foreignField": "_id",
            "as": "organisation"
        }
    },
    {
        "$unwind": {
            "path": "$organisation"
        }
    },
    {
        "$lookup": {
            "from": "csms",
            "localField": "organisation.csmId",
            "foreignField": "_id",
            "as": "csm"
        }
    },
    {
        "$unwind": {
            "path": "$csm",
            "preserveNullAndEmptyArrays": true
        }
    },
    {
        "$lookup": {
            "from": "mkt_packages",
            "localField": "items",
            "foreignField": "_id",
            "as": "packages"
        }
    },
    {
        "$unwind": "$packages"
    },
    {
        "$project": {
            "cartId": "$_id",
            "StudentName": "$user.name",
            "orderId": 1,
            "Institute": "$organisation.name",
            "orgCode": "$organisation.orgCode",
            "orgId": "$organisation._id",
            "orgCreatedAt": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$organisation.created_at",
                    "timezone": "Asia/Kolkata"
                }
            },
            "defaultCreatedAT": 1,
            "rp_fee": 1,
            "rp_tax": 1,
            "CoursePurchased": "$packages.title",
            "packageId": "$packages._id",
            "ProductListedPrice": "$packages.price",
            "Type": "$packages.type",
            "commission": "$commission",
            "PriceofCourse": {
                "$divide": [
                    "$amount",
                    100
                ]
            },
            "TransactionDate": "$created_at",
            "SalesorProduct": {
                "$cond": [
                    {
                        "$gt": [
                            "$packages.payable_cost",
                            19900
                        ]
                    },
                    "Sales",
                    "Product"
                ]
            },
            "CSMName": "$csm.name",
            "CSMPhone": "$csm.phone",
            "razorpay_payment_id": 1,
            "userId": 1,
            "paymentMode": "$paymentMode"
        }
    },
    {
        "$group": {
            "_id": {
                "cartId": "$cartId",
                "StudentName": "$StudentName",
                "Institute": "$Institute",
                "CSMName": "$CSMName",
                "orgCode": "$orgCode",
                "orgCreatedAt": "$orgCreatedAt",
                "defaultCreatedAT": "$defaultCreatedAT",
                "orgId": "$orgId",
                "PriceofCourse": "$PriceofCourse",
                "CSMPhone": "$CSMPhone",
                "TransactionDate": "$TransactionDate",
                "razorpay_payment_id": "$razorpay_payment_id",
                "commission": "$commission",
                "userId": "$userId",
                "paymentMode": "$paymentMode",
                "orderId": "$orderId",
                "rp_fee": "$rp_fee",
                "rp_tax": "$rp_tax",
            },
            "Type": {
                "$push": "$Type"
            },
            "CoursePurchased": {
                "$push": "$CoursePurchased"
            },
            "ProductListedPrice": {
                "$push": "$ProductListedPrice"
            },
            "packageId": {
                "$push": "$packageId"
            }
        }
    },
    {
        "$lookup": {
            "from": "transfers",
            "localField": "_id.razorpay_payment_id",
            "foreignField": "razorpayPaymentId",
            "as": "transfers"
        }
    },
    {
        "$lookup": {
            "from": "tutorfees",
            "localField": "_id.orgId",
            "foreignField": "orgId",
            "as": "tutorfees"
        }
    },
    {
        "$project": {
            "_id": 0,
            "orderId": "$_id.orderId",
            "orgId": "$_id.orgId",
            "orgCreatedAt": "$_id.orgCreatedAt",
            "defaultCreatedAT": "$_id.defaultCreatedAT",
            "referenceId": "$_id.cartId",
            "Institute": "$_id.Institute",
            "orgCode": "$_id.orgCode",
            "rp_fee": "$_id.rp_fee",
            "rp_tax": "$_id.rp_tax",
            "commission": "$_id.commission",
            "paymentMode": "$_id.paymentMode",
            "razorpay_payment_id": "$_id.razorpay_payment_id",
            "PriceofCourse": "$_id.PriceofCourse",
            "transactionDate": "$_id.TransactionDate",
            "units": { "$size": "$packageId" },
            transferId1: {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$type": "$transfers"
                            },
                            "array"
                        ]
                    },
                    "then": {
                        "$arrayElemAt": [
                            "$transfers",
                            0
                        ]
                    },
                    "else": null
                }
            },
            transferId2: {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$type": "$transfers"
                            },
                            "array"
                        ]
                    },
                    "then": {
                        "$arrayElemAt": [
                            "$transfers",
                            1
                        ]
                    },
                    "else": null
                }
            },
            "kam": "$_id.CSMName",
            "userId": "$_id.userId",
            "productType": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$type": "$Type"
                            },
                            "array"
                        ]
                    },
                    "then": {
                        "$arrayElemAt": [
                            "$Type",
                            0
                        ]
                    },
                    "else": "$Type"
                }
            },
            "ProductListedPrice": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$type": "$ProductListedPrice"
                            },
                            "array"
                        ]
                    },
                    "then": {
                        "$arrayElemAt": [
                            "$ProductListedPrice",
                            0
                        ]
                    },
                    "else": 0
                }
            },
            "productName": {
                "$cond": {
                    "if": {
                        "$eq": [
                            {
                                "$type": "$CoursePurchased"
                            },
                            "array"
                        ]
                    },
                    "then": {
                        "$arrayElemAt": [
                            "$CoursePurchased",
                            0
                        ]
                    },
                    "else": "$CoursePurchased"
                }
            },
            "planType": {
                "$filter": {
                    "input": "$tutorfees",
                    "as": "item",
                    "cond": {
                        $and: [
                            {
                                $eq: ["$$item.leadType",
                                    "New"]
                            },
                            { $eq: ['$$item.deletedAt', null] },
                        ],
                    }
                }
            }
        }
    },
    {
        "$lookup": {
            "from": "bankaccounts",
            "localField": "orgId",
            "foreignField": "orgId",
            "as": "bankaccounts"
        }
    },
    {
        "$project": {
            "_id": 0,
            "orgId": 1,
            "orgCreatedAt": 1,
            "bankaccount": {
                $filter: {
                    input: '$bankaccounts',
                    as: 'item',
                    cond: {
                        $and: [
                            { $ne: ['$$item.permanantAccountNumber', null] },
                            { $eq: ['$$item.deletedAt', null] },
                            { $ne: ['$$item.approved_at', null] },
                            { $gt: ['$$item.permanantAccountNumber', null] },
                        ],
                    },
                },
            },
            "defaultCreatedAT": 1,
            "referenceId": 1,
            "Institute": 1,
            "orderId": 1,
            "orgCode": 1,
            "units": 1,
            "rp_fee": 1,
            "rp_tax": 1,
            "commission": 1,
            "paymentMode": 1,
            "razorpay_payment_id": 1,
            "prodeuctSellingPrice": "$PriceofCourse",
            "productListedPrice": { "$divide": ["$ProductListedPrice", 100] },
            "transactionDate": 1,
            "transferId1": "$transferId1.transferId",
            "transferId2": "$transferId2.transferId",
            "transferfee1": {
                "$divide": [
                    "$transferId1.fee",
                    100
                ]
            },
            "transferfee2": {
                "$divide": [
                    "$transferId2.fee",
                    100
                ]
            },

            "transferTax1": {
                "$divide": [
                    "$transferId1.tax",
                    100
                ]
            },
            "transferTax2": {
                "$divide": [
                    "$transferId2.tax",
                    100
                ]
            },
            "transferDate1": {
                "$dateToString": {
                    "format": "%Y/%m/%d",
                    "date": "$transferId1.created_at",
                    "timezone": "Asia/Kolkata"
                }
            },
            "transferDate2": {
                "$dateToString": {
                    "format": "%Y/%m/%d",
                    "date": "$transferId2.created_at",
                    "timezone": "Asia/Kolkata"
                }
            },
            "transferredAmount1": {
                "$divide": [
                    "$transferId1.amount",
                    100
                ]
            },
            "transferredAmount2": {
                "$divide": [
                    "$transferId2.amount",
                    100
                ]
            },
            "productCategory": "Store",
            "kam": 1,
            "userId": 1,
            "productType": 1,
            "productName": 1,
            "planType": "$planType.paymentType",
            "first_paymentDate": "$planType.paymentDate"
        }
    },
    {
        "$unwind": {
            "path": "$planType",
            "preserveNullAndEmptyArrays": true
        }
    },
    {
        "$unwind": {
            "path": "$first_paymentDate",
            "preserveNullAndEmptyArrays": true
        }
    },
    { $sort: { "defaultCreatedAT": 1 } },
    {
        "$project": {
            "_id": 0,
            "org_id": "$orgId",
            "org_created_at": "$orgCreatedAt",
            "reference_id": "$referenceId",
            "org_name": "$Institute",
            "order_id": "$orderId",
            "org_code": "$orgCode",
            "units": 1,
            "commission": 1,
            "rp_fee": {
                $cond: {
                    if: { $eq: ['$rp_fee', null] },
                    then: 0,
                    else: "$rp_fee",
                },
            },
            "rp_tax": {
                $cond: {
                    if: { $eq: ['$rp_tax', null] },
                    then: 0,
                    else: "$rp_tax",
                },
            },
            "payment_mode": "$paymentMode",
            "payment_id": "$razorpay_payment_id",
            "product_selling_price": "$prodeuctSellingPrice",
            "product_listed_price": "$productListedPrice",
            "transaction_date": "$transactionDate",
            "transfer_id_1": {
                $cond: {
                    if: { $eq: ['$transferId1', null] },
                    then: "null",
                    else: "$transferId1",
                },
            },
            "transfer_id_2": {
                $cond: {
                    if: { $eq: ['$transferId2', null] },
                    then: "null",
                    else: "$transferId2",
                },
            },
            "transfer_fee_1": {
                $cond: {
                    if: { $eq: ['$transferfee1', null] },
                    then: 0,
                    else: "$transferfee1",
                },
            },
            "transfer_fee_2": {
                $cond: {
                    if: { $eq: ['$transferfee2', null] },
                    then: 0,
                    else: "$transferfee2",
                },
            },
            "transfer_tax_1": {
                $cond: {
                    if: { $eq: ['$transferTax1', null] },
                    then: 0,
                    else: "$transferTax1",
                },
            },
            "transfer_tax_2": {
                $cond: {
                    if: { $eq: ['$transferTax2', null] },
                    then: 0,
                    else: "$transferTax2",
                },
            },
            "transferred_amount_1": {
                $cond: {
                    if: { $eq: ['$transferredAmount1', null] },
                    then: 0,
                    else: "$transferredAmount1",
                },
            },
            "transferred_amount_2": {
                $cond: {
                    if: { $eq: ['$transferredAmount2', null] },
                    then: 0,
                    else: "$transferredAmount2",
                },
            },
            "product_category": "store",
            "kam": 1,
            "user_id": "$userId",
            "product_type": "$productType",
            "product_name": "$productName",
            "plan_type": "$planType",
            "first_payment_date": "$first_paymentDate",
            "is_cross_sell": "false",
            "is_pan": {
                $cond: {
                    if: { $eq: ['$bankaccount', []] },
                    then: "false",
                    else: "true",
                },
            },
            "is_updated": "false"
        }
    }
    ]).toArray();
    console.log("next", transferObjStore.length)
    // let transferObj;
    // let length

    if (!_.isEmpty(transferObjStore)) {
        length = transferObjStore.length
        transferObj = transferObjStore;
        const data = await Loop(transferObj);
    }
    console.log("done")
}
run()
