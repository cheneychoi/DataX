package com.alibaba.datax.plugin.reader.mongodbreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.mongodbreader.util.AESUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.CollectionSplitUtil;
import com.alibaba.datax.plugin.reader.mongodbreader.util.MongoUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by jianying.wcj on 2015/3/19 0019.
 * Modified by mingyan.zc on 2016/6/13.
 * Modified by mingyan.zc on 2017/7/5.
 */
public class MongoDBReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig = null;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;

        @Override
        public List<Configuration> split(int adviceNumber) {
            return CollectionSplitUtil.doSplit(originalConfig, adviceNumber, mongoClient);
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.userName = originalConfig.getString(KeyConstant.MONGO_USER_NAME, originalConfig.getString(KeyConstant.MONGO_USERNAME));
            this.password = originalConfig.getString(KeyConstant.MONGO_USER_PASSWORD, originalConfig.getString(KeyConstant.MONGO_PASSWORD));
            String database = originalConfig.getString(KeyConstant.MONGO_DB_NAME, originalConfig.getString(KeyConstant.MONGO_DATABASE));
            String authDb = originalConfig.getString(KeyConstant.MONGO_AUTHDB, database);
            if (!Strings.isNullOrEmpty(this.userName) && !Strings.isNullOrEmpty(this.password)) {
                this.mongoClient = MongoUtil.initCredentialMongoClient(originalConfig, userName, password, authDb);
            } else {
                this.mongoClient = MongoUtil.initMongoClient(originalConfig);
            }
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Reader.Task {

        private Configuration readerSliceConfig;

        private MongoClient mongoClient;

        private String userName = null;
        private String password = null;
        private String aesKey = null;

        private String authDb = null;
        private String database = null;
        private String collection = null;
        private JSONArray encryptField = null;

        private String query = null;

        private JSONArray mongodbColumnMeta = null;
        private Object lowerBound = null;
        private Object upperBound = null;
        private boolean isObjectId = true;

        @Override
        public void startRead(RecordSender recordSender) {
            if (lowerBound == null || upperBound == null ||
                    mongoClient == null || database == null ||
                    collection == null || mongodbColumnMeta == null || encryptField == null) {
                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
                        MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }
            MongoDatabase db = mongoClient.getDatabase(database);
            MongoCollection col = db.getCollection(this.collection);

            MongoCursor<Document> dbCursor = null;
            Document filter = new Document();
            if (lowerBound.equals("min")) {
                if (!upperBound.equals("max")) {
                    filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
                }
            } else if (upperBound.equals("max")) {
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound));
            } else {
                filter.append(KeyConstant.MONGO_PRIMARY_ID, new Document("$gte", isObjectId ? new ObjectId(lowerBound.toString()) : lowerBound).append("$lt", isObjectId ? new ObjectId(upperBound.toString()) : upperBound));
            }
            if (!Strings.isNullOrEmpty(query)) {
                Document queryFilter = Document.parse(query);
                filter = new Document("$and", Arrays.asList(filter, queryFilter));
            }
            dbCursor = col.find(filter).iterator();
            List<String> parse = JSONObject.parseArray(encryptField.toJSONString(), String.class);
            while (dbCursor.hasNext()) {
                Document item = dbCursor.next();
                String s = item.toJson();
                JSONObject jsonObject = JSONObject.parseObject(s);
//                System.out.println(s);
                Record record = recordSender.createRecord();
                Iterator columnItera = mongodbColumnMeta.iterator();
                while (columnItera.hasNext()) {
                    JSONObject column = (JSONObject) columnItera.next();
                    String columnName = column.getString(KeyConstant.COLUMN_NAME);
                    String tempCol = jsonObject.getString(columnName);
//                    if (tempCol == null) {
//                        if (KeyConstant.isDocumentType(column.getString(KeyConstant.COLUMN_TYPE))) {
//                            String[] name = columnName.split("\\.");
//                            if (name.length > 1) {
//                                Object obj;
//                                Document nestedDocument = item;
//                                for (String str : name) {
//                                    obj = nestedDocument.get(str);
//                                    if (obj instanceof Document) {
//                                        nestedDocument = (Document) obj;
//                                    }
//                                }
//
//                                if (null != nestedDocument) {
//                                    Document doc = nestedDocument;
//                                    tempCol = doc.get(name[name.length - 1]);
//                                }
//                            }
//                        }

                    if (tempCol != null) {
                        tempCol = tempCol.replaceAll("\r\n|\r|\n", " ");
                        for (String field : parse) {
                            if (field.equals(columnName)) {
                                tempCol = AESUtil.encryptString(tempCol, aesKey);
                            }
                        }
                    }

//                    } else if (tempCol instanceof Double) {
//                        //TODO deal with Double.isNaN()
//                        record.addColumn(new DoubleColumn((Double) tempCol));
//                    } else if (tempCol instanceof Boolean) {
//                        record.addColumn(new BoolColumn((Boolean) tempCol));
//                    } else if (tempCol instanceof Date) {
//                        record.addColumn(new DateColumn((Date) tempCol));
//                    } else if (tempCol instanceof Integer) {
//                        record.addColumn(new LongColumn((Integer) tempCol));
//                    } else if (tempCol instanceof Long) {
//                        record.addColumn(new LongColumn((Long) tempCol));
//                    } else {
//                        if (KeyConstant.isArrayType(column.getString(KeyConstant.COLUMN_TYPE))) {
//                            String splitter = column.getString(KeyConstant.COLUMN_SPLITTER);
//                            if (Strings.isNullOrEmpty(splitter)) {
//                                throw DataXException.asDataXException(MongoDBReaderErrorCode.ILLEGAL_VALUE,
//                                        MongoDBReaderErrorCode.ILLEGAL_VALUE.getDescription());
//                            } else {
//                                ArrayList array = (ArrayList) tempCol;
//                                String tempArrayStr = Joiner.on(splitter).join(array);
//                                record.addColumn(new StringColumn(tempArrayStr));
                    if (tempCol == null) {
                        //continue; 这个不能直接continue会导致record到目的端错位
                        record.addColumn(new StringColumn(null));
                    } else {
                        record.addColumn(new StringColumn(tempCol));
                    }
                }
            recordSender.sendToWriter(record);
        }
    }

    @Override
    public void init() {
        this.readerSliceConfig = super.getPluginJobConf();
        this.userName = readerSliceConfig.getString(KeyConstant.MONGO_USER_NAME, readerSliceConfig.getString(KeyConstant.MONGO_USERNAME));
        this.password = readerSliceConfig.getString(KeyConstant.MONGO_USER_PASSWORD, readerSliceConfig.getString(KeyConstant.MONGO_PASSWORD));
        this.database = readerSliceConfig.getString(KeyConstant.MONGO_DB_NAME, readerSliceConfig.getString(KeyConstant.MONGO_DATABASE));
        this.authDb = readerSliceConfig.getString(KeyConstant.MONGO_AUTHDB, this.database);
        if (!Strings.isNullOrEmpty(userName) && !Strings.isNullOrEmpty(password)) {
            mongoClient = MongoUtil.initCredentialMongoClient(readerSliceConfig, userName, password, authDb);
        } else {
            mongoClient = MongoUtil.initMongoClient(readerSliceConfig);
        }

        this.encryptField = JSON.parseArray(readerSliceConfig.getString("encryptField"));
        this.collection = readerSliceConfig.getString(KeyConstant.MONGO_COLLECTION_NAME);
        this.query = readerSliceConfig.getString(KeyConstant.MONGO_QUERY);
        this.mongodbColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.MONGO_COLUMN));
        this.lowerBound = readerSliceConfig.get(KeyConstant.LOWER_BOUND);
        this.aesKey = readerSliceConfig.getString("aesKey");
        this.upperBound = readerSliceConfig.get(KeyConstant.UPPER_BOUND);
        this.isObjectId = readerSliceConfig.getBool(KeyConstant.IS_OBJECTID);
    }

    @Override
    public void destroy() {

    }

}

    public static void main(String[] args) {
        Object x = "{\n" +
                "    \"_id\" : ObjectId(\"60dc819bd44b1d00b47eafc0\"),\n" +
                "    \"accountId\" : ObjectId(\"609897549104ab069e4589ac\"),\n" +
                "    \"createdAt\" : ISODate(\"2021-06-30T14:37:15.573Z\"),\n" +
                "    \"isDeleted\" : false,\n" +
                "    \"title\" : \"邀请有礼\",\n" +
                "    \"description\" : \"成功邀请5名新用户注册商城会员，即可获得邀请有礼0元每日C橙汁1L*3瓶专享券（满71.9元减71.9元），优惠券有效期3天；被邀请者可获得全场满99减20元优惠券，优惠券有效期7天； 请在优惠券有效期内使用，过期概不负责。\",\n" +
                "    \"type\" : \"FISSION\",\n" +
                "    \"tags\" : [ \n" +
                "        \"群脉零售邀请有礼\"\n" +
                "    ],\n" +
                "    \"period\" : {\n" +
                "        \"startAt\" : ISODate(\"2021-11-25T05:51:49.000Z\"),\n" +
                "        \"endAt\" : ISODate(\"2022-12-26T15:08:13.925Z\")\n" +
                "    },\n" +
                "    \"status\" : \"Document{{textMessage=Document{{template=, smsTopic=味全官方商城}}}\",\n" +
                "    \"settings\" : [ \n" +
                "        {\n" +
                "            \"id\" : ObjectId(\"60dc819bd44b1d00b47eafc1\"),\n" +
                "            \"rule\" : {\n" +
                "                \"counts\" : [ \n" +
                "                    {\n" +
                "                        \"name\" : \"newMemberActivatedReward\",\n" +
                "                        \"count\" : NumberLong(1),\n" +
                "                        \"resetAfterReward\" : true\n" +
                "                    }\n" +
                "                ]\n" +
                "            },\n" +
                "            \"reward\" : {\n" +
                "                \"coupons\" : [ \n" +
                "                    {\n" +
                "                        \"id\" : ObjectId(\"61960a1badb59500fd84cc3b\"),\n" +
                "                        \"name\" : \"全场满99减20元\"\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"score\" : NumberLong(0)\n" +
                "            },\n" +
                "            \"times\" : NumberLong(1)\n" +
                "        }, \n" +
                "        {\n" +
                "            \"id\" : ObjectId(\"60dc819bd44b1d00b47eafc2\"),\n" +
                "            \"rule\" : {\n" +
                "                \"counts\" : [ \n" +
                "                    {\n" +
                "                        \"name\" : \"inviteHelperCount\",\n" +
                "                        \"count\" : NumberLong(5),\n" +
                "                        \"resetAfterReward\" : true\n" +
                "                    }\n" +
                "                ]\n" +
                "            },\n" +
                "            \"reward\" : {\n" +
                "                \"coupons\" : [ \n" +
                "                    {\n" +
                "                        \"id\" : ObjectId(\"619f1080b07dac00ff3a8741\"),\n" +
                "                        \"name\" : \"邀请有礼0元每日C橙汁1L*3瓶优惠券\"\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"score\" : NumberLong(0)\n" +
                "            },\n" +
                "            \"times\" : NumberLong(0)\n" +
                "        }\n" +
                "    ],\n" +
                "    \"extra\" : {\n" +
                "        \"posterText\" : \"味全商城小程序营业中~欢迎大家来做客！\"";
        Document doc = (Document) x;
        String s = doc.toJson();
        System.out.println(s);
//        String x = "{\n" +
//                "    \"_id\" : ObjectId(\"6167da3e3e544c2e2a08dd41\"),\n" +
//                "    \"accountId\" : ObjectId(\"609897549104ab069e4589ac\"),\n" +
//                "    \"type\" : \"score_purchase\",\n" +
//                "    \"total\" : 1,\n" +
//                "    \"used\" : 1,\n" +
//                "    \"remained\" : 0,\n" +
//                "    \"reason\" : \"积分兑换\",\n" +
//                "    \"campaignId\" : ObjectId(\"6165655b4b728730ca40cf03\"),\n" +
//                "    \"member\" : {\n" +
//                "        \"id\" : ObjectId(\"6160e7e9b2d11400d1142ed3\"),\n" +
//                "        \"name\" : \"储园园\",\n" +
//                "        \"avatar\" : \"https://statics.maiscrm.com/609897549104ab069e4589ac/modules/followers/olvCr5MY45GRtm0oT6F4AESm18y8/a034c3b2-ad8a-18f3-8eb7-8e0a7dac946c.jpg\",\n" +
//                "        \"nickname\" : \"储园园\",\n" +
//                "        \"channelAvatar\" : \"https://statics.maiscrm.com/avatar/wechat/aHR0cHM6Ly90aGlyZHd4LnFsb2dvLmNuL21tb3Blbi92aV8zMi9mcHdabWhNa1VVaWNBR2F1YnhUTUliNjc1aDVpYlJySDk0WWlhN3R3N3h4VjBvOHdLTmp0Y21vb2wyNHpSdk1OdmptaG5ldFBtMTZqN0lDNHlxd1phYVdGZy8xMzI=\",\n" +
//                "        \"openId\" : \"olvCr5MY45GRtm0oT6F4AESm18y8\",\n" +
//                "        \"channelId\" : \"60adbd7eaf430bf9adbcdc79\"\n" +
//                "    },\n" +
//                "    \"createdAt\" : ISODate(\"2021-10-14T07:20:30.527Z\"),\n" +
//                "    \"updatedAt\" : ISODate(\"2021-10-14T07:20:30.527Z\"),\n" +
//                "    \"isDeleted\" : false\n" +
//                "}";
//        Matcher matcher = OBJECTID.matcher(x);
//        matcher.find();
//        String group = matcher.group(2);
//        String s = matcher.replaceAll(group);
//        System.out.println(s);

    }
}
