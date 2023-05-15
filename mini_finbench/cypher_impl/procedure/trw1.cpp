/**
 * Copyright 2022 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

#include <exception>
#include <iostream>
#include <unordered_set>
#include "lgraph/lgraph.h"
#include "lgraph/lgraph_types.h"
#include "lgraph/lgraph_utils.h"
#include "tools/json.hpp"
#include <boost/stacktrace.hpp>

using namespace lgraph_api;
using json = nlohmann::json;

extern "C" bool Process(GraphDB& db, const std::string& request, std::string& response) {
    static const std::string ACCOUNT_LABEL = "Account";
    static const std::string ACCOUNT_ID = "id";
    static const std::string ACCOUNT_ISBLOCKED = "isBlocked";
    static const std::string TRANSFER_LABEL = "transfer";
    static const std::string TRANSFER_TIMESTAMP = "timestamp";
    static const std::vector<std::string> TRANSFER_FIELD_NAMES = {"timestamp", "amount", "type"};
    static const std::string DEFAULT_TRANSFER_TYPE = "tp1";
    json output;
    output["txn"] = "abort";
    response = output.dump();
    int64_t src_id, dst_id, time, amt, start_time, end_time;
    try {
        json input = json::parse(request);
        parse_from_json(src_id, "srcId", input);
        parse_from_json(dst_id, "dstId", input);
        parse_from_json(time, "time", input);
        parse_from_json(amt, "amt", input);
        parse_from_json(start_time, "startTime", input);
        parse_from_json(end_time, "endTime", input);
    } catch (std::exception& e) {
        output["msg"] = "json parse error: " + std::string(e.what());
        response = output.dump();
        return false;
    }
    auto txn = db.CreateWriteTxn();
    auto src = txn.GetVertexByUniqueIndex(ACCOUNT_LABEL, ACCOUNT_ID, FieldData(src_id));
    auto dst = txn.GetVertexByUniqueIndex(ACCOUNT_LABEL, ACCOUNT_ID, FieldData(dst_id));
    if (!src.IsValid() || !dst.IsValid()) {
        output["msg"] = "src/dst invalid";
        response = output.dump();
        txn.Abort();
        return false;
    }
    if (src.GetField(ACCOUNT_ISBLOCKED).AsBool() || dst.GetField(ACCOUNT_ISBLOCKED).AsBool()) {
        output["msg"] = "src/dst is blocked";
        response = output.dump();
        txn.Abort();
        return true;
    }
    txn.AddEdge(
        src.GetId(), dst.GetId(), TRANSFER_LABEL, TRANSFER_FIELD_NAMES,
        std::vector<FieldData>{FieldData(time), FieldData(amt), FieldData(DEFAULT_TRANSFER_TYPE)});
    std::unordered_set<int64_t> src_in;
    auto src_eit = src.GetInEdgeIterator();
    while (src_eit.IsValid()) {
        auto ts = src_eit.GetField(TRANSFER_TIMESTAMP);
        if (src_eit.GetLabel() == TRANSFER_LABEL && ts.AsInt64() > start_time &&
            ts.AsInt64() < end_time) {
            src_in.emplace(src_eit.GetSrc());
        }
        src_eit.Next();
    }
    if (src_in.empty()) {
        output["msg"] = "not detected";
        output["txn"] = "commit";
        response = output.dump();
        txn.Commit();
        return true;
    }
    auto dst_eit = dst.GetOutEdgeIterator();
    while (dst_eit.IsValid()) {
        auto ts = dst_eit.GetField(TRANSFER_TIMESTAMP);
        if (dst_eit.GetLabel() == TRANSFER_LABEL && ts.AsInt64() > start_time &&
            ts.AsInt64() < end_time && src_in.find(dst_eit.GetDst()) != src_in.end()) {
            txn.Abort();
            break;
        }
        dst_eit.Next();
    }
    if (txn.IsValid()) {
        output["msg"] = "not detected";
        output["txn"] = "commit";
        response = output.dump();
        txn.Commit();
        return true;
    }
    txn = db.CreateWriteTxn();
    src = txn.GetVertexByUniqueIndex(ACCOUNT_LABEL, ACCOUNT_ID, FieldData(src_id));
    dst = txn.GetVertexByUniqueIndex(ACCOUNT_LABEL, ACCOUNT_ID, FieldData(dst_id));
    if (!src.IsValid() || !dst.IsValid()) {
        txn.Abort();
        output["msg"] = "src/dst invalid";
        response = output.dump();
        return false;
    }
    src.SetField(ACCOUNT_ISBLOCKED, FieldData(true));
    dst.SetField(ACCOUNT_ISBLOCKED, FieldData(true));
    output["msg"] = "block src/dst";
    output["txn"] = "commit";
    response = output.dump();
    txn.Commit();
    return true;
}
