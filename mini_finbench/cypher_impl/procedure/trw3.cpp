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
    static const std::string PERSON_LABEL = "Person";
    static const std::string PERSON_ID = "id";
    static const std::string PERSON_ISBLOCKED = "isBlocked";
    static const std::string GUARANTEE_LABEL = "guarantee";
    static const std::string GUARANTEE_TIMESTAMP = "timestamp";
    static const std::string LOAN_LOANAMOUNT = "loanAmount";
    static const std::vector<std::string> GUARANTEE_FIELD_NAMES = {"timestamp"};
    static const std::string APPLY_LABEL = "apply";
    json output;
    output["txn"] = "abort";
    response = output.dump();
    int64_t src_id, dst_id, time, threshold, start_time, end_time;
    try {
        json input = json::parse(request);
        parse_from_json(src_id, "srcId", input);
        parse_from_json(dst_id, "dstId", input);
        parse_from_json(time, "time", input);
        parse_from_json(threshold, "threshold", input);
        parse_from_json(start_time, "startTime", input);
        parse_from_json(end_time, "endTime", input);
    } catch (std::exception& e) {
        output["msg"] = "json parse error: " + std::string(e.what());
        response = output.dump();
        return false;
    }
    auto txn = db.CreateWriteTxn();
    auto src = txn.GetVertexByUniqueIndex(PERSON_LABEL, PERSON_ID, FieldData(src_id));
    auto dst = txn.GetVertexByUniqueIndex(PERSON_LABEL, PERSON_ID, FieldData(dst_id));
    if (!src.IsValid() || !dst.IsValid()) {
        output["msg"] = "src/dst invalid";
        response = output.dump();
        txn.Abort();
        return false;
    }
    if (src.GetField(PERSON_ISBLOCKED).AsBool() || dst.GetField(PERSON_ISBLOCKED).AsBool()) {
        output["msg"] = "src/dst is blocked";
        response = output.dump();
        txn.Abort();
        return true;
    }
    txn.AddEdge(src.GetId(), dst.GetId(), GUARANTEE_LABEL, GUARANTEE_FIELD_NAMES,
                std::vector<FieldData>{FieldData(time)});

    // expand src
    std::unordered_set<int64_t> visited, dst_set, src_set{src.GetId()}, loans;
    while (!src_set.empty()) {
        for (auto& vid : src_set) {
            src.Goto(vid);
            auto eit = src.GetOutEdgeIterator();
            while (eit.IsValid()) {
                if (eit.GetLabel() == GUARANTEE_LABEL) {
                    auto ts = eit.GetField(GUARANTEE_TIMESTAMP).AsInt64();
                    if (ts > start_time && ts < end_time &&
                        visited.find(eit.GetDst()) == visited.end()) {
                        dst_set.emplace(eit.GetDst());
                        visited.emplace(eit.GetDst());
                    }
                }
                eit.Next();
            }
        }
        swap(src_set, dst_set);
        dst_set.clear();
    }
    int64_t sum_loan;
    for (auto& vid : visited) {
        src.Goto(vid);
        auto eit = src.GetOutEdgeIterator();
        while (eit.IsValid()) {
            if (eit.GetLabel() == APPLY_LABEL) {
                loans.emplace(eit.GetDst());
            }
            eit.Next();
        }
    }
    int64_t loan_sum = 0;
    for (auto& loan : loans) {
        src.Goto(loan);
        loan_sum += src.GetField(LOAN_LOANAMOUNT).AsInt64();
        if (loan_sum > threshold) {
            txn.Abort();
            break;
        }
    }
    if (txn.IsValid()) {
        output["msg"] = "not detected";
        output["txn"] = "commit";
        response = output.dump();
        txn.Commit();
        return true;
    }
    txn = db.CreateWriteTxn();
    src = txn.GetVertexByUniqueIndex(PERSON_LABEL, PERSON_ID, FieldData(src_id));
    dst = txn.GetVertexByUniqueIndex(PERSON_LABEL, PERSON_ID, FieldData(dst_id));
    if (!src.IsValid() || !dst.IsValid()) {
        txn.Abort();
        output["msg"] = "src/dst invalid";
        response = output.dump();
        return false;
    }
    src.SetField(PERSON_ISBLOCKED, FieldData(true));
    dst.SetField(PERSON_ISBLOCKED, FieldData(true));
    output["msg"] = "block src/dst";
    output["txn"] = "commit";
    response = output.dump();
    txn.Commit();
    return true;
}
