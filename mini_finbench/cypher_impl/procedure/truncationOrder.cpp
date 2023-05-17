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

#include <algorithm>
#include <exception>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include "lgraph/lgraph.h"
#include "lgraph/lgraph_types.h"
#include "lgraph/lgraph_utils.h"
#include "tools/json.hpp"

using namespace lgraph_api;
using json = nlohmann::json;

extern "C" bool Process(GraphDB& db, const std::string& request, std::string& response) {
    static const std::string ACCOUNT_LABEL = "Account";
    static const std::string ACCOUT_ID = "id";
    static const std::string TIMESTAMP = "timestamp";
    json output;
    int64_t src, dst, start_time, end_time;
    float threshold;
    try {
        json input = json::parse(request);
        parse_from_json(src, "src", input);
        parse_from_json(dst, "dst", input);
    } catch (std::exception& e) {
        output["msg"] = "json parse error: " + std::string(e.what());
        response = output.dump();
        return false;
    }

    auto txn = db.CreateReadTxn();
    auto account_src = txn.GetVertexByUniqueIndex(ACCOUNT_LABEL, ACCOUT_ID, FieldData(src)).GetId();
    auto account_dst = txn.GetVertexByUniqueIndex(ACCOUNT_LABEL, ACCOUT_ID, FieldData(dst)).GetId();
    auto vit = txn.GetVertexIterator();
    vit.Goto(account_src);
    auto eit = vit.GetOutEdgeIterator(EdgeUid(0, 0, 0, 0, 0), true);
    std::vector<int64_t> ts_list;
    while (eit.IsValid() && eit.GetSrc() == account_src && eit.GetLabelId() == 0) {
        if (account_dst == eit.GetDst()) {
            ts_list.push_back(eit.GetField(TIMESTAMP).AsInt64());
        }
        eit.Next();
    }
    output = ts_list;
    response = output.dump();
    return true;
}
