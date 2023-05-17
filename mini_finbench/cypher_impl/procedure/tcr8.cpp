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
    static const std::string LOAN_LABEL = "Loan";
    static const std::string LOAN_ID = "id";
    static const std::string DEPOSIT_LABEL = "deposit";
    static const std::string TRANSFER_LABEL = "transfer";
    static const std::string TIMESTAMP = "timestamp";
    static const std::string AMOUNT = "amount";
    json output;
    int64_t id, start_time, end_time;
    float threshold;
    try {
        json input = json::parse(request);
        parse_from_json(id, "id", input);
        parse_from_json(threshold, "threshold", input);
        parse_from_json(start_time, "startTime", input);
        parse_from_json(end_time, "endTime", input);
    } catch (std::exception& e) {
        output["msg"] = "json parse error: " + std::string(e.what());
        response = output.dump();
        return false;
    }
    auto add_amount = [](std::unordered_map<int64_t, int64_t>& m, int64_t vid, int64_t amount) {
        if (vid != -1) {
            auto it = m.find(vid);
            if (it == m.end() || it->second > amount) {
                m.emplace(vid, amount);
            }
        }
    };
    auto add_dst =
        [](std::unordered_map<int64_t, std::unordered_map<int64_t, std::pair<int64_t, size_t>>>&
               merged_in,
           std::unordered_map<int64_t, int64_t>& m, int64_t dst, int64_t src, int64_t amount,
           float threshold, size_t hop) {
            if (dst != -1 || hop == 0) {
                auto min = m.find(src)->second;
                if (amount > min * threshold) {
                    auto it = merged_in.find(dst);
                    if (it == merged_in.end()) {
                        merged_in.emplace(dst,
                                          std::unordered_map<int64_t, std::pair<int64_t, size_t>>{
                                              {src, std::make_pair(amount, hop)}});
                    } else if (it->second.find(src) == it->second.end()) {
                        it->second.emplace(src, std::make_pair(amount, hop));
                    }
                }
            }
        };
    auto txn = db.CreateReadTxn();
    auto loan = txn.GetVertexByUniqueIndex(LOAN_LABEL, LOAN_ID, FieldData(id));
    auto vit = txn.GetVertexIterator();
    std::unordered_map<int64_t, std::unordered_map<int64_t, std::pair<int64_t, size_t>>> merged_in;
    std::unordered_map<int64_t, int64_t> min_amount;
    std::unordered_set<int64_t> src{loan.GetId()}, dst;
    for (size_t i = 0; i < 4; i++) {
        for (auto& vid : src) {
            vit.Goto(vid);
            auto eit = vit.GetOutEdgeIterator();
            int64_t last_vid = -1;
            int64_t last_amount = 0;
            while (eit.IsValid()) {
                if ((i == 0 && eit.GetLabel() == DEPOSIT_LABEL) ||
                    (i != 0 && eit.GetLabel() == TRANSFER_LABEL)) {
                    auto ts = eit.GetField(TIMESTAMP).AsInt64();
                    auto at = eit.GetField(AMOUNT).AsInt64();
                    auto dst_vid = eit.GetDst();
                    if (ts > start_time && ts < end_time) {
                        if (last_vid == dst_vid) {
                            last_amount += at;
                        } else {
                            add_amount(min_amount, last_vid, last_amount);
                            if (i != 0) {
                                add_dst(merged_in, min_amount, last_vid, vid, last_amount,
                                        threshold, i);
                            }
                            last_amount = at;
                            last_vid = dst_vid;
                            dst.emplace(dst_vid);
                        }
                    }
                }
                eit.Next();
            }
            if (last_vid != -1) {
                add_amount(min_amount, last_vid, last_amount);
                if (i != 0) {
                    add_dst(merged_in, min_amount, last_vid, vid, last_amount, threshold, i);
                }
                dst.emplace(last_vid);
            }
        }
        std::swap(src, dst);
        dst.clear();
    }
    std::vector<std::pair<std::pair<int64_t, size_t>, int64_t>> result;
    for (auto& kv1 : merged_in) {
        int64_t sum = 0;
        size_t hop = 0;
        for (auto& kv2 : kv1.second) {
            sum += kv2.second.first;
            hop = kv2.second.second;
        }
        result.push_back(std::make_pair(std::make_pair(sum, hop), kv1.first));
    }
    std::sort(result.begin(), result.end(),
              [=](std::pair<std::pair<int64_t, size_t>, int64_t>& l,
                  std::pair<std::pair<int64_t, size_t>, int64_t>& r) {
                  return l.first.second == r.first.second ? l.first.first > r.first.first
                                                          : l.first.second > r.first.second;
              });
    output = result;
    response = output.dump();
    return true;
}
