/*
 *  The code is part of our project called DrTM, which leverages HTM and RDMA
 * for speedy distributed in-memory transactions.
 *
 *
 * Copyright (C) 2015 Institute of Parallel and Distributed Systems (IPADS),
 * Shanghai Jiao Tong University All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more about this software, visit:  http://ipads.se.sjtu.edu.cn/drtm.html
 *
 */
// ljh 该文件关于it的String和unordered MAP部分尚未改
#ifndef NETWORK_NODE_H
#define NETWORK_NODE_H

#include <errno.h>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>
#include <unordered_map>
// #include <zmq.hpp>

int hash(int _pid, int _nid) { return _pid * 200 + _nid; }

struct Network_Node {
    size_t total_partition;
    int pid;
    int nid;
    zmq::context_t context;
    zmq::socket_t *receiver;

    std::vector<std::string> net_def;
    std::unordered_map<int, zmq::socket_t *> socket_map;
};

Network_Node *Network_Node_new(size_t _total_partition, int _pid, int _nid) {
    Network_Node *it = (Network_Node *)malloc(sizeof(Network_Node));
    it->nid = _nid;
    it->pid = _pid;
    it->context = 1;

    it->total_partition = _total_partition;
    fprintf(stdout, "start %d %d listening...\n", _pid, _nid);

    fprintf(stdout, "using default network fun\n");
    it->net_def.push_back("10.0.0.100");
    it->net_def.push_back("10.0.0.101");
    it->net_def.push_back("10.0.0.102");
    it->net_def.push_back("10.0.0.103");
    it->net_def.push_back("10.0.0.104");
    it->net_def.push_back("10.0.0.105");

    it->receiver = new zmq::socket_t(context, ZMQ_PULL);
    char address[30] = "";
    sprintf(address, "tcp://*:%d", 5500 + hash(it->pid, it->nid));
    fprintf(stdout, "tcp binding address %s\n", address);
    it->receiver->bind(address);
    fprintf(stdout, "init netpoint done\n");
    return it;
}

void Network_Node_free(Network_Node *it) {
    for (auto iter : it->socket_map) {
        if (iter.second != NULL) {
            free(&iter.second);
            iter.second = NULL;
        }
    }
    // 释放receiver空间
    free(&it->receiver);
}
void Send(Network_Node *it, int _pid, int _nid, std::string msg) {
    std::string header = "00";
    header[0] = it->pid;
    header[1] = it->nid;
    msg = header + msg;
    int id = hash(_pid, _nid);
    if (socket_map.find(id) == socket_map.end()) {
        socket_map[id] = new zmq::socket_t(context, ZMQ_PUSH);
        char address[30] = "";

        assert(_pid < total_partition);
        snprintf(address, 30, "tcp://%s:%d", net_def[_pid].c_str(), 5500 + id);
        fprintf(stdout, "mul estalabish %s\n", address);

        socket_map[id]->connect(address);
    }
    zmq::message_t request(msg.length());
    memcpy((void *)request.data(), msg.c_str(), msg.length());
    socket_map[id]->send(request);
}

std::string Recv(Network_Node *it, ) {
    zmq::message_t reply;
    if (it->receiver->recv(&reply) < 0) {
        fprintf(stderr, "recv with error %s\n", strerror(errno));
        exit(-1);
    }
    return std::string((char *)reply.data(), reply.size());
}

std::string tryRecv(Network_Node *it, ) {
    zmq::message_t reply;
    if (it->receiver->recv(&reply, ZMQ_NOBLOCK))
        return std::string((char *)reply.data(), reply.size());
    else
        return "";
}

#endif
