#include <zmq.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include "uthash.h"

#define MUL

extern size_t total_partition;
extern size_t current_partition;

typedef struct {
    int pid;
    int nid;
    void *context;
    void *receiver;
    char **net_def;
    int net_def_count;
    struct SocketMapEntry *socket_map;
} Network_Node;

typedef struct SocketMapEntry {
    int id;
    void *socket;
    UT_hash_handle hh;
} SocketMapEntry;

int hash(int _pid, int _nid) {
    return _pid * 200 + _nid;
}

Network_Node* Network_Node_new(int _pid, int _nid, const char *conf) {
    Network_Node *node = malloc(sizeof(Network_Node));
    node->nid = _nid;
    node->pid = _pid;
    node->context = zmq_ctx_new();
    node->net_def = NULL;
    node->net_def_count = 0;
    node->socket_map = NULL;

    printf("start %d %d listening...\n", _pid, _nid);
    FILE *ist = fopen(conf, "r");
    if (ist) {
        char mac[100];
        while (fgets(mac, sizeof(mac), ist)) {
            mac[strcspn(mac, "\n")] = 0; // Remove newline
            node->net_def = realloc(node->net_def, sizeof(char*) * (node->net_def_count + 1));
            node->net_def[node->net_def_count] = strdup(mac);
            node->net_def_count++;
        }
    } else {
        printf("using default network fun %d %d\n", _pid, _nid);
        const char *defaults[] = {"10.0.0.100", "10.0.0.101", "10.0.0.102", "10.0.0.103", "10.0.0.104", "10.0.0.105"};
        int defaults_count = sizeof(defaults) / sizeof(defaults[0]);
        node->net_def = malloc(sizeof(char*) * defaults_count);
        for (int i = 0; i < defaults_count; i++) {
            node->net_def[i] = strdup(defaults[i]);
        }
        node->net_def_count = defaults_count;
    }
    fclose(ist);

    node->receiver = zmq_socket(node->context, ZMQ_PULL);
    char address[30] = "";
    sprintf(address, "tcp://*:%d", 5500 + hash(node->pid, node->nid));
    printf("tcp binding address %s\n", address);
    zmq_bind(node->receiver, address);

    return node;
}

void Network_Node_destroy(Network_Node *node) {
    SocketMapEntry *current, *tmp;
    HASH_ITER(hh, node->socket_map, current, tmp) {
        HASH_DEL(node->socket_map, current);
        zmq_close(current->socket);
        free(current);
    }
    zmq_close(node->receiver);
    zmq_ctx_destroy(node->context);
    for (int i = 0; i < node->net_def_count; i++) {
        free(node->net_def[i]);
    }
    free(node->net_def);
    free(node);
}

void Network_Node_Send(Network_Node *node, int _pid, int _nid, const char *msg) {
    char header[3] = {node->pid, node->nid, 0};
    char *full_msg = malloc(strlen(header) + strlen(msg) + 1);
    strcpy(full_msg, header);
    strcat(full_msg, msg);
    int id = hash(_pid, _nid);
    SocketMapEntry *s;
    HASH_FIND_INT(node->socket_map, &id, s);
    if (!s) {
        s = malloc(sizeof(SocketMapEntry));
        s->id = id;
        s->socket = zmq_socket(node->context, ZMQ_PUSH);
        char address[30] = "";
#ifdef MUL
        snprintf(address, 30, "tcp://%s:%d", node->net_def[_pid], 5500 + id);
        printf("mul establish %s\n", address);
#else
        sprintf(address, "tcp://10.0.0.%d:%d", 100 + _pid, 5500 + id);
#endif
        zmq_connect(s->socket, address);
        HASH_ADD_INT(node->socket_map, id, s);
    }
    zmq_msg_t request;
    zmq_msg_init_size(&request, strlen(full_msg));
    memcpy(zmq_msg_data(&request), full_msg, strlen(full_msg));
    zmq_msg_send(&request, s->socket, 0);
    zmq_msg_close(&request);
    free(full_msg);
}

char* Network_Node_Recv(Network_Node *node) {
    zmq_msg_t reply;
    zmq_msg_init(&reply);
    if (zmq_msg_recv(&reply, node->receiver, 0) < 0) {
        fprintf(stderr, "recv with error %s\n", strerror(errno));
        exit(-1);
    }
    char *response = strndup((char *)zmq_msg_data(&reply), zmq_msg_size(&reply));
    zmq_msg_close(&reply);
    return response;
}

char* Network_Node_tryRecv(Network_Node *node) {
    zmq_msg_t reply;
    zmq_msg_init(&reply);
    if (zmq_msg_recv(&reply, node->receiver, ZMQ_DONTWAIT) > 0) {
        char *response = strndup((char *)zmq_msg_data(&reply), zmq_msg_size(&reply));
        zmq_msg_close(&reply);
        return response;
    } else {
        zmq_msg_close(&reply);
        return strdup("");
    }
}


