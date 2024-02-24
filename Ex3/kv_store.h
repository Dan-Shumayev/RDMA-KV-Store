
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h>
#include <sys/time.h>
#include <stdlib.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <sys/time.h>
#include <infiniband/verbs.h>
/****/
#define OVERRIDE_BUFF_SIZE (1024*1024)
#define CLIENT_WARMUP_MSG_AMOUNT  (10000) // Notice it has to be lower than tx_depth since we're checking only in
// multiplications of tx_depth
#define BUFFER_PARTITION_SIZE 8192
#define KEY_MAX_SIZE 4015
#define CLIENT_MSG_AMOUNT  (10000)        // We wanted to normalize and smooth the results using an enormous amount of
// packets to send for each size, normalizing network hiccups.
#define EXP_AMOUNT (21)
#define MESSAGE_LENGTH (2000)
#define CHECK_COMPLETION_ITERS (20)
#define SEND_RECV_THRESHOLD (101)
#define SET_PREFIX "key-value:"
#define SET_RENDEZVOUS "rendezvous:"
#define GET_PREFIX "key:"
#define GET_RESULT_PREFIX "get-%s:"
/****/

#define WC_BATCH (1)

enum
{
    PINGPONG_RECV_WRID = 1,
    PINGPONG_SEND_WRID = 2,
};

static int page_size;


struct pingpong_context
{
    struct ibv_context *context;
    struct ibv_comp_channel *channel;
    struct ibv_pd *pd;
    struct ibv_mr *mr;
    struct ibv_cq *cq;
    struct ibv_qp *qp;
    char *buf;
    int size;
    int rx_depth;
    int routs;
    void* write_buf;
    struct ibv_mr *write_mr;
    void* read_buf;
    struct ibv_mr *read_mr;
    struct ibv_port_attr portinfo;
};

struct pingpong_dest
{
    int lid;
    int qpn;
    int psn;
    union ibv_gid gid;
};
typedef struct map_item {
    char* key;
    char* value;
    int large;
    struct ibv_mr* mr;
} map_item;
typedef struct kv_handle {
    struct pingpong_context *ctx;
    int ib_port;
    int dset_psn;
    enum ibv_mtu ibvMtu;
    int sl;
    struct pingpong_dest *rem_dest;
    int gidx;
} kv_handle;

typedef struct work_request_buffer
{
    char* buf;
}work_request_buffer;
int kv_open(char *servername, void **kv_handle); /*Connect to server*/
int kv_set(void *kv_handle, const char *key, const char *value);
int kv_get(void *kv_handle, const char *key, char **value);
void kv_release(char *value);/* Called after get() on value pointer */
int kv_close(void *kv_handle); /* Destroys the QP */