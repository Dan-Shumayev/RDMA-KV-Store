
#include "kv_store.h"
//The number of regestries in the server database
int registeries = 0;
//A list of pointers to map item structs which holds the database data
map_item **map_item_list;
work_request_buffer **wr_buffer_list;
//The client handler struct
kv_handle *handler;
// defualt vm from where client reads the value
uint64_t remote_get_addr;
//For client: holds the vm address from where to read a get request
//For server: pointer to ctx->buf[100] which holds the address of a client get request
uint64_t remote_read_get_addr;
//For client: holds the vm address from where to write into the database
//For server: pointer to ctx->buf[101] which holds the address to where the client writes for set request
uint64_t remote_read_write_addr;
//Holds the r_key to server's buffer
uint32_t remote_get_key;
//uint64_t next_set;
uint64_t curr_rendezvous;

char *servername;
static void usage(const char *argv0)
{
    printf("Usage:\n");
    printf("  %s            start a server and wait for connection\n", argv0);
    printf("  %s <host>     connect to server at <host>\n", argv0);
    printf("\n");
    printf("Options:\n");
    printf("  -p, --port=<port>      listen on/connect to port <port> (default 18515)\n");
    printf("  -d, --ib-dev=<dev>     use IB device <dev> (default first device found)\n");
    printf("  -i, --ib-port=<port>   use port <port> of IB device (default 1)\n");
    printf("  -s, --size=<size>      size of message to exchange (default 4096)\n");
    printf("  -m, --mtu=<size>       path MTU (default 1024)\n");
    printf("  -r, --rx-depth=<dep>   number of receives to post at a time (default 500)\n");
    printf("  -n, --iters=<iters>    number of exchanges (default 1000)\n");
    printf("  -l, --sl=<sl>          service level value\n");
    printf("  -e, --events           sleep on CQ events (default poll)\n");
    printf("  -g, --gid-idx=<gid index> local port gid index\n");
}

/**
 * Server function: This function creates a new map item
 * @return map_item
 */
map_item* create_map_item()
{
    map_item *item = (map_item*)malloc(sizeof (map_item));
    item->key = NULL;
    item->value = NULL;
    map_item_list[registeries] = item;
    registeries++;

    return item;
}
/**
 * Server function: This function sets key and value to a map item
 * @param key the key to assign
 * @param value the value to assign
 * @param item the map item that we are changing
 * @return
 */
int set_map_item(char* key, char* value, map_item* item)
{
    item->key = key;
    item->value = value;
    return 0;
}

enum ibv_mtu pp_mtu_to_enum(int mtu)
{
    switch (mtu)
    {
        case 256:
            return IBV_MTU_256;
        case 512:
            return IBV_MTU_512;
        case 1024:
            return IBV_MTU_1024;
        case 2048:
            return IBV_MTU_2048;
        case 4096:
            return IBV_MTU_4096;
        default:
            return -1;
    }
}

uint16_t pp_get_local_lid(struct ibv_context *context, int port)
{
    struct ibv_port_attr attr;

    if (ibv_query_port(context, port, &attr))
    {
        return 0;
    }

    return attr.lid;
}

int pp_get_port_info(struct ibv_context *context, int port, struct ibv_port_attr *attr)
{
    return ibv_query_port(context, port, attr);
}

void wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
{
    char tmp[9];
    uint32_t v32;
    int i;

    for (tmp[8] = 0, i = 0; i < 4; ++i)
    {
        memcpy(tmp, wgid + i * 8, 8);
        sscanf(tmp, "%x", &v32);
        *(uint32_t *) (&gid->raw[i * 4]) = ntohl(v32);
    }
}

void gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
    int i;

    for (i = 0; i < 4; ++i)
    {
        sprintf(&wgid[i * 8], "%08x", htonl(*(uint32_t *) (gid->raw + i * 4)));
    }
}

static int
pp_connect_ctx(struct pingpong_context *ctx, int port, int my_psn, enum ibv_mtu mtu, int sl, struct pingpong_dest *dest,
               int sgid_idx)
{
    struct ibv_qp_attr attr = { .qp_state        = IBV_QPS_RTR, .path_mtu        = mtu, .dest_qp_num        = dest->qpn, .rq_psn            = dest->psn, .max_dest_rd_atomic    = 1, .min_rnr_timer        = 12, .ah_attr        = { .is_global    = 0, .dlid        = dest->lid, .sl        = sl, .src_path_bits    = 0, .port_num    = port }};

    if (dest->gid.global.interface_id)
    {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.dgid = dest->gid;
        attr.ah_attr.grh.sgid_index = sgid_idx;
    }
    if (ibv_modify_qp(ctx->qp, &attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                                      IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER))
    {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }

    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = my_psn;
    attr.max_rd_atomic = 1;
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                      IBV_QP_MAX_QP_RD_ATOMIC))
    {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return 1;
    }

    return 0;
}

static struct pingpong_dest *pp_client_exch_dest(const char *servername, int port, const struct pingpong_dest *my_dest)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = { .ai_family   = AF_INET, .ai_socktype = SOCK_STREAM };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
    {
        return NULL;
    }

    n = getaddrinfo(servername, service, &hints, &res);

    if (n < 0)
    {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next)
    {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0)
        {
            if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
            {
                break;
            }
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0)
    {
        fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        return NULL;
    }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(sockfd, msg, sizeof msg) != sizeof msg)
    {
        fprintf(stderr, "Couldn't send local address\n");
        goto out;
    }

    if (read(sockfd, msg, sizeof msg) != sizeof msg)
    {
        perror("client read");
        fprintf(stderr, "Couldn't read remote address\n");
        goto out;
    }

    write(sockfd, "done", sizeof "done");

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
    {
        goto out;
    }

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);

    //remote read address buffer
    if (read(sockfd, msg, sizeof msg) != sizeof msg)
    {
        perror("client read");
        fprintf(stderr, "Couldn't read remote address\n");
        goto out;
    }

    sscanf(msg, "%p", &remote_read_get_addr);
//    next_set = remote_read_get_addr + BUFFER_PARTITION_SIZE;


    if (read(sockfd, msg, sizeof msg) != sizeof msg)
    {
        perror("client read");
        fprintf(stderr, "Couldn't read remote address\n");
        goto out;
    }
    sscanf(msg, "%p", &remote_read_write_addr);

    if (read(sockfd, msg, sizeof msg) != sizeof msg)
    {
        perror("client read");
        fprintf(stderr, "Couldn't read remote address\n");
        goto out;
    }


    sscanf(msg, "%lu", &remote_get_key);


    uint64_t lkey;
    if (read(sockfd, msg, sizeof msg) != sizeof msg)
    {
        perror("client read");
        fprintf(stderr, "Couldn't read remote address\n");
        goto out;
    }
    sscanf(msg, "%d", &lkey);

    wire_gid_to_gid(gid, &rem_dest->gid);

    out:
    close(sockfd);
    return rem_dest;
}
/**
 * Client function: Initializes the kv handle struct
 * @param ctx the client ctx
 * @param size buffer size
 * @param ib_port
 * @param psn
 * @param mtu
 * @param sl
 * @param rem_dest
 * @param gidx
 * @return returns kv_handle struct
 */
kv_handle* init_kv_handle(struct pingpong_context* ctx, int size, int ib_port, int psn, enum ibv_mtu mtu,
            int sl, struct pingpong_dest* rem_dest, int gidx) {
    handler = malloc(sizeof (kv_handle));
    handler->ctx = ctx;
    handler->ctx->size = 1000;
    handler->ib_port = ib_port;
    handler->dset_psn = psn;
    handler->ibvMtu = mtu;
    handler->sl = sl;
    handler->rem_dest = rem_dest;
    handler->gidx = gidx;
    return handler;
}
/**
 * Server function: update the new write address after a set request from client
 * @param ctx server ctx
 * @param key the key for the set request from client
 * @param addr the address for the value
 * @param r_key the remote key
 */
void set_write_address(struct pingpong_context *ctx, char* key, char *addr, uint64_t r_key) {
    char* end = (char *) remote_read_write_addr;
    sprintf(end, "%s:%p-%lu", key, addr, r_key);
}

static struct pingpong_dest *
        pp_server_exch_dest(struct pingpong_context *ctx, int ib_port, enum ibv_mtu mtu, int port, int sl,
                    const struct pingpong_dest *my_dest, int sgid_idx)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = { .ai_flags    = AI_PASSIVE, .ai_family   = AF_INET, .ai_socktype = SOCK_STREAM };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1, connfd;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
    {
        return NULL;
    }

    n = getaddrinfo(NULL, service, &hints, &res);

    if (n < 0)
    {
        fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next)
    {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0)
        {
            n = 1;

            setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

            if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
            {
                break;
            }
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0)
    {
        fprintf(stderr, "Couldn't listen to port %d\n", port);
        return NULL;
    }

    listen(sockfd, 1);
    connfd = accept(sockfd, NULL, 0);
    close(sockfd);
    if (connfd < 0)
    {
        fprintf(stderr, "accept() failed\n");
        return NULL;
    }

    n = read(connfd, msg, sizeof msg);
    if (n != sizeof msg)
    {
        perror("server read");
        fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int) sizeof msg);
        goto out;
    }

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
    {
        goto out;
    }

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

    if (pp_connect_ctx(ctx, ib_port, my_dest->psn, mtu, sl, rem_dest, sgid_idx))
    {
        fprintf(stderr, "Couldn't connect to remote QP\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }


    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(connfd, msg, sizeof msg) != sizeof msg)
    {
        fprintf(stderr, "Couldn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }
    remote_get_addr = ctx->buf + (SEND_RECV_THRESHOLD - 1)*BUFFER_PARTITION_SIZE;
    remote_get_key = ctx->mr->rkey;
    // this is where we save the read adresses for eager or rendezvous get
    remote_read_get_addr = ctx->buf + (SEND_RECV_THRESHOLD)*BUFFER_PARTITION_SIZE;
    sprintf( remote_read_get_addr, "xxx:%p-%lu", remote_get_addr, remote_get_key);

    sprintf(msg, "%p", remote_read_get_addr);
    if (write(connfd, msg, sizeof msg) != sizeof msg)
    {
        fprintf(stderr, "Couldn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }

    remote_read_write_addr = ctx->buf + (SEND_RECV_THRESHOLD + 1) * BUFFER_PARTITION_SIZE;
    sprintf(remote_read_write_addr, "xxx:%p-%lu", remote_get_addr, remote_get_key);

    sprintf(msg, "%p", remote_read_write_addr);
    if (write(connfd, msg, sizeof msg) != sizeof msg)
    {
        fprintf(stderr, "Couldn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }

    sprintf(msg, "%lu", ctx->mr->rkey);
    if (write(connfd, msg, sizeof msg) != sizeof msg)
    {
        fprintf(stderr, "Couldn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }

    sprintf(msg, "%d", ctx->mr->lkey);
    if (write(connfd, msg, sizeof msg) != sizeof msg)
    {
        fprintf(stderr, "Couldn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }
    // this is where the write address is stored for client so he will know where to write large set
    set_write_address(ctx, "xxx", ctx->write_buf, ctx->write_mr->rkey);

    read(connfd, msg, sizeof msg);

    out:
    close(connfd);
    return rem_dest;
}


static struct pingpong_context *
pp_init_ctx(struct ibv_device *ib_dev, int size, int rx_depth, int tx_depth, int port, int use_event, int is_server)
{
    struct pingpong_context *ctx;

    ctx = calloc(1, sizeof *ctx);
    if (!ctx)
    {
        return NULL;
    }

    ctx->size = size;
    ctx->rx_depth = rx_depth;
    ctx->routs = rx_depth;

    ctx->buf = malloc(roundup(OVERRIDE_BUFF_SIZE, page_size) + 20*1024);
    for(int i = 0; i < roundup(OVERRIDE_BUFF_SIZE, page_size) + 20*1024; i++) {
        ctx->buf[i] = '\0';
    }
    ctx->write_buf = malloc(roundup(OVERRIDE_BUFF_SIZE, page_size) + 20*1024);
    ctx->read_buf = malloc(roundup(OVERRIDE_BUFF_SIZE, page_size) + 20*1024);

    if (!ctx->buf)
    {
        fprintf(stderr, "Couldn't allocate work buf.\n");
        return NULL;
    }

    memset(ctx->buf, 0x7b + is_server, OVERRIDE_BUFF_SIZE);

    ctx->context = ibv_open_device(ib_dev);
    if (!ctx->context)
    {
        fprintf(stderr, "Couldn't get context for %s\n", ibv_get_device_name(ib_dev));
        return NULL;
    }

    if (use_event)
    {
        ctx->channel = ibv_create_comp_channel(ctx->context);
        if (!ctx->channel)
        {
            fprintf(stderr, "Couldn't create completion channel\n");
            return NULL;
        }
    }
    else
    {
        ctx->channel = NULL;
    }

    ctx->pd = ibv_alloc_pd(ctx->context);
    if (!ctx->pd)
    {
        fprintf(stderr, "Couldn't allocate PD\n");
        return NULL;
    }

    ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, OVERRIDE_BUFF_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    ctx->write_mr = ibv_reg_mr(ctx->pd, ctx->write_buf, OVERRIDE_BUFF_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    ctx->read_mr = ibv_reg_mr(ctx->pd, ctx->read_buf, OVERRIDE_BUFF_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if (!ctx->mr)
    {
        fprintf(stderr, "Couldn't register MR\n");
        return NULL;
    }

    ctx->cq = ibv_create_cq(ctx->context, rx_depth + tx_depth, NULL, ctx->channel, 0);
    if (!ctx->cq)
    {
        fprintf(stderr, "Couldn't create CQ\n");
        return NULL;
    }

    {
        struct ibv_qp_init_attr attr = { .send_cq = ctx->cq, .recv_cq = ctx->cq, .cap     = { .max_send_wr  = tx_depth, .max_recv_wr  = rx_depth, .max_send_sge = 1, .max_recv_sge = 1 }, .qp_type = IBV_QPT_RC };

        ctx->qp = ibv_create_qp(ctx->pd, &attr);
        if (!ctx->qp)
        {
            fprintf(stderr, "Couldn't create QP\n");
            return NULL;
        }
    }

    {
        struct ibv_qp_attr attr = { .qp_state        = IBV_QPS_INIT, .pkey_index      = 0, .port_num        = port, .qp_access_flags =
        IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE };

        if (ibv_modify_qp(ctx->qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS))
        {
            fprintf(stderr, "Failed to modify QP to INIT\n");
            return NULL;
        }
    }

    return ctx;
}

int pp_close_ctx(struct pingpong_context *ctx)
{
    if (ibv_destroy_qp(ctx->qp))
    {
        fprintf(stderr, "Couldn't destroy QP\n");
        return 1;
    }

    if (ibv_destroy_cq(ctx->cq))
    {
        fprintf(stderr, "Couldn't destroy CQ\n");
        return 1;
    }

    if (ibv_dereg_mr(ctx->mr))
    {
        fprintf(stderr, "Couldn't deregister MR\n");
        return 1;
    }

    if (ibv_dereg_mr(ctx->write_mr))
    {
        fprintf(stderr, "Couldn't deregister WMR\n");
        return 1;
    }

    if (ibv_dereg_mr(ctx->read_mr))
    {
        fprintf(stderr, "Couldn't deregister RMR\n");
        return 1;
    }


    if (ibv_dealloc_pd(ctx->pd))
    {
        fprintf(stderr, "Couldn't deallocate PD\n");
        return 1;
    }

    if (ctx->channel)
    {
        if (ibv_destroy_comp_channel(ctx->channel))
        {
            fprintf(stderr, "Couldn't destroy completion channel\n");
            return 1;
        }
    }

    if (ibv_close_device(ctx->context))
    {
        fprintf(stderr, "Couldn't release context\n");
        return 1;
    }

    free(ctx->buf);
    free(ctx);

    return 0;
}

static int pp_post_recv(struct pingpong_context *ctx)
{

    int i;
    for (i = 0; i < SEND_RECV_THRESHOLD - 1; ++i)
    {

        struct ibv_sge list = { .addr    = (uintptr_t) wr_buffer_list[i]->buf, .length = OVERRIDE_BUFF_SIZE, .lkey    = ctx->mr->lkey };
        struct ibv_recv_wr wr = { .wr_id        = i, .sg_list    = &list, .num_sge    = 1, .next       = NULL };
        struct ibv_recv_wr *bad_wr;
        if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
        {
            break;
        }
    }

    return i;
}

static int pp_post_send(struct pingpong_context *ctx, enum ibv_wr_opcode opcode, uint64_t remote_addr)
{
    struct ibv_sge list = { .addr    = (uint64_t) ctx->buf, .length = ctx->size, .lkey    = ctx->mr->lkey };

    struct ibv_send_wr *bad_wr, wr = { .wr_id        = PINGPONG_SEND_WRID, .sg_list    = &list, .num_sge    = 1, .opcode     = opcode, .send_flags = IBV_SEND_SIGNALED, .next       = NULL };

//    wr.wr.rdma.remote_addr = remote_addr;
//    wr.wr.rdma.rkey = remote_get_key;
    return ibv_post_send(ctx->qp, &wr, &bad_wr);
}
/**
 * Client function: client performs a read
 * @param ctx client ctx
 * @param remote_addr the address to read from
 * @param r_key the remote key
 * @return
 */
static int pp_post_send_read(struct pingpong_context *ctx,  uint64_t remote_addr, uint64_t r_key)
        {
    struct ibv_sge list = { .addr    = (uint64_t) ctx->read_buf, .length = 8000, .lkey    = ctx->read_mr->lkey };

    struct ibv_send_wr *bad_wr, wr = { .wr_id        = PINGPONG_SEND_WRID, .sg_list    = &list, .num_sge    = 1, .opcode     = IBV_WR_RDMA_READ, .send_flags = IBV_SEND_SIGNALED, .next       = NULL };

    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = r_key;
    return ibv_post_send(ctx->qp, &wr, &bad_wr);
        }
/**
 * Client function: the client performs a write
 * @param ctx the client ctx
 * @param len
 * @param remote_addr the address to read from
 * @param r_key the remote key
 * @return
 */
static int pp_post_send_write(struct pingpong_context *ctx, char* buf, uint64_t lkey, uint64_t len, uint64_t remote_addr, uint64_t r_key)
{
    struct ibv_sge list = { .addr    = (uint64_t) buf, .length = len, .lkey    = lkey };

    struct ibv_send_wr *bad_wr, wr = { .wr_id        = PINGPONG_SEND_WRID, .sg_list    = &list, .num_sge    = 1, .opcode     = IBV_WR_RDMA_WRITE, .send_flags = IBV_SEND_SIGNALED, .next       = NULL };

    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = r_key;
    return ibv_post_send(ctx->qp, &wr, &bad_wr);
}
int pp_wait_completions(struct pingpong_context *ctx)
{
    int rcnt = 0, scnt = 0;

    while (rcnt + scnt < 1)
    {
        struct ibv_wc wc[1] = { 0 };
        int ne, i;

        do
        {
            ne = ibv_poll_cq(ctx->cq, WC_BATCH, wc);
            if (ne < 0)
            {
                fprintf(stderr, "poll CQ failed %d\n", ne);
                return 1;
            }
        }

        while (ne < 1);
        for (i = 0; i < ne; ++i)
        {
            if (wc[i].status != IBV_WC_SUCCESS)
            {


                fprintf(stderr, "Failed status %s (%d) for wr_id %d, iteration %d\n", ibv_wc_status_str(wc[i].status),
                        wc[i].status, (int) wc[i].wr_id, i);
                return 1;
            }



//            if((int) wc[i].wr_id >= SEND_RECV_THRESHOLD)
//            {
//                ++scnt;
//                break;
//            }
//
//            else
//            {
//                if (--ctx->routs <= 5 && !servername)
//                {
//
//                    if (ctx->routs < ctx->rx_depth)
//                    {
//
////
////                        fprintf(stderr, "Couldn't post receive (%d)\n", ctx->routs);
////                        return 1;
//                    }
//                }
//                ++rcnt;
//                break;
//            }


        }
        return wc[0].wr_id;

    }

    return -1;
}

/**
 * Utility function: checks if a string starts with a certain prefix
 * @param pre the prefix
 * @param str the string
 * @return true if string starts with prefix
 */
int startsWith(const char *pre, const char *str)
{
    size_t lenpre = strlen(pre);
    size_t lenstr = strlen(str);
    return lenstr < lenpre ? 0 : memcmp(pre, str, lenpre) == 0;
}
/**
 * Server function: the server searches for a map item with the given key after client get request
 * @param key the key
 * @return map item if key exists in database, else returns NULL
 */
map_item * get_map_item_by_key(char* key)
{

    for(int i = 0; i < registeries; i++)
    {
        if(strcmp(map_item_list[i]->key, key)==0)
        {
            return map_item_list[i];
        }
    }
    return NULL;
}
/**
 * Server function: handles a client rendezvous set request
 * @param value the value to add
 * @param key_buf the buffer to copy the key from
 * @return the corresponding item for the key-value
 */
map_item *server_handle_large_set(char *value, char* key_buf) {
    int len_key = strlen(key_buf) - strlen(SET_RENDEZVOUS);
    char* key = (char*) malloc(len_key) + 1;
    key[len_key] = '\0';
    strncpy(key, key_buf + strlen(SET_RENDEZVOUS), len_key);
    map_item* item = get_map_item_by_key(key);
    if (!item)
    {
        item = create_map_item();
    }
    item->key = key;
    item->value = value;
    return item;
}

/**
 * Server function: handles a set request from client
 * @param buf the buffer that holds the key-value
 * @return the corresponding item for the key-value
 */
map_item *server_handle_set(char* buf) {
    char *key = (char*) malloc(4000);
    sscanf(buf, "%[^-]",key);
    map_item* item = get_map_item_by_key(key);
    if (item == NULL)
    {
        item = create_map_item();
    }
    set_map_item(key, buf + strlen(key) + 1, item);

    return item;
}
/**
 * Server function: updates the new read address after get request from client
 * @param key the key the client wants its value
 * @param r_key the remote key
 * @param value_addr the value address
 * @param ctx the server ctx
 */
void server_update_read_addr(char *key, uint64_t r_key, uint64_t value_addr, struct pingpong_context *ctx)
{
    sprintf(remote_read_get_addr, "%s:%p-%lu",key, value_addr, r_key);
}
/**
 * Server function: handles rendezvous get requests from client
 * @param item the matching item
 * @param ctx the server ctx
 */
void server_handle_large_get(map_item* item, struct pingpong_context *ctx){
    server_update_read_addr(item->key, item->mr->rkey,(uint64_t) item->value, ctx);
}
/**
 * Server function: handles eager get requests from client
 * @param item the matching item
 * @param ctx the server ctx
 */
void server_handle_eager_get(map_item* item, struct pingpong_context *ctx) {
    char* addr = item->value;
    if(!addr) {
        return;
    }
    server_update_read_addr(item->key, item->mr->rkey, item->value, ctx);
}
/**
 * Server function: handles get requests from client
 * @param key the key the client sent
 * @param ctx server ctx
 * @return
 */
int server_handle_get(char* key, struct pingpong_context *ctx ) {

    map_item* item = get_map_item_by_key(key);
    if(item->large) {
        server_handle_large_get(item, ctx);
    }else {
        server_handle_eager_get(item, ctx);
    }

    return 0;
}
/**
 * Server function: handles get and set requests from client
 * @param ctx server ctx
 * @return
 */
int handle_server(struct pingpong_context *ctx) {
//    for(int i = 0; i < 100; i++) {
    int counter = 0;
    while (1){
        int wr_id = pp_wait_completions(ctx);
        counter++;

        if (wr_id < 0)
        {
            printf("bad wr_id %d\n", wr_id);
            return 1;
        }
        char* buf = wr_buffer_list[wr_id]->buf;
        if(startsWith(GET_PREFIX, buf)) {
            printf("got get request %s\n", buf + strlen(GET_PREFIX));
            server_handle_get(buf + strlen(GET_PREFIX), ctx);
        }else if(startsWith(SET_PREFIX, buf)){
            printf("got set request %s\n", buf + strlen(SET_PREFIX));
            map_item * item = server_handle_set(buf + strlen(SET_PREFIX));
            item->mr = ctx->mr;
            item->large = 0;
            set_write_address(ctx, item->key, item->value, item->mr->rkey);
        }else if(startsWith(SET_RENDEZVOUS, buf)) {
            map_item * item = server_handle_large_set(ctx->write_buf, buf);
            item->large = 1;
            item->mr = ctx->write_mr;
            ctx->write_buf = malloc(roundup(OVERRIDE_BUFF_SIZE, page_size) + 20*1024);
            ctx->write_mr = ibv_reg_mr(ctx->pd, ctx->write_buf, OVERRIDE_BUFF_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
            set_write_address(ctx, item->key, item->value, item->mr->rkey);
        }else {
            printf("invalid request from client");
            break;
        }
        if(counter == SEND_RECV_THRESHOLD - 1)
        {
            ctx->buf = malloc(roundup(OVERRIDE_BUFF_SIZE, page_size) + 20*1024);
            wr_buffer_list = (work_request_buffer**) malloc(sizeof(work_request_buffer*)*SEND_RECV_THRESHOLD);
            for(int i = 0; i < SEND_RECV_THRESHOLD; i++)
            {
                work_request_buffer *buffer = (work_request_buffer*)malloc(sizeof(work_request_buffer));
                buffer->buf = ctx->buf + i*BUFFER_PARTITION_SIZE;
                wr_buffer_list[i] = buffer;
            }
            ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, OVERRIDE_BUFF_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
            ctx->routs += pp_post_recv(ctx);

            counter = 0;
        }
    }
    for(int j = 0; j < registeries; j++) {
        printf("%d %s %s\n",j,map_item_list[j]->key, map_item_list[j]->value);
    }
    return 0;
}
/**
 * Client function: releases the value after get requests
 * @param value the value
 */
void kv_release(char *value)
{
    free(value);
}
/**
 * Client function: Destroys the QP and releases resources
 * @param kv_handler the kv_handle struct
 * @return
 */
int kv_close(void *kv_handler)
{

    kv_handle *handle = (kv_handle*)kv_handler;
    if(ibv_destroy_qp(handle->ctx->qp))
    {
        fprintf(stderr, "Unable to destroy QP\n");
        return 1;
    }
    if(ibv_destroy_cq(handle->ctx->cq))
    {
        fprintf(stderr, "Unable to destroy CQ\n");
        return 1;
    }

    if(ibv_dereg_mr(handle->ctx->mr))
    {
        fprintf(stderr, "Unable to deregister MR\n");
        return 1;
    }
    if(ibv_dereg_mr(handle->ctx->read_mr))
    {
        fprintf(stderr, "Unable to deregister Read MR\n");
        return 1;
    }
    if(ibv_dereg_mr(handle->ctx->write_mr))
    {
        fprintf(stderr, "Unable to deregister Write MR\n");
        return 1;
    }
    if(ibv_dealloc_pd(handle->ctx->pd))
    {
        fprintf(stderr, "Unable to dealloc PD\n");
        return 1;
    }
    if(handle->ctx->channel)
    {
        if(ibv_destroy_comp_channel(handle->ctx->channel))
        {
            fprintf(stderr, "Unable to destroy completion channel\n");
            return 1;
        }
    }
    if(ibv_close_device(handle->ctx->context))
    {
        fprintf(stderr, "Unable to release context\n");
        return 1;
    }
    free(handle->ctx->read_buf);
    free(handle->ctx->write_buf);
    free(handle->ctx->buf);
    free(handle->ctx);

    return 0;

}

void kv_get_set_test(kv_handle *handler, char* key, char* value) {
    printf("setting %s %s\n", key, value);
    kv_set(&handler, key, value);

    printf("getting %s\n", key);
    char* getVal = (char* ) malloc(20);
    kv_get(&handler, key, &getVal);
    printf("got %s\n",value);
}

void regular_test(kv_handle *handler) {

    kv_get_set_test(handler, "rotem", "inon");
    kv_get_set_test(handler, "rotem", "rotem");

    char * info = (char*) malloc(5500);
    info[5000] = '\0';
    for(int i = 0; i < 5000; i++ ) {
        info[i] = '1';
    }
    kv_get_set_test(handler, "tomer", info);

    for(int i = 0; i < 5000; i++ ) {
        info[i] = '2';
    }
    kv_get_set_test(handler, "tomer", info);
}

void many_eager_set(kv_handle *handler) {
    char key[10];
    char val[10];
    for(int i = 0; i < 250; i++) {
        sprintf(key, "%d", i);
        sprintf(val, "%d", i);
        kv_get_set_test(handler, key, val);
    }
}
/**
 * Client function: starts the client-server
 * @param servername the server name
 * @param handler kv_handle struct
 * @return
 */
int handle_client(char* servername, kv_handle *handler) {

    if (kv_open(servername, (void**) &handler))
    {
        return 1;
    }
//    regular_test(handler);
    many_eager_set(handler);
    return 0;
}
/**
 * Client function: connects to server
 * @param servername the server name
 * @param h kv_handle struct
 * @return
 */
int kv_open(char *servername, void **h) {
    kv_handle *handle = *((kv_handle **)  h);
    if (pp_connect_ctx(handle->ctx, handle->ib_port, handle->dset_psn, handle->ibvMtu,
                       handle->sl, handle->rem_dest, handle->gidx))
    {
        return 1;
    }
    return 0;
}
/**
 * Client function: handles kv_set rendezvous requests of client
 * @param kv the kv_handle struct
 * @param key the key
 * @param value the value
 */
void kv_set_rendezvous(void *kv, const char *key, const char *value) {
    kv_handle *handle = *((kv_handle **)  kv);
    uint64_t r_key;

    // this is a control message to let the server know that a rendezvous request
    // is going to happen
    sprintf(handle->ctx->buf, "rendezvous:%s",key);
    pp_post_send(handle->ctx, IBV_WR_SEND, remote_read_get_addr);
    pp_wait_completions(handle->ctx);

    char currKey[4000] = "";
    // read the remote write address and rkey
     do {
         pp_post_send_read(handle->ctx, remote_read_write_addr, remote_get_key);
         pp_wait_completions(handle->ctx);
         sscanf(handle->ctx->read_buf, "%[^:]:%p-%lu",currKey, &curr_rendezvous, &r_key);
     }while(strcmp(currKey, key));

    // here we are sending the large rendezvous value and key
    struct ibv_mr *mr = ibv_reg_mr(handle->ctx->pd, value, strlen(value) + 1,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    pp_post_send_write(handle->ctx, value, mr->lkey, strlen(value) + 1, curr_rendezvous, r_key);
    pp_wait_completions(handle->ctx);
    ibv_dereg_mr(mr);

}
/**
 * Client function: handles kv_set eager requests of client
 * @param kv the kv_handle struct
 * @param key the key
 * @param value the value
 */
void kv_set_eager(void *kv, const char *key, const char *value) {
    kv_handle *handle = *((kv_handle **)  kv);
    sprintf(handle->ctx->buf, "key-value:%s-%s", key, value);
    pp_post_send(handle->ctx, IBV_WR_SEND, remote_read_get_addr);
    pp_wait_completions(handle->ctx);
    char currKey[4000];
    uint64_t r_key;
    do {
        pp_post_send_read(handle->ctx, remote_read_write_addr, remote_get_key);
        pp_wait_completions(handle->ctx);
        sscanf(handle->ctx->read_buf, "%[^:]:%p-%lu", currKey, &curr_rendezvous, &r_key);
    }while(strcmp(currKey, key));

    strncpy(handle->ctx->buf, value, strlen(value) + 1);
    pp_post_send_write(handle->ctx, handle->ctx->buf, handle->ctx->mr->lkey, strlen(value) + 1, curr_rendezvous, r_key);
    pp_wait_completions(handle->ctx);
}
/**
 * Client function: handles set requests of client
 * @param kv the kv_handle struct
 * @param key the key
 * @param value the value
 * @return
 */
int kv_set(void *kv, const char *key, const char *value) {
    kv_handle *handle = *((kv_handle **)  kv);
    int key_len = strlen(key);
    int value_len = strlen(value);
    int total_len = key_len + value_len + 2;
    if(total_len >= 4*1024) {
        kv_set_rendezvous(kv, key,value);
    }else {
        kv_set_eager(kv, key,value);
    }

    return 0;
}
/**
 * Client function: handles get requests of client
 * @param kv the kv_handle struct
 * @param key the key
 * @param value the value
 * @return
 */
int kv_get(void *kv, const char *key, char **value)
{
    kv_handle *handle = *((kv_handle **)  kv);
    // copy the key into the client buffer
    sprintf(handle->ctx->buf, "key:%s", key);
    //sends the server the message of what key the client wants
    pp_post_send(handle->ctx, IBV_WR_SEND, remote_read_get_addr);
    pp_wait_completions(handle->ctx);
    char prefix[4000];
    char* new_read_buff = NULL;
    struct ibv_mr* mr = NULL;
    while(1) {
        pp_post_send_read(handle->ctx, remote_read_get_addr, remote_get_key);
        pp_wait_completions(handle->ctx);
        uint64_t read_addr, r_key;
        sscanf(handle->ctx->read_buf, "%[^:]:%p-%lu",prefix , &read_addr, &r_key);
        pp_post_send_read(handle->ctx,read_addr, r_key );

        if(!new_read_buff) {
            new_read_buff = malloc(roundup(OVERRIDE_BUFF_SIZE, page_size) + 20*1024);
            mr =  ibv_reg_mr(handle->ctx->pd, new_read_buff, OVERRIDE_BUFF_SIZE,
                                               IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
        }

        pp_wait_completions(handle->ctx);
        if(strcmp(prefix, key) == 0) {
            (*value) = handle->ctx->read_buf;
            ibv_dereg_mr(handle->ctx->read_mr);
            handle->ctx->read_buf = new_read_buff;
            handle->ctx->read_mr = mr;
            break;
        }
    }


    return 0;
}


int main(int argc, char *argv[]) {
    struct ibv_device **dev_list;
    struct ibv_device *ib_dev;
    struct pingpong_context *ctx;
    struct pingpong_context *writeCtx;
    struct pingpong_context *recvCtx;
    struct pingpong_dest my_dest;
    struct pingpong_dest *rem_dest;
    char *ib_devname = NULL;
    int port = 12345;
    int ib_port = 1;
    enum ibv_mtu mtu = IBV_MTU_2048;
    int rx_depth = 100;
    int tx_depth = 100;
    int iters = 10000;
    int use_event = 0;
    int size = 1;
    int sl = 0;
    int gidx = -1;
    char gid[33] = { 0 };



    {
        srand48(getpid() * time(NULL));

        while (1)
        {
            int c;

            static struct option long_options[] = {{ .name = "port", .has_arg = 1, .val = 'p' },
                                                   { .name = "ib-dev", .has_arg = 1, .val = 'd' },
                                                   { .name = "ib-port", .has_arg = 1, .val = 'i' },
                                                   { .name = "size", .has_arg = 1, .val = 's' },
                                                   { .name = "mtu", .has_arg = 1, .val = 'm' },
                                                   { .name = "rx-depth", .has_arg = 1, .val = 'r' },
                                                   { .name = "iters", .has_arg = 1, .val = 'n' },
                                                   { .name = "sl", .has_arg = 1, .val = 'l' },
                                                   { .name = "events", .has_arg = 0, .val = 'e' },
                                                   { .name = "gid-idx", .has_arg = 1, .val = 'g' },
                                                   { 0 }};

            c = getopt_long(argc, argv, "p:d:i:s:m:r:n:l:eg:", long_options, NULL);
            if (c == -1)
            {
                break;
            }

            switch (c)
            {
                case 'p':
                    port = strtol(optarg, NULL, 0);
                    if (port < 0 || port > 65535)
                    {
                        usage(argv[0]);
                        return 1;
                    }
                    break;

                case 'd':
                    ib_devname = strdup(optarg);
                    break;

                case 'i':
                    ib_port = strtol(optarg, NULL, 0);
                    if (ib_port < 0)
                    {
                        usage(argv[0]);
                        return 1;
                    }
                    break;

                case 's':
                    size = strtol(optarg, NULL, 0);
                    break;

                case 'm':
                    mtu = pp_mtu_to_enum(strtol(optarg, NULL, 0));
                    if (mtu < 0)
                    {
                        usage(argv[0]);
                        return 1;
                    }
                    break;

                case 'r':
                    rx_depth = strtol(optarg, NULL, 0);
                    break;

                case 'n':
                    iters = strtol(optarg, NULL, 0);
                    break;

                case 'l':
                    sl = strtol(optarg, NULL, 0);
                    break;

                case 'e':
                    ++use_event;
                    break;

                case 'g':
                    gidx = strtol(optarg, NULL, 0);
                    break;

                default:
                    usage(argv[0]);
                    return 1;
            }
        }

        servername = NULL;
        if (optind == argc - 1)
        {
            servername = strdup(argv[optind]);
        }
        else if (optind < argc)
        {
            usage(argv[0]);
            return 1;
        }


        page_size = sysconf(_SC_PAGESIZE);

        dev_list = ibv_get_device_list(NULL);
        if (!dev_list)
        {
            perror("Failed to get IB devices list");
            return 1;
        }

        if (!ib_devname)
        {
            ib_dev = *dev_list;
            if (!ib_dev)
            {
                fprintf(stderr, "No IB devices found\n");
                return 1;
            }
        }
        else
        {
            int i;
            for (i = 0; dev_list[i]; ++i)
            {
                if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
                {
                    break;
                }
            }
            ib_dev = dev_list[i];
            if (!ib_dev)
            {
                fprintf(stderr, "IB device %s not found\n", ib_devname);
                return 1;
            }
        }

        ctx = pp_init_ctx(ib_dev, size, rx_depth, tx_depth, ib_port, use_event, !servername);

        if (!ctx)
        {

            return 1;
        }

        wr_buffer_list = (work_request_buffer**) malloc(sizeof(work_request_buffer*)*SEND_RECV_THRESHOLD);
        for(int i = 0; i < SEND_RECV_THRESHOLD; i++)
        {
            work_request_buffer *buffer = (work_request_buffer*)malloc(sizeof(work_request_buffer));
            buffer->buf = ctx->buf + i*BUFFER_PARTITION_SIZE;
            wr_buffer_list[i] = buffer;
        }
        if(!servername) {
            ctx->routs = pp_post_recv(ctx);

        }

        if (ctx->routs < ctx->rx_depth)
        {
//            fprintf(stderr, "Couldn't post receive (%d)\n", ctx->routs);
//            return 1;
        }


        if (use_event)
        {
            if (ibv_req_notify_cq(ctx->cq, 0))
            {
                fprintf(stderr, "Couldn't request CQ notification\n");
                return 1;
            }
        }


        if (pp_get_port_info(ctx->context, ib_port, &ctx->portinfo))
        {
            fprintf(stderr, "Couldn't get port info\n");
            return 1;
        }


        my_dest.lid = ctx->portinfo.lid;
        if (ctx->portinfo.link_layer == IBV_LINK_LAYER_INFINIBAND && !my_dest.lid)
        {
            fprintf(stderr, "Couldn't get local LID\n");
            return 1;
        }


        if (gidx >= 0)
        {
            if (ibv_query_gid(ctx->context, ib_port, gidx, &my_dest.gid))
            {
                fprintf(stderr, "Could not get local gid for gid index %d\n", gidx);
                return 1;
            }
        }
        else
        {
            memset(&my_dest.gid, 0, sizeof my_dest.gid);
        }


        my_dest.qpn = ctx->qp->qp_num;
        my_dest.psn = lrand48() & 0xffffff;
        inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);

        if(!servername)
        {
            map_item_list = (map_item**) malloc(sizeof(map_item*)*SEND_RECV_THRESHOLD);

        }

        if (servername)
        {
            rem_dest = pp_client_exch_dest(servername, port, &my_dest);

        }
        else
        {
            rem_dest = pp_server_exch_dest(ctx, ib_port, mtu, port, sl, &my_dest, gidx);

        }


        if (!rem_dest)
        {

            return 1;
        }


        inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);

        if (servername)
        {

            handler = malloc(sizeof (kv_handle));
            handler->ctx = ctx;
            handler->ctx->size = 100;
            handler->ib_port = ib_port;
            handler->dset_psn = my_dest.psn;
            handler->ibvMtu = mtu;
            handler->sl = sl;
            handler->rem_dest = rem_dest;
            handler->gidx = gidx;
            handle_client(servername, handler);
        }
        else {
            handle_server(ctx);
            for(int i = 0; i < registeries; i++) {
                map_item * item = map_item_list[i];
                free(item->key);
                free(item->value);
                free(item);
            }
        }
//        free(ctx->buf);

    }

    // OUR CODE STARTS HERE
    //	int curr_size_to_send = 1;
    //	struct timespec st, et;
    //	double measurements[EXP_AMOUNT] = { 0 };
    //	long nanosec_elapsed = 0;
    //	unsigned long throughput, bits_sent, size_sent;
    pp_close_ctx(handler->ctx);
    printf("Finished!\n");

}