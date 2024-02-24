
This program is a database that is controlled by a server, and that can receive set and get requests from a client. The
messages and actions of read and write between the client and server is with the RDMA protocol, using the lib_verbs
library.

Client functions:
-kv_open : connects to server
-kv_set : handles set requests of client
-kv_get : handles get requests of client
-kv_release : releases the value after get requests
-kv_close : Destroys the QP and releases resources

Server functions:
-create_map_item : creates a new map item
-set_map_item : sets key and value to a map item
-set_write_address : update the new write address after a set request from client
-get_map_item_by_key : searches for a map item with the given key after client get request
-server_handle_set : handles a set request from client
-server_handle_large_set : handles a client rendezvous set request
-server_update_read_addr : updates the new read address after get request from client
-server_handle_get : handles get requests from client
-server_handle_large_get : handles rendezvous get requests from client
-server_handle_eager_get : handles eager get requests from client
-handle_server : handles get and set requests from client

Program flow - set:
At the beginning of the program the server sends to the client an address on his ctx buffer that will contain the
address to where the client will write the value he wanted for the corresponding key. The client will keep on reading
the agreed upon address on the server ctx buffer until the correct format is written to there by the server, and only
then will the client write his value to the corresponding key. If the set request by the client holds a large value,
we will use the rendezvous protocol - the server will register a new memory region with a corresponding buffer of a
large enough size.

Program flow - get:
At the beginning of the program the server sends to the client an address on his ctx buffer that will contain the
address to where the client will read the value he wanted for the corresponding key. The client will keep on reading
the agreed upon address on the server ctx buffer until the correct format is written to there by the server, and only
then will the client read his value to the corresponding key. The client on his part will allocate a large enough
buffer to read the value into, since he doesn't know how big the value is.


