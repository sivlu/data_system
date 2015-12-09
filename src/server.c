/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The server should allow for multiple concurrent connections from clients.
 * Each client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "common.h"
#include "cs165_api.h"
#include "message.h"
#include "parser.h"
#include "utils.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

//global
struct db_node *db_table; //used in execute query


// Here, we allow for a global of DSL COMMANDS to be shared in the program
//dsl** dsl_commands;

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
db_operator* parse_command(message* recv_message, message* send_message) {
    send_message->status = OK_WAIT_FOR_RESPONSE;
    db_operator *dbo = malloc(sizeof(db_operator));
    dbo->lhs_var1=NULL;
    dbo->lhs_var2=NULL;
    dbo->string = NULL;
    dbo->rhs_var1=NULL;
    dbo->rhs_var2=NULL;
    dbo->rhs_var3=NULL;
    dbo->rhs_var4=NULL;



    // Here you parse the message and fill in the proper db_operator fields for
    // now we just log the payload
    cs165_log(stdout, recv_message->payload);

    // Here, we give you a default parser, you are welcome to replace it with anything you want
    status parse_status = parse_command_string(recv_message->payload, dbo);
    if (parse_status.code != OK) {
        // Something went wrong
    }

    return dbo;
}

/** execute_db_operator takes as input the db_operator and executes the query.
 * It should return the result (currently as a char*, although I'm not clear
 * on what the return type should be, maybe a result struct, and then have
 * a serialization into a string message).
 **/
char* execute_db_operator(db_operator* op) {
    //NOTE: skipped create tbl, and create col
    char* op_name[] = {"CREATE_DB", "CREATE_TBL", "CREATE_COL", "CREATE_IDX", "SELECT",
                   "FETCH", "LOAD", "HASH_JOIN", "RELATIONAL_INSERT", "ADD",
                   "SUBTRACT", "UPDATE", "TUPLE", "SHUTDOWN", "GET_MIN", "GET_MAX", "GET_AVG"};
    char ret_buffer[BUF_SIZE];
    char* tuple_res = NULL;
    sprintf(ret_buffer, "Executed: %s", op_name[op->type]);
    log_info("Executing operation: %s", op_name[op->type]);

    if (op->type == CREATE_DB){
        db_table->this_db = NULL;
        db* mydb = db_table->this_db;
        create_db(op->string, &mydb);

    }else if (op->type == CREATE_IDX){
        if (op->idx_type == CLUSTERED){
            create_clustered_index(op->tbl, op->col);
        }else {
            create_index(op->col, op->idx_type);
        }

    }else if (op->type == SELECT){
        result** exist = create_var_in_pool(op->lhs_var1);
        if (op->col->index){
            if (op->col->index->type == SORTED){
                sorted_select_local(op->col, op->low, op->high, exist, op->rhs_var1);
            }else if (op->col->index->type == B_PLUS_TREE){
                btree_select_local(op->col, op->low, op->high, exist, op->rhs_var1);
            }
        }else{
            col_select_local(op->col, op->low, op->high, exist, op->rhs_var1);
        }

    }else if (op->type == FETCH){
        result** res = create_var_in_pool(op->lhs_var1);
        fetch(op->col, op->rhs_var1, res);

    }else if (op->type == LOAD){
        open_db(op->string, db_table->this_db);

    }else if (op->type == HASH_JOIN){
        //NOTE: uses multi threads
        result** res_pos1 = create_var_in_pool(op->lhs_var1);
        result** res_pos2 = create_var_in_pool(op->lhs_var2);
        hash_join(op->rhs_var1, op->rhs_var2, op->rhs_var3, op->rhs_var4, res_pos1, res_pos2);

    }else if (op->type == RELATIONAL_INSERT){
        relational_insert(op->tbl, op->string);

    }else if (op->type == ADD){
        result** res = create_var_in_pool(op->lhs_var1);
        add(op->rhs_var1, op->rhs_var2, res);

    }else if (op->type == SUBTRACT){
        result** res = create_var_in_pool(op->lhs_var1);
        subtract(op->rhs_var1, op->rhs_var2, res);

    }else if (op->type == UPDATE){
        update(op->col, op->rhs_var1, op->value);

    }else if (op->type == TUPLE){
        char buf[strlen(op->string)+1];
        strcpy(buf, op->string);
        buf[strlen(op->string)] = 0;
        result* args[NUM_VARS];
        int count = 0;
        char* token = strtok(buf, ",");
        while(token){
            args[count++] = *(get_var(token));
            token = strtok(NULL, ",");
        }
        tuple(args, count, &tuple_res); //get tuple, save in tuple_res


    }else if (op->type == SHUTDOWN){
        //save to "../data/"
        prepare_close_conn(DATA_PATH);
        free_variable_pool();
        //do I need to exit?

    }else if (op->type == GET_MIN){
        result** min_val = create_var_in_pool(op->lhs_var1);
        result** min_pos = NULL;
        if (op->lhs_var2) min_pos = create_var_in_pool(op->lhs_var2);
        min(op->rhs_var1, op->rhs_var2, min_val, min_pos);


    }else if (op->type == GET_MAX){
        result** max_val = create_var_in_pool(op->lhs_var1);
        result** max_pos = NULL;
        if (op->lhs_var2) max_pos = create_var_in_pool(op->lhs_var2);
        max(op->rhs_var1, op->rhs_var2, max_val, max_pos);

    }else if (op->type == GET_AVG){
        result** average = create_var_in_pool(op->lhs_var1);
        avg(op->rhs_var1, average);
    }



    free_op(op);
    return tuple_res==NULL? strdup(ret_buffer):tuple_res;
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            db_operator* query = parse_command(&recv_message, &send_message);

            // 2. Handle request
            char* result = execute_db_operator(query);
            send_message.length = strlen(result);

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                free(result);
                exit(1);
            }

            // 4. Send response of request
            if (send(client_socket, result, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                free(result);
                exit(1);
            }
        }
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.


int main(void)
{
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    // Populate the global dsl commands
//    dsl_commands = dsl_commands_init();

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }

    handle_client(client_socket);

    return 0;
}

