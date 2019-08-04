#include <stdio.h> 
#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>
#include <ell/ell.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/types.h>

#define QUEUE_NAME "test_queue"
#define EXCHANGE_NAME "test_exchange"
#define BINDING_KEY "*.*"

static void main_loop_quit(struct l_timeout *timeout, void *user_data)
{
	l_main_quit();
}

static void l_terminate(void)
{
	static bool terminating = false;

	if (terminating)
		return;

	terminating = true;

	l_timeout_create(1, main_loop_quit, NULL, NULL);
}

static bool l_main_loop_init()
{
	return l_main_init();
}

static void signal_handler(uint32_t signo, void *user_data)
{
	switch (signo) {
	case SIGINT:
	case SIGTERM:
		l_terminate();
		break;
	}
}

static void l_main_loop_run()
{
	l_main_run_with_signal(signal_handler, NULL);

	l_main_exit();
}

static amqp_connection_state_t amqp_start() {
	int err, status;
	amqp_socket_t *socket = NULL;
	amqp_connection_state_t conn;
	amqp_rpc_reply_t r;
	struct amqp_connection_info c_info;
	struct timeval time_out = { .tv_usec=10000 };

	amqp_default_connection_info(&c_info);
	fprintf(stderr, "Connecting to host=%s, port=%d\n", c_info.host, c_info.port);
	conn = amqp_new_connection();
	socket = amqp_tcp_socket_new(conn);
	if (!socket) {
		fprintf(stderr, "error creating tcp socket\n");
		return NULL;
	}

	status = amqp_socket_open_noblock(socket, c_info.host, c_info.port, &time_out);
	if (status) {
		fprintf(stderr, "error openning socket\n");
		return NULL;
	}

	r = amqp_login(conn, c_info.vhost, AMQP_DEFAULT_MAX_CHANNELS,
			AMQP_DEFAULT_FRAME_SIZE, AMQP_DEFAULT_HEARTBEAT,
			AMQP_SASL_METHOD_PLAIN, c_info.user, c_info.password);
	if (r.reply_type != AMQP_RESPONSE_NORMAL) {
		fprintf(stderr, "error logging to amqp server\n");
		return NULL;
	}
	amqp_channel_open(conn, 1);
	if (amqp_get_rpc_reply(conn).reply_type != AMQP_RESPONSE_NORMAL) {
		fprintf(stderr, "error logging to amqp server\n");
		return NULL;
	}

	return conn;
}

static void amqp_stop(amqp_connection_state_t conn)
{
	amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
	amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
	amqp_destroy_connection(conn);
}

static void start_timeout(struct l_timeout* timeout, void* user_data)
{
	amqp_connection_state_t* conn_p = user_data;
	amqp_connection_state_t conn = *conn_p;
	amqp_rpc_reply_t res;
      	amqp_envelope_t envelope;
	struct timeval time_out = { .tv_usec=10000 };

      	amqp_maybe_release_buffers(conn);

      	res = amqp_consume_message(conn, &envelope, &time_out, 0);

      	if (AMQP_RESPONSE_NORMAL != res.reply_type) {
		l_timeout_modify_ms(timeout, 500);
		return;
      	}

     	printf("Delivery %u, exchange %.*s routingkey %.*s\n",
             (unsigned)envelope.delivery_tag, (int)envelope.exchange.len,
             (char *)envelope.exchange.bytes, (int)envelope.routing_key.len,
             (char *)envelope.routing_key.bytes);

      	if (envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
        	printf("Content-type: %.*s\n",
        	        (int)envelope.message.properties.content_type.len,
               		(char *)envelope.message.properties.content_type.bytes);
		printf("Body: %.*s\n",
			(int)envelope.message.body.len,
			(char *)envelope.message.body.bytes);
      	}
      	printf("----\n");
	amqp_destroy_envelope(&envelope);

	l_timeout_modify_ms(timeout, 500);
}

static int prepare_to_consume(amqp_connection_state_t* conn)
{
	amqp_bytes_t queuename;
	amqp_queue_declare_ok_t *r = amqp_queue_declare(
		*conn, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
	if (amqp_get_rpc_reply(*conn).reply_type != AMQP_RESPONSE_NORMAL) {
		fprintf(stderr, "error declaring queue name");
		return -1;
	}

	queuename = amqp_bytes_malloc_dup(r->queue);
	if (queuename.bytes == NULL) {
		fprintf(stderr, "Out of memory while copying queue name");
		return -1;
	}
	amqp_queue_bind(*conn, 1, queuename, amqp_cstring_bytes(EXCHANGE_NAME),
                        amqp_cstring_bytes(BINDING_KEY), amqp_empty_table);
	if (amqp_get_rpc_reply(*conn).reply_type != AMQP_RESPONSE_NORMAL) {
		fprintf(stderr, "error in bind");
		return -1;
	}
	amqp_basic_consume(*conn, 1, queuename, amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
	if (amqp_get_rpc_reply(*conn).reply_type != AMQP_RESPONSE_NORMAL) {
		fprintf(stderr, "error in basic consume");
		return -1;
	}

	return 0;
}

int main()
{
	int err = EXIT_FAILURE;
	amqp_connection_state_t conn;

	fprintf(stderr, "Starting\n");
	if (!l_main_loop_init())
		goto fail;
	conn = amqp_start();
	if (prepare_to_consume(&conn) < 0)
		goto fail_conn;
	l_timeout_create_ms(500, start_timeout, &conn, NULL);
	l_main_loop_run();
fail_conn:
	amqp_stop(conn);
	fprintf(stderr, "Exiting\n");
	err = EXIT_SUCCESS;
fail:
	return err;
}
