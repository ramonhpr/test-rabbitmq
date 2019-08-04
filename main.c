#include <stdio.h> 
#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>
#include <ell/ell.h>
#include <signal.h>

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

	amqp_default_connection_info(&c_info);
	fprintf(stderr, "Connecting to host=%s, port=%d\n", c_info.host, c_info.port);
	conn = amqp_new_connection();
	socket = amqp_tcp_socket_new(conn);
	if (!socket) {
		fprintf(stderr, "error creating tcp socket\n");
		return NULL;
	}
	status = amqp_socket_open(socket, c_info.host, c_info.port);
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

static void amqp_stop(amqp_connection_state_t conn) {
	amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
	amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
	amqp_destroy_connection(conn);
}

int main()
{
	int err = EXIT_FAILURE;
	amqp_connection_state_t conn;

	fprintf(stderr, "Starting\n");
	if (!l_main_loop_init())
		goto fail;
	conn = amqp_start();

	l_main_loop_run();
	amqp_stop(conn);
	fprintf(stderr, "Exiting\n");
	err = EXIT_SUCCESS;
fail:
	return err;
}
