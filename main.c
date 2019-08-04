#include <stdio.h> 
#include <amqp.h>
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


int main()
{
	int err = EXIT_FAILURE;
	fprintf(stderr, "Starting\n");
	if (!l_main_loop_init())
		goto fail;
	l_main_loop_run();
	fprintf(stderr, "Exiting\n");
	err = EXIT_SUCCESS;
fail:
	return err;
}
