#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <unistd.h>
#include "cacti.h"


#define BASE_ACTORS_VECTOR_SIZE 64


__thread actor_id_t current_actor_id = -1;


typedef struct actor {
	actor_id_t id;
	role_t *role;
	pthread_mutex_t *actor_mutex;
	message_t *jobs;
	size_t job_count;
	size_t job_index;
	size_t job_insert_index;
	bool is_dead;
} actor_t;


typedef struct thread_manager {
	actor_t *actors;
	size_t actors_size;
	size_t actor_count;
	size_t dead_actor_count;
	pthread_mutex_t *access_mutex;
	pthread_cond_t *work_cond;
	pthread_cond_t *finish_cond;
	size_t job_count;
	size_t actor_index;
	pthread_t *threads;
	pthread_t *sig_thread;
} thread_manager_t;

thread_manager_t *tm;


static void actor_destroy(actor_t *actor) {
	if (actor == NULL) {
		return;
	}

	if (actor->role != NULL) {
		free(actor->role);
	}

	if (actor->actor_mutex != NULL) {
		pthread_mutex_destroy(actor->actor_mutex);
		free(actor->actor_mutex);
	}

	if (actor->jobs != NULL) {
		free(actor->jobs);
	}

	free(actor);
}


static void tm_destroy() {
	if (tm == NULL) {
		return;
	}

	for (size_t i = 0; i < tm->actor_count; i++) {
		actor_destroy(&tm->actors[i]);
	}

	if (tm->access_mutex != NULL) {
		pthread_mutex_destroy(tm->access_mutex);
		free(tm->access_mutex);
	}

	if (tm->work_cond != NULL) {
		pthread_cond_destroy(tm->work_cond);
		free(tm->work_cond);
	}

	if (tm->finish_cond != NULL) {
		pthread_cond_destroy(tm->finish_cond);
		free(tm->finish_cond);
	}

	if (tm->sig_thread != NULL) {
		free(tm->sig_thread);
	}

	free(tm);
}


static actor_id_t create_new_actor(role_t *const role) {
	pthread_mutex_lock(tm->access_mutex);

	if (tm->actor_count == CAST_LIMIT) {
		return -1;
	}

	if (tm->actor_count == tm->actors_size) {
		actor_t *temp = calloc(2 * tm->actors_size, sizeof(actor_t));
		tm->actors_size *= 2;
		if (temp == NULL) {
			exit(1);
		}

		for (size_t i = 0; i < tm->actor_count; i++) {
			temp[i] = tm->actors[i];
		}

		free(tm->actors);
		tm->actors = temp;
	}

	actor_t *new_actor = &(tm->actors[tm->actor_count]);

	new_actor->id = tm->actor_count;
	new_actor->role = role;

	new_actor->actor_mutex = calloc(1, sizeof(pthread_mutex_t));
	if (new_actor->actor_mutex == NULL) exit(1);
	if (pthread_mutex_init(new_actor->actor_mutex, NULL) == -1) exit(1);

	new_actor->jobs = calloc(ACTOR_QUEUE_LIMIT, sizeof(message_t));
	if (new_actor->jobs == NULL) {
		exit(1);
	}
	new_actor->job_count = 0;
	new_actor->job_index = 0;
	new_actor->job_insert_index = 0;
	new_actor->is_dead = false;

	tm->actor_count++;

	pthread_mutex_unlock(tm->access_mutex);

	return new_actor->id;
}


static message_t *tm_job_get(thread_manager_t *tm) {
	for (size_t steps = 0; steps < tm->actor_count; steps++) {
		pthread_mutex_lock(tm->actors[tm->actor_index].actor_mutex);

		actor_t *curr_actor = &(tm->actors[tm->actor_index]);

		if (curr_actor->job_count) {
			message_t *job = &(curr_actor->jobs[curr_actor->job_index]);

			curr_actor->job_index = (curr_actor->job_index + 1) % ACTOR_QUEUE_LIMIT;

			pthread_mutex_unlock(tm->actors[tm->actor_index].actor_mutex);

			tm->actor_index = (tm->actor_index + 1) % tm->actor_count;
			current_actor_id = curr_actor->id;

			return job;
		}

		tm->actor_index = (tm->actor_index + 1) % tm->actor_count;

		pthread_mutex_unlock(tm->actors[tm->actor_index].actor_mutex);
	}

	return NULL;
}


static void handle_sigint() {
	pthread_mutex_lock(tm->access_mutex);

	for (size_t i = 0; i < tm->actor_count; i++) {
		pthread_mutex_lock(tm->actors[i].actor_mutex);

		if (!tm->actors[i].is_dead) {
			tm->dead_actor_count++;
		}
		tm->actors[i].is_dead = true;

		pthread_mutex_unlock(tm->actors[i].actor_mutex);
	}
	
	pthread_mutex_unlock(tm->access_mutex);
}


static void *sig_thread_run() {
	struct sigaction action;
	sigset_t block_mask;

	sigemptyset(&block_mask);
	sigaddset(&block_mask, SIGINT);

	action.sa_sigaction = handle_sigint;
	action.sa_mask = block_mask;
	action.sa_flags = 0;

	// if (sigaction(SIGINT, &action, 0) == -1) syserr("haha");
	sigaction(SIGINT, &action, 0);

	for (int i = 0; i < POOL_SIZE; i++) {
		pthread_join(tm->threads[i], NULL);
	}

	return NULL; // TODO: exit(1)?
}


static void *worker_thread_run() {
	while (1) {
		pthread_mutex_lock(tm->access_mutex);

		while (tm->job_count == 0 && tm->dead_actor_count != tm->actor_count) {
			pthread_cond_wait(tm->work_cond, tm->access_mutex);
		}
		if (tm->job_count == 0 && tm->dead_actor_count == tm->actor_count) {
			break;
		}

		message_t *job = tm_job_get(tm);

		pthread_mutex_unlock(tm->access_mutex);

		actor_t *actor = &(tm->actors[current_actor_id]);

		if (job != NULL) {
			switch (job->message_type) {
				case MSG_SPAWN: {
					actor_id_t id = create_new_actor(job->data);
					if (id == -1) {
						break;
					}

					message_t *message = calloc(1, sizeof(message_t));
					if (message == NULL) {
						exit(1);
					}
					message->message_type = MSG_HELLO;
					message->nbytes = 1;
					message->data = (void *)actor_id_self();

					send_message(id, *message);

					break;
				}

				case MSG_GODIE:
					pthread_mutex_lock(actor->actor_mutex);

					if (!actor->is_dead) {
						pthread_mutex_lock(tm->access_mutex);

						tm->dead_actor_count++;

						pthread_mutex_unlock(tm->access_mutex);
					}
					actor->is_dead = true;

					pthread_mutex_unlock(actor->actor_mutex);
					break;

				// case MSG_HELLO: // TODO: czy to nie jest zbedne? (MSG_HELLO == 0)
				// 	actor->role->prompts[0](NULL, job->nbytes, job->data);
				// 	break;

				default:
					actor->role->prompts[job->message_type](NULL, job->nbytes, job->data);
					break;
			}

			free(job); // TODO: czy na pewno zwalniamy pamiec?
		}

		pthread_mutex_lock(tm->access_mutex);

		current_actor_id = -1;
		tm->job_count--;
		if (tm->job_count == 0 && tm->dead_actor_count == tm->actor_count) {
			pthread_cond_broadcast(tm->work_cond);
		}

		pthread_mutex_unlock(tm->access_mutex);
	}

	pthread_cond_signal(tm->finish_cond);
	pthread_mutex_unlock(tm->access_mutex);
	
	return NULL;
}


actor_id_t actor_id_self() {
	return current_actor_id;
}


int actor_system_create(actor_id_t *actor, role_t *const role) {
	tm = calloc(1, sizeof(thread_manager_t));
	if (tm == NULL) {
		return -1;
	}

	tm->actors = calloc(BASE_ACTORS_VECTOR_SIZE, sizeof(actor_t));
	if (tm->actors == NULL) {
		return -1;
	}
	tm->actors_size = BASE_ACTORS_VECTOR_SIZE;
	tm->actor_count = 0;
	tm->dead_actor_count = 0;

	tm->access_mutex = calloc(1, sizeof(pthread_mutex_t));
	if (tm->access_mutex == NULL) return -1;
	if (pthread_mutex_init(tm->access_mutex, NULL) == -1) return -2;

	tm->work_cond = calloc(1, sizeof(pthread_cond_t));
	if (tm->work_cond == NULL) return -1;
	if (pthread_cond_init(tm->work_cond, NULL) == -1) return -2;

	tm->finish_cond = calloc(1, sizeof(pthread_cond_t));
	if (tm->finish_cond == NULL) return -1;
	if (pthread_cond_init(tm->finish_cond, NULL) == -1) return -2;

	*actor = create_new_actor(role);

	tm->job_count = 0;
	tm->actor_index = 0;

	tm->threads = calloc(POOL_SIZE, sizeof(pthread_t));
	if (tm->threads == NULL) {
		return -1;
	}
	for (size_t i = 0; i < POOL_SIZE; i++) {
		if (pthread_create(&(tm->threads[i]), NULL, worker_thread_run, NULL) == -1) return -3;
	}

	tm->sig_thread = calloc(1, sizeof(pthread_t));
	if (tm->sig_thread == NULL) return -1;
	if (pthread_create(tm->sig_thread, NULL, sig_thread_run, NULL) == -1) return -3;

	message_t *message = calloc(1, sizeof(message_t));
	if (message == NULL) {
		exit(1);
	}
	message->message_type = MSG_HELLO;
	message->nbytes = 1;
	message->data = (void *)0;
	send_message(0, *message);

	return 0;
}


void actor_system_join(actor_id_t actor) {
	if (tm == NULL) {
		return;
	}
	pthread_mutex_lock(tm->access_mutex);
	if (actor < 0 || (size_t)actor > tm->actor_count) {
		pthread_mutex_unlock(tm->access_mutex);
		return;
	}
	pthread_mutex_unlock(tm->access_mutex);

	pthread_join(*tm->sig_thread, NULL);

	tm_destroy();
}


int send_message(actor_id_t actor, message_t message) {
	int return_value = 0;

	pthread_mutex_lock(tm->access_mutex);
	if (actor < 0 || (size_t)actor > tm->actor_count) {
		return_value = -2;
	}
	if (return_value) {
		pthread_mutex_unlock(tm->access_mutex);
		return return_value;
	}

	pthread_mutex_lock(tm->actors[actor].actor_mutex);
	if (tm->actors[actor].is_dead) {
		return_value = -1;
	}
	pthread_mutex_unlock(tm->access_mutex);
	if (return_value) {
		pthread_mutex_unlock(tm->actors[actor].actor_mutex);
		return return_value;
	}

	actor_t *recipient = &(tm->actors[actor]);
	recipient->jobs[recipient->job_insert_index] = message;
	recipient->job_insert_index = (recipient->job_insert_index + 1) % ACTOR_QUEUE_LIMIT;

	if (tm->job_count == 0) { // TODO: czy to na pewno dobrze?
		pthread_cond_signal(tm->work_cond);
	}

	pthread_mutex_unlock(tm->actors[actor].actor_mutex);

	return 0;
}
