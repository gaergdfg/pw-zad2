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


typedef void (*job_func_t)(void *arg);

typedef struct job {
	job_func_t func;
	void *arg;
} job_t;


typedef struct actor {
	actor_id_t id;
	role_t role;
	pthread_mutex_t actor_mutex;
	job_t *jobs; // TODO: make it behave like a vector
	size_t job_count;
	size_t job_index;
	bool is_dead;
} actor_t;


typedef struct thread_manager {
	actor_t *actors; // TODO: make it behave like a vector
	size_t actor_count;
	size_t dead_actor_count;
	pthread_mutex_t access_mutex;
	pthread_cond_t work_cond;
	pthread_cond_t finish_cond;
	size_t job_count;
	size_t working_count;
	size_t total_count;
	size_t actor_index;
} thread_manager_t;

thread_manager_t *tm;


// TODO: create/destroy job_t


static job_t *tm_job_get(thread_manager_t *tm) {
	for (size_t steps = 0; steps < tm->actor_count; steps++) {
		actor_t *curr_actor = &(tm->actors[tm->actor_index]);

		if (curr_actor->job_count) {
			job_t *job = &(curr_actor->jobs[curr_actor->job_index]);

			curr_actor->job_index = (curr_actor->job_index + 1) % ACTOR_QUEUE_LIMIT;
			tm->actor_index = (tm->actor_index + 1) % tm->actor_count;

			return job;
		}

		tm->actor_index = (tm->actor_index + 1) % tm->actor_count;
	}

	return NULL;
}


actor_id_t actor_id_self() {
	return 2137; // TODO: implement it
}


static void *thread_run() {
	while (1) {
		pthread_mutex_lock(&(tm->access_mutex));
		while (tm->job_count == 0) {
			pthread_cond_wait(&(tm->work_cond), &(tm->access_mutex));
		}

		job_t *job = tm_job_get(tm);
		tm->working_count++;
		pthread_mutex_unlock(&(tm->access_mutex));

		if (job != NULL) {
			job->func(job->arg);
			// tpool_work_destroy(work);
			// TODO: destroy the job object?
		}

		pthread_mutex_lock(&(tm->access_mutex));
		tm->working_count--;
		tm->job_count--;
		// if (!tm->finished && tm->working_count == 0 && tm->work_first == NULL)
		// TODO: check if all actors are dead and there are no more jobs
			// pthread_cond_signal(&(tm->finish_cond));
		if (tm->dead_actor_count == tm->actor_count && !tm->job_count) {
			break;
		}
		pthread_mutex_unlock(&(tm->access_mutex));
	}

	tm->total_count--;
	pthread_cond_signal(&(tm->finish_cond));
	pthread_mutex_unlock(&(tm->access_mutex));
	return NULL;
}

int actor_system_create(actor_id_t *actor, role_t *const role) {
	tm = calloc(1, sizeof(*tm));

	pthread_mutex_init(&(tm->access_mutex), NULL);
	pthread_cond_init(&(tm->work_cond), NULL);
	pthread_cond_init(&(tm->finish_cond), NULL);

	// tm->work_first = NULL;
	// tm->work_last  = NULL;
	// TODO: initialise the first actor

	pthread_t thread;
	tm->working_count = POOL_SIZE;
	for (size_t i = 0; i < POOL_SIZE; i++) {
		pthread_create(&thread, NULL, thread_run, NULL);
		pthread_detach(thread);
	}

	return 0;
}


void actor_system_join(actor_id_t actor) {
	// TODO: implement it
}


int send_message(actor_id_t actor, message_t message) {
	return 69; // TODO: implement it
}
