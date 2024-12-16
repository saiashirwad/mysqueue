import { UTCDate } from '@date-fns/utc'
import { createId } from '@paralleldrive/cuid2'
import { addMilliseconds } from 'date-fns'
import { Context, Data, Duration, Effect, Schedule, pipe } from 'effect'
import type { UnknownException } from 'effect/Cause'
import type { Insertable, Selectable } from 'kysely'
import { db } from '~/db/db'
import type { QueueJob } from '~/db/schema'
import { dbJson } from '~/utils/kysely'

type JobQueueOptions = {
	scheduledFor?: Date
	maxRetries?: number
	timeoutAfter?: number
}

export class JobQueueError extends Data.TaggedError('QueueError')<{
	queue: string
	jobId: string
	message: string
}> {}

export class JobQueueRetry extends Data.TaggedError('QueueRetry')<{
	queue: string
	jobId: string
	duration: Duration.Duration
}> {}

export type QueueContext = {
	job: Selectable<QueueJob>
	retry: (
		duration: Duration.Duration,
	) => Effect.Effect<never, JobQueueError | JobQueueRetry, never>
	fail: (message: string) => Effect.Effect<never, JobQueueError, never>
}

export const QueueContext = Context.GenericTag<QueueContext>('QueueContext')

function createContext(job: Selectable<QueueJob>): QueueContext {
	const fail = (message: string) =>
		Effect.fail(new JobQueueError({ queue: job.queue, jobId: job.id, message }))

	const retry = (duration: Duration.Duration) => {
		return Effect.gen(function* () {
			if (job.attempts >= job.maxRetries) {
				return yield* fail(`Max retries reached for job ${job.id}`)
			}
			return yield* new JobQueueRetry({
				queue: job.queue,
				jobId: job.id,
				duration,
			})
		})
	}

	return {
		job,
		retry,
		fail,
	}
}

function fail<E extends Error>(error: E, job: Selectable<QueueJob>) {
	return Effect.promise(() =>
		db.transaction().execute(async (tx) => {
			await tx
				.updateTable('queueJob')
				.set({ status: 'failed', errorMessage: error.message })
				.where('id', '=', job.id)
				.execute()
			await tx
				.insertInto('queueJobExecution')
				.values({
					id: createId(),
					timestamp: new UTCDate(),
					queueJobId: job.id,
					payload: dbJson({ error: error.message }),
					status: 'failed',
				})
				.execute()
		}),
	).pipe(Effect.flatMap(() => Effect.fail(new Error('Failed'))))
}

function retry(duration: Duration.Duration, job: Selectable<QueueJob>) {
	return Effect.promise(() =>
		db.transaction().execute(async (tx) => {
			await tx
				.updateTable('queueJob')
				.set({
					status: 'pending',
					scheduledFor: addMilliseconds(
						new UTCDate(),
						Duration.toMillis(duration),
					),
				})
				.where('id', '=', job.id)
				.execute()

			await tx
				.insertInto('queueJobExecution')
				.values({
					id: createId(),
					timestamp: new UTCDate(),
					queueJobId: job.id,
					payload: dbJson({}),
					status: 'retried',
				})
				.execute()
		}),
	).pipe(Effect.flatMap(() => Effect.fail(new Error('Retrying'))))
}

function complete(result: any, job: Selectable<QueueJob>) {
	return Effect.promise(() =>
		db.transaction().execute(async (tx) => {
			await tx
				.updateTable('queueJob')
				.set({ status: 'completed' })
				.where('id', '=', job.id)
				.execute()

			await tx
				.insertInto('queueJobExecution')
				.values({
					id: createId(),
					timestamp: new UTCDate(),
					queueJobId: job.id,
					payload: dbJson(result ?? {}),
					status: 'success',
				})
				.execute()
		}),
	)
}

type Validator<T> = (payload: unknown) => T

interface Queue<T> {
	name: string
	schema: Validator<T>
	run: (
		payload: T,
	) => Effect.Effect<void, JobQueueError | JobQueueRetry, QueueContext>
	enqueue(payload: T, options?: JobQueueOptions): Promise<Selectable<QueueJob>>
	enqueueEffect(
		payload: T,
		options?: JobQueueOptions,
	): Effect.Effect<Selectable<QueueJob>, UnknownException, never>
}

export function createQueue<T>(
	name: string,
	schema: Validator<T>,
	run: (
		payload: T,
	) => Effect.Effect<void, JobQueueError | JobQueueRetry, QueueContext>,
): Queue<T> {
	return {
		name,
		schema,
		run,
		enqueue: async (payload, options = {}) => {
			const data = {
				id: createId(),
				queue: name,
				payload: dbJson(payload),
				scheduledFor: options.scheduledFor ?? new UTCDate(),
				attempts: 0,
				createdAt: new UTCDate(),
				maxRetries: options.maxRetries ?? 3,
				status: 'pending',
				errorMessage: null,
			} satisfies Insertable<QueueJob>
			await db.insertInto('queueJob').values(data).execute()
			return data
		},
		enqueueEffect: (payload, options = {}) =>
			Effect.gen(function* () {
				const data = {
					id: createId(),
					queue: name,
					payload: dbJson(payload),
					scheduledFor: options.scheduledFor ?? new UTCDate(),
					attempts: 0,
					createdAt: new UTCDate(),
					maxRetries: options.maxRetries ?? 3,
					status: 'pending',
					errorMessage: null,
				} satisfies Insertable<QueueJob>
				yield* Effect.tryPromise(() =>
					db.insertInto('queueJob').values(data).execute(),
				)
				return data
			}),
	}
}

function getNextJob(queue: Queue<any>) {
	return Effect.promise(() =>
		db.transaction().execute(async (tx) => {
			const job = await tx
				.selectFrom('queueJob')
				.selectAll('queueJob')
				.where('queue', '=', queue.name)
				.where((eb) =>
					eb.or([eb('status', '=', 'pending'), eb('status', '=', 'failed')]),
				)
				.whereRef('attempts', '<=', 'maxRetries')
				// .where("scheduledFor", "<=", new UTCDate())
				.where((eb) =>
					eb.or([
						eb('scheduledFor', 'is', null),
						eb('scheduledFor', '<=', new UTCDate()),
					]),
				)
				.orderBy('scheduledFor', 'asc')
				.limit(1)
				.executeTakeFirst()

			console.log(job)

			if (!job) {
				return null
			}

			await tx
				.updateTable('queueJob')
				.set({
					status: 'processing',
					attempts: (eb) => eb('attempts', '+', 1),
					// Optionally add a small delay for failed jobs to prevent tight retry loops
					scheduledFor:
						job.status === 'failed'
							? addMilliseconds(new UTCDate(), 1000) // 1 second delay
							: job.scheduledFor,
				})
				.where('id', '=', job.id)
				.execute()

			return job
		}),
	).pipe(
		Effect.withSpan('getNextJob', {
			attributes: {
				queue: queue.name,
			},
		}),
	)
}

function processNextJob<T>(queue: Queue<T>) {
	return Effect.gen(function* () {
		const job = yield* getNextJob(queue)
		if (job) {
			const payload = queue.schema.parse(job.payload)
			const context = createContext(job)
			yield* queue.run(payload).pipe(
				Effect.catchTag('QueueError', (error) => fail(error, job)),
				Effect.catchTag('QueueRetry', (data) => retry(data.duration, job)),
				Effect.flatMap((result) => complete(result, job)),
				Effect.catchAll(() => Effect.void),
				Effect.provideService(QueueContext, context),
			)
		}
	})
}

export function startWorker<T>(
	queue: Queue<T>,
): Effect.Effect<number, never, never> {
	console.log('Starting worker for queue', queue.name)
	return pipe(
		processNextJob(queue).pipe(Effect.catchAll((s) => Effect.void)),
		Effect.repeat(Schedule.spaced('1 seconds')),
		Effect.interruptible,
	)
}

export function startWorkers(queues: Queue<any>[]) {
	return Effect.gen(function* () {
		const workerEffect = Effect.all(queues.map(startWorker), {
			concurrency: 'unbounded',
		})

		yield* workerEffect
	}).pipe(Effect.runPromise)
}
