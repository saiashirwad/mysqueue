import { SqlClient, SqlResolver } from '@effect/sql'
import { createId } from '@paralleldrive/cuid2'
import { Effect } from 'effect'
import { InsertQueueJobSchema } from './schema'

const insertQueueJob = SqlClient.SqlClient.pipe(
	Effect.flatMap((sql) =>
		SqlResolver.void('InsertQueueJob', {
			Request: InsertQueueJobSchema,
			execute: (requests) => {
				const input = {
					...requests,
					id: createId(),
					createdAt: new Date(),
				}
				return sql`INSERT INTO queue_jobs ${sql.insert(input)}`
			},
		}),
	),
)

const example = Effect.gen(function* () {
	yield* insertQueueJob.pipe(
		Effect.flatMap((insert) =>
			insert.execute({
				status: 'pending',
				queue: 'default',
				payload: JSON.stringify({ hi: 'there' }),
				attempts: 0,
				maxRetries: 3,
				errorMessage: null,
				scheduledFor: null,
			}),
		),
	)
})
