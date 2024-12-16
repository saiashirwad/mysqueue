import { SqlClient, SqlResolver } from '@effect/sql'
import { createId } from '@paralleldrive/cuid2'
import { Effect } from 'effect'
import * as Schema from 'effect/Schema'
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
