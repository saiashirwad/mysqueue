import { SqlClient, SqlResolver } from '@effect/sql'
import { createId } from '@paralleldrive/cuid2'
import { Effect } from 'effect'
import * as Schema from 'effect/Schema'

export class QueueJob extends Schema.Class<QueueJob>('QueueJob')({
	id: Schema.String,
	payload: Schema.Any,
	status: Schema.Union(
		Schema.Literal('completed'),
		Schema.Literal('failed'),
		Schema.Literal('pending'),
		Schema.Literal('processing'),
	),
	attempts: Schema.Number,
	createdAt: Schema.Date,
	maxRetries: Schema.Number,
	scheduledFor: Schema.NullOr(Schema.Date),
	queue: Schema.String,
	errorMessage: Schema.NullOr(Schema.String),
}) {}

export const InsertQueueJobSchema = Schema.Struct(QueueJob.fields).pipe(
	Schema.omit('id', 'createdAt'),
)
