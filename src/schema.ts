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
	scheduledFor: Schema.Date,
	queue: Schema.String,
	errorMessage: Schema.NullOr(Schema.String),
}) {}
