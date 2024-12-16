import { Data, Duration } from 'effect'

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
