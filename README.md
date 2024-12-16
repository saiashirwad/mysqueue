# Very much a work in progress. 

```typescript
import { z } from 'zod'
import { Duration, Effect } from 'effect'
import { createQueue, QueueContext } from "@texoport/mysqueue";
import * as Schema from '@effect/schema'

const something = Effect.gen(function* () {
  const ctx = yield* QueueContext;
  console.log(ctx.job)
})


const testQueue = createQueue(
  "test",
  Schema.Struct({
    name: Schema.String
  }),
  (payload) =>
    Effect.gen(function* () {
      const ctx = yield* QueueContext
      console.log("Do stuff")
      yield* something
      if (payload.name === "texoport") {
          return yield* ctx.retry(Duration.seconds(2))
      }
      console.log(payload.name, " complete");
    }),
);

```

0% chance of working lmao (I just copy pasted the code from a different project)

# TODO
- [ ] use `SELECT ... FOR UPDATE SKIP LOCKED`
- [ ] Remove dependency on date-fns (effect/DateTime should be able to handle it)
- [ ] Remove dependency on kysely (switch to @effect/sql-mysql2)
- [ ] Set up migrations function that runs whenever the queue starts
- [ ] Remove dependency on Zod and support any validator
