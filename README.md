# 💓 Prisma Pulse Toolkit

This repo contains open source components of [Prisma Pulse](https://prisma.io/pulse).

This includes:

- `@prisma/pulse-cdc-pg` for consuming PostgreSQL logical replication data in JavaScript applications.
- more to come 👀

Prisma Pulse extends from the components of this repository by adding robust infrastructure and event persistence, enabling teams to deploy real-time applications and event-driven architectures without maintaining complex infrastructure.

## What is Prisma Pulse?

[Prisma Pulse](https://www.prisma.io/data-platform/pulse?utm_source=github&utm_medium=pulse-readme) is a managed Change Data Capture (CDC) service that makes it easy to react to changes in your databases with type-safe model streams.

It enables developers to build real-time apps by streaming database changes into their application in a type-safe way — with just a few lines of code:

```tsx
// 1. Subscribe to all changes on the `User` table
const stream = await prisma.user.stream();

// 2. Wait for changes to happen in the DB so that new events arrive
for await (let event of stream) {
  // 3. Do something with an event, e.g. log the its details to the terminal
  console.log(`Something happened in the database: `, event);
}
```

Here is an overview of the main features Prisma Pulse provides:

- Type-safe reactions to database changes
- Delivery guarantees for events: "at least once" and in the "right order"
- Resumable event streams in case your server goes down
- Unidirectional data flow via Change Data Capture (CDC)
- Great DX integrated with usage of Prisma ORM (easy setup, development and maintenance)
- Compatibility with your existing database
- Insights dashboard, so you can view and understand all database events captured by Pulse

## Learn more

If you’re interested in learning more, be sure to check out the [Prisma Pulse portion of the Prisma docs](https://www.prisma.io/docs/pulse).

For community help and to connect with other developers, be sure to [join the Prisma Discord](https://pris.ly/discord)!
