# pg-events

This is a small library for using Postgres LISTEN/NOTIFY to subscribe
to table change events.

When a row is inserted, updated or deleted postgres will notify
your application and your custom callbacks will be invoked allowing
you to respond to the event anyway you like.

The row is provided to your callback as a JSON string.

Here's a quick example:

```golang
func main() {
	connectionString := "host=localhost port=5432 ..."

	// connect to postgres
	listener, err := pgevents.OpenListener(connectionString)
	if err != nil {
		log.Fatal(err)
	}

	// attach the listener to 1 or more table(s)
	if err := listener.Attach("my_table"); err != nil {
		log.Fatal(err)
	}

	// attach 1 or more callback(s)
	listener.OnEvent(func(event *pgevents.TableEvent) {
		fmt.Printf("received event: %v\n", event)
	})
}
```
