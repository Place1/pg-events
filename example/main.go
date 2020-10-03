package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/pkg/errors"
	"github.com/place1/pg-events/pkg/pgevents"
)

type ExampleTable struct {
	ID        uint      `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func OpenGorm(connectionString string) (*gorm.DB, error) {
	db, err := gorm.Open("postgres", connectionString)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to connect to %s", "postgres"))
	}

	// Migrate the schema
	db.AutoMigrate(&ExampleTable{})

	return db, nil
}

func main() {
	connectionString := "host=localhost port=5432 sslmode=disable dbname=postgres user=postgres password=development"

	db, err := OpenGorm(connectionString)
	if err != nil {
		log.Fatal(err)
	}

	listener, err := pgevents.OpenListener(connectionString)
	if err != nil {
		log.Fatal(err)
	}

	if err := listener.Attach("example_tables"); err != nil {
		log.Fatal(err)
	}

	listener.OnEvent(func(event *pgevents.TableEvent) {
		row := &ExampleTable{}
		if err := json.Unmarshal([]byte(event.Data), row); err == nil {
			fmt.Println(row.CreatedAt)
		}
	})

	i := 0
	for {
		db.Save(&ExampleTable{
			Name:      fmt.Sprintf("example-row-%d", i),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		})
		time.Sleep(5 * time.Second)
		i++
	}
}
