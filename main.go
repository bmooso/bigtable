// Copyright 2016 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Hello world is a sample program demonstrating use of the Bigtable client
// library to perform basic CRUD operations
package main

import (
	"context"
	"flag"
	"log"

	"github.com/bmooso/bigtable/internal"
	"github.com/bmooso/bigtable/store"
	"github.com/labstack/echo"
)

func main() {
	project := flag.String("project", "dev-srplatform", "The Google Cloud Platform project ID. Required.")
	instance := flag.String("instance", "rich-deleteme", "The Google Cloud Bigtable instance ID. Required.")
	flag.Parse()

	for _, f := range []string{"project", "instance"} {
		if flag.Lookup(f).Value.String() == "" {
			log.Fatalf("The %s flag is required.", f)
		}
	}

	e := echo.New()

	ctx := context.Background()

	tableName := "test-bmooso"

	msgStore, err := store.NewMessageStore(ctx, tableName, project, instance)

	if err != nil {
		panic(err)
	}

	messageRow := store.RowMetaData{
		ColumnFamilyName: "cf1",
		Key:              "com.sr#test#messages",
	}

	mh := internal.MessageHandler{
		RowMetaData:  messageRow,
		MessageStore: *msgStore,
	}

	sh := internal.SubscriberHandler{
		RowMetaData:  messageRow,
		MessageStore: *msgStore,
	}

	mh.InitRoutes(e.Group("/bigtable"))
	sh.InitRoutes(e.Group("/bigtable/subscriber"))

	// e.Pre(middleware.RemoveTrailingSlash())
	e.Logger.Fatal(e.Start(":8080"))
}
