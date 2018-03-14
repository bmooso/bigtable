package internal

import (
	"github.com/bmooso/bigtable/request"
	"github.com/bmooso/bigtable/store"
	"github.com/labstack/echo"
)

type MessageHandler struct {
	RowMetaData  store.RowMetaData
	MessageStore store.MessageStore
}

const (
	columnName = "Message"
)

func (mh MessageHandler) InitRoutes(g *echo.Group) {
	g.POST("/messages", mh.create)
	g.GET("/messages/:id", mh.read)
	// g.GET("/messages", mh.readAll)
	g.DELETE("/messages/:id", mh.delete)
	g.DELETE("/table", mh.tearDown)
	g.GET("/messages/deleted", mh.readAllDeleted)
	g.PUT("/messages/:id", mh.update)
}

func (mh MessageHandler) tearDown(c echo.Context) error {
	return mh.MessageStore.TearDown()
}

func (mh MessageHandler) create(c echo.Context) error {
	var m request.Message
	if err := c.Bind(&m); err != nil {
		return err
	}

	return mh.MessageStore.CreateNew(mh.RowMetaData, m)
}

func (mh MessageHandler) read(c echo.Context) error {
	id := c.Param("id")
	return mh.MessageStore.ReadSingle(mh.RowMetaData, columnName, id)
}

// func (mh MessageHandler) readAll(c echo.Context) error {

// 	return mh.MessageStore.ReadAll(mh.RowMetaData, columnName)
// }

func (mh MessageHandler) readAllDeleted(c echo.Context) error {

	return mh.MessageStore.ReadAllDeleted(mh.RowMetaData, columnName)
}

func (mh MessageHandler) update(c echo.Context) error {
	var m request.Message
	if err := c.Bind(&m); err != nil {
		return err
	}

	id := c.Param("id")
	return mh.MessageStore.Update(mh.RowMetaData, id, m)
}

func (mh MessageHandler) delete(c echo.Context) error {
	id := c.Param("id")
	return mh.MessageStore.Delete(mh.RowMetaData, columnName, id)

}
